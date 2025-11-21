import ibis
from ibis import _
from cng.utils import *
from cng.h3 import *
duckdb_install_h3()
from minio import Minio
from minio.error import S3Error
from urllib.parse import urlparse
import os
import argparse

def minio_file_exists(s3_url):
    """
    Check if an S3/MinIO file exists.
    s3_url example: 's3://bucket/path/to/file.parquet'
    """
    key = os.getenv("MINIO_KEY")
    secret = os.getenv("MINIO_SECRET")

    # connect to minio
    client = Minio(
        "minio.carlboettiger.info",
        access_key=key,
        secret_key=secret,
        secure=True  # change to False if you're not using HTTPS
    )
    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    try:
        client.stat_object(bucket, key)
        return True
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False
        return False


def geom_to_cell(df, zoom=8, keep_cols=None):
    '''
    Convert geom to h3 cell. Returns nested cells 
    '''
    con = df.get_backend()
    
    # Default to keeping all columns except geom if not specified
    if keep_cols is None:
        keep_cols = [col for col in df.columns if col != 'geom']
    
    # Build column list for SELECT statements
    col_list = ', '.join(keep_cols)
    
    # all types must be multi-polygons
    cases = ibis.cases(
        (df.geom.geometry_type() == 'POLYGON', ST_Multi(df.geom)),
        else_=df.geom,
    )
    
    df = df.mutate(geom=cases)
    sql = ibis.to_sql(df)
    
    expr = f'''
        WITH t1 AS (
            SELECT {col_list}, UNNEST(ST_Dump(ST_GeomFromWKB(geom))).geom AS geom 
            FROM ({sql})
        ) 
        SELECT *, h3_polygon_wkt_to_cells_string(geom, {zoom}) AS h3id FROM t1
    '''

    out = con.sql(expr)
    return out

def convert_to_h3(z, save_url, df, args):
    '''
    Driver function to convert h3.
    Will call chunking function if it's too large
    '''
    try:     
        df_h3 = (
            geom_to_cell(df, zoom=z) # convert geoms to h3
            .mutate(h8 = _.h3id.unnest())
            .mutate(h0 = h3_cell_to_parent(_.h8, 0))
            .drop('h3id')
        )
        df_h3.to_parquet(save_url) #write to minio 
        print(f'Saved to {save_url}')
    except Exception as e:
        folder = save_url.rsplit(f'_z{z}.parquet', 1)[0]
        print(f'Need to chunk.')
        print(f'Will save output to {folder}')
        run_in_chunks(z, folder, df, args)
    return 
        

def run_in_chunks(z, folder, df, args):
    '''
    Chunking function
    '''
    CHUNK_SIZE = 10
    MIN_CHUNK_SIZE = 1

    # Get total row count and calculate chunks
    total_rows = df.count().execute()
    print(f"Total rows: {total_rows:,}")

    while CHUNK_SIZE >= MIN_CHUNK_SIZE:
        num_chunks = (total_rows + CHUNK_SIZE - 1) // CHUNK_SIZE
        print(f"Trying chunk size {CHUNK_SIZE}, number of chunks: {num_chunks}")
        
        for chunk_id in range(num_chunks):
            offset = chunk_id * CHUNK_SIZE
            chunk = df.limit(CHUNK_SIZE, offset=offset)
            save_url = f"{folder}/chunk_{chunk_id:06d}.parquet"
            if minio_file_exists(save_url):
                print(f'File already exists: {save_url}')
            else:
                try:
                    convert_to_h3(z, save_url, chunk, args)
                except Exception as e:
                    print(f"Chunk {chunk_id} failed with chunk size {CHUNK_SIZE}: {e}")
                    if CHUNK_SIZE == MIN_CHUNK_SIZE:
                        print(f"Chunk {chunk_id} cannot be processed even at MIN_CHUNK_SIZE. Skipping.")
                        continue  # move on to next chunk
                    CHUNK_SIZE = max(CHUNK_SIZE // 2, MIN_CHUNK_SIZE)
                    break  # retry all chunks with smaller size
        else:
            # All chunks succeeded
            return

def main():
    parser = argparse.ArgumentParser(description="Process polygon file i to zoom z")
    parser.add_argument("--i", type=int, default=0, help="Chunk index to process (0-based)")
    parser.add_argument("--zoom", type=int, default=8, help="H3 resolution to aggregate to (default 8)")
    parser.add_argument("--chamber", type=str, default="upper", help="lower or upper chamber of state legislative districts (default upper)")
    parser.add_argument("--base-url", default = "s3://public-census/2024/sld",  help="Output geoparquet bucket (doesn't end with /)")
    args = parser.parse_args()
    base_url = args.base_url
    z = args.zoom
    chamber = args.chamber 
    
    con = ibis.duckdb.connect(extensions = ["spatial", "h3"])
    con.raw_sql("SET THREADS=100;")
    set_secrets(con)
        
    #get fips code for each state
    fips_url = 'https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt'
    fips_codes = con.read_csv(fips_url).filter(_.STATE.notin(['AS','GU','MP','PR','UM','VI'])).select("STATEFP").execute().values.flatten().tolist()
    if chamber == 'lower':
        fips_codes.remove('11') #DC doesn't have lower chamber
        fips_codes.remove('31') #Nebrasks doesn't have lower chamber 
        
    for state in fips_codes:
        chamber_acronym = 'sldu' if chamber == 'upper' else 'sldl' #sldu for upper, sldl for lower 
        
        save_url = f'{base_url}/{chamber}/z{z}/tl_2024_{state}_{chamber_acronym}_z{z}.parquet'
        if not minio_file_exists(save_url):
            print(f'Processing for {state}')
            url = f'{base_url}/{chamber}/tl_2024_{state}_{chamber_acronym}.parquet'
            df = con.read_parquet(url).mutate(geom = _.geom.convert('EPSG:4269','EPSG:4326'))
            convert_to_h3(z, save_url, df, args)
        else: 
            print(f'File already exists for {state}')
        
if __name__ == "__main__":
    main()