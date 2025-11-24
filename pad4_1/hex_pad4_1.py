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
from urllib.parse import urlparse
from minio import Minio
from minio.error import S3Error
import os

def minio_file_exists(s3_url, z=None):
    """
    Check whether an exact S3/MinIO file exists.
    If not, check if the *folder* (prefix) exists.
    Folder is derived by stripping `_z{z}.parquet` from the URL.
    """
    access_key = os.getenv("MINIO_KEY")
    secret_key = os.getenv("MINIO_SECRET")

    client = Minio(
        "minio.carlboettiger.info",
        access_key=access_key,
        secret_key=secret_key,
        secure=True
    )

    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
     
    try:
        client.stat_object(bucket, key)
        return True
    except S3Error as e:
        if e.code not in ("NoSuchKey", "NotFound"):
            raise  # unexpected error → rethrow

    # if no file, check folder 
    if z is None:
        # No Z provided → cannot derive folder
        return False

    # Build the folder prefix by removing `_z{z}.parquet`
    suffix = f"_z{z}.parquet"
    if key.endswith(suffix):
        folder_prefix = key[: -len(suffix)]
    else:
        # fallback: split on suffix
        folder_prefix = key.rsplit(suffix, 1)[0]

    # Ensure prefix ends with '/'
    if not folder_prefix.endswith("/"):
        folder_prefix += "/"

    # Check if any objects exist under that prefix
    objects = client.list_objects(bucket, prefix=folder_prefix, recursive=False)
    for _ in objects:
        return True  # folder exists

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
            .drop('h3id','geom')
        )
        df_h3.to_parquet(save_url) #write to minio 
        print(f'Saved to {save_url}')
    except Exception as e:
        print(e)
        folder = os.path.dirname(save_url)
        # base_name = os.path.splitext(os.path.basename(save_url))[0]
        print(f'Need to chunk.')
        print(f'Will save output to {folder}')
        run_in_chunks(z, folder, df, args)
    return 
        

def run_in_chunks(z, folder, df, args):
    '''
    Chunking function
    '''
    CHUNK_SIZE = 1000
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
    parser.add_argument("--base-url", default = "s3://public-biodiversity/pad-us-4_1/no_overlap",  help="Output geoparquet bucket (doesn't end with /)")
    args = parser.parse_args()
    base_url = args.base_url
    z = args.zoom
    
    con = ibis.duckdb.connect(extensions = ["spatial", "h3"])
    con.raw_sql("SET THREADS=100;")
    set_secrets(con)

        
    #get fips code for each state
    fips_url = 'https://www2.census.gov/geo/docs/reference/codes2020/national_state2020.txt'
    states = con.read_csv(fips_url).select("STATE").execute().values.flatten().tolist()
    states.append('UNKF')

    for state in states:
        save_url = f'{base_url}/z{z}/{state}_z{z}.parquet'
        if not minio_file_exists(save_url):
            print(f'Processing for {state}')
            url = f'{base_url}/pad-us-4_1_no_overlap.parquet'
            df = con.read_parquet(url).filter(_.State_Nm == state).select('row_n','Unit_Nm','State_Nm','geometry').rename(geom = 'geometry')
            convert_to_h3(z, save_url, df, args)
        else: 
            print(f'File already exists for {state}')
        
if __name__ == "__main__":
    main()