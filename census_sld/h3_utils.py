import ibis
from ibis import _
from cng.utils import *
from cng.h3 import *
duckdb_install_h3()
from minio import Minio
from minio.error import S3Error
from urllib.parse import urlparse
import os

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

def convert_to_h3(z, save_url, df):
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
        run_in_chunks(z, folder, df)
    return 
        

def run_in_chunks(z, folder, df):
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

            try:
                convert_to_h3(z, save_url, chunk)
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


## this function just makes a noise when the code is done running 
from IPython.lib.display import Audio
import numpy as np

def alert():
    framerate = 4410
    play_time_seconds = 1
    
    t = np.linspace(0, play_time_seconds, framerate*play_time_seconds)
    audio_data = np.sin(2*np.pi*300*t) + np.sin(2*np.pi*240*t)
    display(Audio(audio_data, rate=framerate, autoplay=True))
    return 
