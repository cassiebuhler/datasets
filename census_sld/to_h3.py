import ibis
from ibis import _
from cng.utils import *
from cng.h3 import *
duckdb_install_h3()

def geom_to_cell(df, zoom=8, keep_cols=None):
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
    try:     
        df_h3 = (
            geom_to_cell(df, zoom=z) # convert geoms to h3
            .mutate(h8 = _.h3id.unnest())
            .mutate(h0 = h3_cell_to_parent(_.h8, 0))
            .drop('h3id')
        )
        df_h3.to_parquet(save_url) #write to minio 
        print(f'Success: State {state} has been converted')
    except Exception as e:
         print(f'Failed: State {state} is too big. Will try chunking')
        # chunk the data 
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
