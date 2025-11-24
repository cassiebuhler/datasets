# READ ME 

1. Use `census_download.ipynb` to download the data and convert shape files to parquet
2. Convert geometries in the parquet to hex with `census_sld.py`


   ```python census_sld.py --chamber lower```
   and
   ```python census_sld.py --chamber upper```
