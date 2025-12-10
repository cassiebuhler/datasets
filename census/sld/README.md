# READ ME 

1. Use `sld_download.ipynb` to download the data and convert shape files to parquet
2. Convert geometries in the parquet to hex with `sld_to_hex.py`


   ```python sld_to_hex.py --chamber lower```
   and
   ```python sld_to_hex.py --chamber upper```
