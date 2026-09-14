"""
This script transforms GIS coordinates from EPSG:2882 (NAD83(HARN) Florida West (ftUS))
to EPSG:4326 (Standard WGS84 Lat/Lon).

It reads a Parquet file containing parcel data, performs the transformation,
and saves the updated coordinates back to the same Parquet file.
"""

import pandas as pd
from pyproj import Transformer

file_path = './data/parquet/dim_parcels.parquet'

print(f"▷ Loading {file_path}")
df = pd.read_parquet(file_path)

if 'longitude' in df.columns and 'latitude' in df.columns:
    print("▷ Transforming coordinates from EPSG:2882 to EPSG:4326")
    
    # EPSG:2882 = NAD83(HARN) Florida West (ftUS)
    # EPSG:4326 = Standard WGS84 Lat/Lon
    transformer = Transformer.from_crs("EPSG:2882", "EPSG:4326", always_xy=True)
    
    lon_wgs84, lat_wgs84 = transformer.transform(
        df['longitude'].values, 
        df['latitude'].values
    )
    
    # Overwrite the State Plane columns with the new standard WGS84 coordinates
    df['longitude'] = lon_wgs84
    df['latitude'] = lat_wgs84
    
    print("▷ Saving updated data back to parquet")
    # Overwrite the original file
    df.to_parquet(file_path, index=False)
    
    print("₪₪₪ Coordinate transformation complete ₪₪₪")
else:
    print("Error: 'latitude' or 'longitude' columns not found in the Parquet file.")