# Polk County Mass Appraisal and Valuation Intelligence Pipeline

An end-to-end data engineering and machine learning pipeline built to process, transform, and cluster mass appraisal real estate data for Polk County, Florida. The resulting dataset is optimized for a Power BI portfolio dashboard featuring spatial anomalies and escaped value analysis.

## Pipeline Architecture

This repository contains three primary execution scripts that transform raw county tax roll and GIS data into a highly optimized, ZSTD-compressed Parquet star schema.

1. **`etl_pipeline.py`**: The core script, using DuckDB's in-memory processing to ingest CSV, TXT, and SHP files. It handles spatial loading, staging, statutory stratification logic, and outputs finalized fact and dimension tables.
2. **`transform_gis_coords.py`**: A specialized spatial projection script. Uses `pyproj` to convert local Florida State Plane coordinates (EPSG:2882) into standard WGS84 Latitude/Longitude (EPSG:4326) within the Parquet files to enable native Azure Maps rendering in Power BI.
3. **`k_means_etl.py`**: An unsupervised machine learning pipeline. Applies K-Means clustering to parcel characteristics (value, size, market area, quality, and use code) using a geometric, automated Elbow Method algorithm to detect the optimal number of clusters. 

## Repository Structure
```text
.
├── docs/
│   └── data_dictionary.md      # Schema and logic documentation
├── scripts/
│   ├── etl_pipeline.py         # DuckDB staging and dimensional modeling
│   ├── transform_gis_coords.py # Coordinate reprojection
│   └── k_means_etl.py          # ML clustering
├── .gitignore                  # Excludes data files
└── README.md
```
---

## Mass Appraisal Statistics

The pipeline calculates standard mass appraisal uniformity metrics as governed by the International Association of Assessing Officers (IAAO):

> **Sales Ratio ($R_i$)**
>
> $R_i = \displaystyle\frac{\text{Assessed (or Just) Value}}{\text{Sale Price}}$

> **Coefficient of Dispersion:** Measures appraisal uniformity, and is calcuated as the average absolute deviation of individual assessment-to-sales ratios from the median ratio, expressed as a percentage of that median.
>
> $\displaystyle COD = \frac{\Large\frac{\sum{|R_i - Median|}}{n}}{Median}\times100$


> **Price-Related Differential:** Measures vertical equity (regressivity vs. progressivity), and is calculated by dividing the mean assessment-to-sales ratio by the price-weighted mean assessment ratio.
> 
> $\displaystyle PRD = \frac{Mean Ratio}{Weighted Mean Ratio}$
>
>Where $Mean Ratio =$ The sum of all individual property assessment ratios divided by the number of properties,  
>and $Weighted Mean Ratio =$ The total sum of all assessed values divided by the total sum of all sale prices. 

---

## Data Dictionary

For full table schemas, column definitions and transformation logic, please refer to **`data_dictionary.md`**.