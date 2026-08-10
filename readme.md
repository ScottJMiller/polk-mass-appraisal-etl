# Polk County Mass Appraisal and Valuation Intelligence Pipeline

An end-to-end data engineering pipeline based on my research into the operational needs of the Polk County Property Appraiser (PCPA).

I built this pipeline to demonstrate my abilities as a Data Scientist.

---

## Data Architecture & Flow

``` text
[INPUT]: Downloaded NAL, GIS, CAMA, and Permit Data ──┐
                                                      |
     ┌─ Staging Tables ───────────────────────────────┘
     |  |
     │  ├─ NAL Data (stg_nal):
     |  |       clean_parcel_id (PK), raw_parcel_id, just_value,
     |  |       assessed_val_school, taxable_val_school, land_value,
     |  |       dor_use_code, pa_use_code, actual_year_built,
     │  │       effective_year_built, living_area_sqft, building_count,
     |  |       property_address, property_city, property_zip,
     │  │       neighborhood_code
     |  |
     │  ├─ GIS Data (stg_gis): clean_parcel_id (PK), longitude, latitude
     |  |
     │  ├─ Subdivision Reference Data (stg_sub): sub_number (FK), sub_name
     |  |
     │  └─ CAMA Data
     |     |
     │     ├─ Sales (stg_sales):
     |     |     clean_parcel_id (PK), sale_date, sale_price, trans_code,
     │     │     instrument_type, deed_book, deed_page, grantor, grantee
     |     |
     │     ├─ Building Specs (stg_bldg):
     |     |     clean_parcel_id (PK), bld_num, bld_type_desc, bld_style_desc,
     │     │     stories, year_built, effective_year, living_area,
     │     │     total_roof_area, exterior_wall_desc
     |     |
     │     └─ Permit Logs (stg_permits):
     |           clean_parcel_id (PK), permit_number, issue_date, permit_val,
     │           permit_type, permit_status, permit_status_desc
     |
     ┴
DuckDB Transformation:
* Standardize Parcel Keys
* Compute Sales Ratio
* Build Star Schema for Power BI Dashboard
* Store as ZSTD Parquet Files
     ┬
     |
     |
     └─ [OUT]: Final Schema
          |
          ├─ Sales Ratio and Ranks Fact Table (fact_sales_ratio):
          |  |
          │  ├─ From stg_sales: clean_parcel_id (PK), sale_date (and year),
          |  |                  sale_price, trans_code, instrument_type
          |  |
          │  ├─ From stg_nal: clean_parcel_id (PK), property_city,
          |  |                property_zip, dor_use_code, neighborhood_code,
          |  |                just_value, living_area_sqft
          |  |
          │  └─ From stg_gis: clean_parcel_id (PK), latitude, longitude
          |
          ├─ Permit Discrepancies Fact Table (fact_permits_audit):
          |  |
          │  ├─ From stg_permits: clean_parcel_id (PK), permit_number,
          |  |                    issue_date, permit_val, permit_type,
          │  │                    permit_status_desc
          |  |
          │  ├─ From stg_nal: clean_parcel_id (PK), property_address,
          |  |                property_city, living_area_sqft, just_value
          |  |
          │  └─ From stg_gis: clean_parcel_id (PK), latitude, longitude
          |
          └─ Parcel Profile Dimension Table (dim_parcels):
             |
             ├─ From stg_nal: clean_parcel_id (PK), raw_parcel_id,
             |                property_address, property_city, property_zip,
             │                dor_use_code, pa_use_code, neighborhood_code,
             |                actual_year_built, effective_year_built,
             │                living_area_sqft, building_count, just_value,
             |                assessed_val_school, taxable_val_school,
             │                land_value
             |
             ├─ From stg_sub: clean_parcel_id (PK), sub_number (FK), sub_name
             |
             ├─ From stg_bldg: bld_style_desc, exterior_wall_desc,
             |                 bld_num (restricted JOIN to primary structure
             |                 only)
             |
             └─ From stg_gis: clean_parcel_id (PK), longitude, latitude
```

## Performance

Executing on an in-memory DuckDB engine:

Console Output: ```etl_pipeline.py```

``` text
************
ETL PIPELINE
************

Getting DuckDB Spatial Extension
Processing NAL Data
  stg_nal: Raw = 435,650 rows, Staged = 435,650 rows (100.00%)
Processing GIS Data
  stg_gis: Raw = 435,954 rows, Staged = 435,954 rows (100.00%)
Processing Subdivision Reference Data
  stg_sub: Raw = 8,057 rows, Staged = 8,057 rows (100.00%)
Processing CAMA Data: Sales
  stg_sales: Raw = 3,022,285 rows, Staged = 3,022,285 rows (100.00%)
Processing CAMA Data: Building Specs
  stg_bldg: Raw = 323,870 rows, Staged = 323,761 rows (99.97%)
Processing CAMA Data: Permit Logs
  stg_permits: Raw = 530,764 rows, Staged = 530,650 rows (99.98%)
Building Analytical Fact and Dimension Tables
Table: fact_sales_ratio, File: ./data/parquet/fact_sales_ratio.parquet
Rows: 1,512,349, Columns: 15
Columns: clean_parcel_id, property_city, property_zip, dor_use_code, neighborhood_code, sale_date, sale_year, sale_price, just_value, living_area_sqft, sales_ratio, trans_code, instrument_type, latitude, longitude
Table: fact_permit_audit, File: ./data/parquet/fact_permit_audit.parquet
Rows: 25,662, Columns: 12
Columns: clean_parcel_id, property_address, property_city, permit_number, issue_date, permit_val, permit_type, permit_status_desc, living_area_sqft, just_value, latitude, longitude
Table: dim_parcels, File: ./data/parquet/dim_parcels.parquet
Rows: 435,650, Columns: 22
Columns: clean_parcel_id, raw_parcel_id, property_address, property_city, property_zip, dor_use_code, pa_use_code, neighborhood_code, sub_code, sub_name, actual_year_built, effective_year_built, living_area_sqft, building_count, just_value, assessed_val_school, taxable_val_school, land_value, bld_style_desc, exterior_wall_desc, latitude, longitude
***********************************************
ETL Pipeline completed in 19.52 seconds
Parquet files written to ./data/parquet
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

## Project Repository Structure

```text
.
├── scripts/
│   ├── etl_pipeline.py ───────> DuckDB & Pandas ETL
│   └── deploy_to_homelab.sh ──> bash deployment script
├── docs/
│   └── data_dictionary.md ────> Star Schema field definitions
├── requirements.txt ──────────> Python dependencies
├── .gitignore ────────────────> Excludes data files
└── README.md ─────────────────> This documentation