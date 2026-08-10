"""
ETL Pipeline for Mass Appraisal Data

IN: NAL, GIS, CAMA, and Permit Data
Standardize parcel keys and compute IAAO appraisal metrics
OUT: Parquet Files

(See readme.md and data_dictionary.md for details)
"""

import os
import time
import duckdb
import pandas as pd

RAW_DATA_DIR = "./data/raw"
TEMP_DIR = f"{RAW_DATA_DIR}/temp"
PARQUET_OUT_DIR = "./data/parquet"
DB_FILE = ":memory:"

def save_and_log_staging(con, table_name, raw_count):
    stg_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    pct = (stg_count / raw_count) * 100 if raw_count > 0 else 0.0
    print(f"  {table_name}: Raw = {raw_count:,} rows, Staged = {stg_count:,} rows ({pct:.2f}%)")

    temp_path = f"{TEMP_DIR}/{table_name}.parquet"
    con.execute(f"COPY {table_name} TO '{temp_path}' (FORMAT PARQUET, COMPRESSION ZSTD);")

def export_and_log_final(con, query_sql, table_name, out_path):
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query_sql}")

    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    cols_info = con.execute(f"DESCRIBE {table_name}").fetchall()
    col_names = [col[0] for col in cols_info]
    col_count = len(col_names)

    print(f"Table: {table_name}, File: {out_path}")
    print(f"Rows: {row_count:,}, Columns: {col_count}")
    print(f"Columns: {', '.join(col_names)}")

    con.execute(f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD);")

def run_etl():
    start_time = time.time()
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(PARQUET_OUT_DIR, exist_ok=True)
    con = duckdb.connect(DB_FILE)
    con.execute("SET enable_progress_bar = true;")

    print(f"{'*' * 12}\nETL PIPELINE\n{'*' * 12}\n")

    con = duckdb.connect(DB_FILE)

    # Need DuckDB Spatial Extension for GIS Data
    print("Getting DuckDB Spatial Extension")
    con.execute("INSTALL spatial; LOAD spatial")

    print("Processing NAL Data")
    nal_file = f"{RAW_DATA_DIR}/NAL63P202601.csv"
    raw_nal_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{nal_file}', header=True, ignore_errors=True)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE stg_nal AS
    SELECT
       REPLACE(REPLACE(TRIM(CAST(PARCEL_ID AS VARCHAR)), '-', ''), ' ', '') AS clean_parcel_id,
       TRIM(CAST(PARCEL_ID AS VARCHAR)) AS raw_parcel_id,
       TRY_CAST(JV AS DOUBLE) AS just_value,
       TRY_CAST(AV_SD AS DOUBLE) AS assessed_val_school,
       TRY_CAST(TV_SD AS DOUBLE) AS taxable_val_school,
       TRY_CAST(LND_VAL AS DOUBLE) AS land_value,
       DOR_UC AS dor_use_code,
       PA_UC AS pa_use_code,
       TRY_CAST(ACT_YR_BLT AS INTEGER) AS actual_year_built,
       TRY_CAST(EFF_YR_BLT AS INTEGER) AS effective_year_built,
       TRY_CAST(TOT_LVG_AREA AS DOUBLE) AS living_area_sqft,
       TRY_CAST(NO_BULDNG AS INTEGER) AS building_count,
       PHY_ADDR1 AS property_address,
       PHY_CITY AS property_city,
       TRY_CAST(PHY_ZIPCD AS VARCHAR) AS property_zip,
       NBRHD_CD AS neighborhood_code
    FROM read_csv_auto('{nal_file}', header=True, ignore_errors=True);
    """)
    save_and_log_staging(con, "stg_nal", raw_nal_count)

    print("Processing GIS Data")
    gis_file = f"{RAW_DATA_DIR}/polk_2026Ppar/polk_2026Ppar.shp"
    raw_gis_count = con.execute(
        f"SELECT COUNT(*) FROM ST_Read('{gis_file}')"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE stg_gis AS
    SELECT
       REPLACE(REPLACE(TRIM(CAST(PARCEL_ID AS VARCHAR)), '-', ''), ' ', '') AS clean_parcel_id,
       ST_X(ST_Centroid(geom)) AS longitude,
       ST_Y(ST_Centroid(geom)) AS latitude
    FROM ST_Read('{gis_file}');
    """)
    save_and_log_staging(con, "stg_gis", raw_gis_count)

    print("Processing Subdivision Reference Data")
    sub_file = f"{RAW_DATA_DIR}/ftp_sub.csv"
    with open(sub_file, 'r', encoding='utf-8', errors='ignore') as f:
        raw_sub_count = max(0, sum(1 for _ in f) - 1)

    df_sub = pd.read_csv(
        sub_file,
        encoding='utf-8',
        on_bad_lines='skip',
        dtype=str
        )
    con.register("stg_sub_raw", df_sub)

    con.execute(f"""
    CREATE OR REPLACE TABLE stg_sub AS
    SELECT
       TRIM(CAST("SUB NUMBER" AS VARCHAR)) AS sub_number,
       TRIM(NAME) AS sub_name
    FROM stg_sub_raw;
    """)
    save_and_log_staging(con, "stg_sub", raw_sub_count)

    print("Processing CAMA Data: Sales")
    sales_file = f"{RAW_DATA_DIR}/ftp_sales.csv"
    raw_sales_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{sales_file}', header=True, ignore_errors=True)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE stg_sales AS
    SELECT
        REPLACE(REPLACE(TRIM(CAST(PARCEL_ID AS VARCHAR)), '-', ''), ' ', '') AS clean_parcel_id,
        TRY_CAST(SALEDT AS DATE) AS sale_date,
        TRY_CAST(PRICE AS DOUBLE) AS sale_price,
        TRNS_CD AS trans_code,
        INSTRTYP AS instrument_type,
        BOOK AS deed_book,
        PAGE AS deed_page,
        GRANTOR AS grantor,
        GRANTEE AS grantee
    FROM read_csv_auto('{sales_file}', header=True, ignore_errors=True);
    """)
    save_and_log_staging(con, "stg_sales", raw_sales_count)

    print("Processing CAMA Data: Building Specs")
    bldg_file = f"{RAW_DATA_DIR}/ftp_bldg.csv"
    with open(bldg_file, 'r', encoding='utf-8', errors='ignore') as f:
        raw_bldg_count = max(0, sum(1 for _ in f) - 1)

    df_bldg = pd.read_csv(
        bldg_file,
        encoding='utf-8',
        on_bad_lines='skip',
        dtype=str
        )
    con.register("stg_bldg_raw", df_bldg)

    con.execute(f"""
    CREATE OR REPLACE TABLE stg_bldg AS
    SELECT
        REPLACE(REPLACE(TRIM(CAST(PARCEL_ID AS VARCHAR)), '-', ''), ' ', '') AS clean_parcel_id,
        TRY_CAST(BLD_NUM AS INTEGER) AS bld_num,
        IMPR_TYPE_DESC AS bld_type_desc,
        STYLE_DESC AS bld_style_desc,
        TRY_CAST(STORIES AS DOUBLE) AS stories,
        TRY_CAST(YEARBUILT AS INTEGER) AS year_built,
        TRY_CAST(EFF_YEAR AS INTEGER) AS effective_year,
        TRY_CAST(LIVINGAREA AS DOUBLE) AS living_area,
        TRY_CAST(TOTALUNDERROOF AS DOUBLE) AS total_roof_area,
        EXWALLDESC AS exterior_wall_desc
    FROM stg_bldg_raw;
    """)
    save_and_log_staging(con, "stg_bldg", raw_bldg_count)

    print("Processing CAMA Data: Permit Logs")
    permit_file = f"{RAW_DATA_DIR}/ftp_permit.csv"
    with open(permit_file, 'r', encoding='utf-8', errors='ignore') as f:
        raw_permit_count = max(0, sum(1 for _ in f) - 1)

    df_permits = pd.read_csv(
        permit_file,
        encoding='utf-8',
        on_bad_lines='skip',
        dtype=str
        )
    con.register("stg_permits_raw", df_permits)

    con.execute(f"""
    CREATE OR REPLACE TABLE stg_permits AS
    SELECT
        REPLACE(REPLACE(TRIM(CAST(PARCEL_ID AS VARCHAR)), '-', ''), ' ', '') AS clean_parcel_id,
        PERMIT_NUM AS permit_number,
        TRY_CAST(ISSUE_DT AS TIMESTAMP):: DATE AS issue_date,
        TRY_CAST(EST_VAL AS DOUBLE) AS permit_val,
        PERMIT_TYPE AS permit_type,
        STATUS AS permit_status,
        STATUS_DSCR AS permit_status_desc
    FROM stg_permits_raw;
    """)
    save_and_log_staging(con, "stg_permits", raw_permit_count)

    print("Building Analytical Fact and Dimension Tables")

    # Table 1: Sales Ratio Study
    sql_sales_ratio = """
        SELECT
            s.clean_parcel_id,
            n.property_city,
            n.property_zip,
            n.dor_use_code,
            n.neighborhood_code,
            s.sale_date,
            YEAR(s.sale_date) AS sale_year,
            s.sale_price,
            n.just_value,
            n.living_area_sqft,
            (n.just_value / NULLIF(s.sale_price, 0)) AS sales_ratio,
            s.trans_code,
            s.instrument_type,
            g.latitude,
            g.longitude,
        FROM stg_sales s
        JOIN stg_nal n ON s.clean_parcel_id = n.clean_parcel_id
        LEFT JOIN stg_gis g ON s.clean_parcel_id = g.clean_parcel_id
        WHERE s.sale_price >= 10000
    """
    export_and_log_final(con, sql_sales_ratio, "fact_sales_ratio", f"{PARQUET_OUT_DIR}/fact_sales_ratio.parquet")

    # Table 2: Permit Audit and Unappraised Improvements
    sql_permit_audit = """
        SELECT 
            p.clean_parcel_id,
            n.property_address,
            n.property_city,
            p.permit_number,
            p.issue_date,
            p.permit_val,
            p.permit_type,
            p.permit_status_desc,
            n.living_area_sqft,
            n.just_value,
            g.latitude,
            g.longitude
        FROM stg_permits p
        JOIN stg_nal n ON p.clean_parcel_id = n.clean_parcel_id
        LEFT JOIN stg_gis g ON p.clean_parcel_id = g.clean_parcel_id
        WHERE p.permit_val >= 25000 AND p.issue_date >= '2024-01-01'
    """
    export_and_log_final(con, sql_permit_audit, "fact_permit_audit", f"{PARQUET_OUT_DIR}/fact_permit_audit.parquet")

    # Table 3: Master Parcel Profile
    sql_dim_parcels = """
        SELECT 
            n.clean_parcel_id,
            n.raw_parcel_id,
            n.property_address,
            n.property_city,
            n.property_zip,
            n.dor_use_code,
            n.pa_use_code,
            n.neighborhood_code,
            SUBSTRING(n.clean_parcel_id, 7, 6) AS sub_code,
            s.sub_name,
            n.actual_year_built,
            n.effective_year_built,
            n.living_area_sqft,
            n.building_count,
            n.just_value,
            n.assessed_val_school,
            n.taxable_val_school,
            n.land_value,
            b.bld_style_desc,
            b.exterior_wall_desc,
            g.latitude,
            g.longitude
        FROM stg_nal n
        LEFT JOIN stg_bldg b ON n.clean_parcel_id = b.clean_parcel_id AND b.bld_num = 1
        LEFT JOIN stg_gis g ON n.clean_parcel_id = g.clean_parcel_id
        LEFT JOIN (
            SELECT sub_number, FIRST(sub_name) AS sub_name 
            FROM stg_sub 
            WHERE sub_number IS NOT NULL AND sub_number != ''
            GROUP BY sub_number
        ) s ON SUBSTRING(n.clean_parcel_id, 7, 6) = s.sub_number
    """
    export_and_log_final(con, sql_dim_parcels, "dim_parcels", f"{PARQUET_OUT_DIR}/dim_parcels.parquet")

    elapsed = round(time.time() - start_time, 2)
    print(f"{'*' * 47}\nETL Pipeline completed in {elapsed} seconds")
    print(f"Parquet files written to {PARQUET_OUT_DIR}")

if __name__ == "__main__":
    run_etl()