"""
ETL Pipeline for Mass Appraisal Data

This script orchestrates the ETL process for mass appraisal data, including:
- Extracting data from raw CSV and GIS files
- Transforming and cleaning the data
- Loading the data into DuckDB for further analysis in Power BI

(See readme.md and data_dictionary.md for details)
"""

import os
import time
import duckdb
import pandas as pd

RAW_DATA_DIR = "./data/raw"
LOOKUP_DIR = f"{RAW_DATA_DIR}/lookups"
TEMP_DIR = f"{RAW_DATA_DIR}/temp"
PARQUET_OUT_DIR = "./data/parquet"
LOOKUP_OUT_DIR = f"{PARQUET_OUT_DIR}/lookups"
DB_FILE = ":memory:"

def save_and_log_staging(con, table_name, raw_count):
    stg_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    pct = (stg_count / raw_count) * 100 if raw_count > 0 else 0.0

    if "lup" in table_name:
        print(f"  🖫 {table_name}: {stg_count:,} of {raw_count:,} rows saved ({pct:.2f}%)")
        temp_path = f"{LOOKUP_OUT_DIR}/{table_name}.parquet"
    else:
        print(f"    {table_name}: {stg_count:,} of {raw_count:,} rows staged ({pct:.2f}%)")
        temp_path = f"{TEMP_DIR}/{table_name}.parquet"

    con.execute(f"COPY {table_name} TO '{temp_path}' (FORMAT PARQUET, COMPRESSION ZSTD);")

def export_and_log_final(con, query_sql, orig_table, table_name, out_path):
    con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {query_sql}")

    orig_count = con.execute(f"SELECT COUNT(*) FROM {orig_table}").fetchone()[0]
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    cols_info = con.execute(f"DESCRIBE {table_name}").fetchall()
    col_names = [col[0] for col in cols_info]
    col_count = len(col_names)

    print(f"▷ Table: {table_name}, File: {out_path}")
    print(f"    Rows: {row_count:,}, Columns: {col_count}")
    if row_count > orig_count:
        print(f"    !Possible Row Fan-Out! {row_count - orig_count:,} ({((row_count - orig_count) / orig_count * 100) if orig_count > 0 else 0:.2f}%)")
    else:
        print("    Row Count 🆗")
    #print(f"Columns: {', '.join(col_names)}\n")

    con.execute(f"COPY {table_name} TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD);")

def run_etl():
    start_time = time.time()
    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(PARQUET_OUT_DIR, exist_ok=True)
    os.makedirs(LOOKUP_OUT_DIR, exist_ok=True)
    con = duckdb.connect(DB_FILE)
    con.execute("SET enable_progress_bar = true;")

    print(f"{'◟◞◜◝' * 6}\n      ETL PIPELINE\n{'◟◞◜◝' * 6}")

    con = duckdb.connect(DB_FILE)

    # Need DuckDB Spatial Extension for GIS Data
    try:
        con.execute("LOAD spatial")
    except Exception:
        print("Getting DuckDB Spatial Extension")
        con.execute("INSTALL spatial; LOAD spatial")

    # Need DuckDB Markdown Extension for Markdown Table Input
    try:
        con.execute("LOAD markdown")
    except Exception:
        print("Getting DuckDB Markdown Extension")
        con.execute("INSTALL markdown FROM community; LOAD markdown")

    #                                     ◢◤
    #                                   ◢◤
    #           CREATE                ◢◤
    #           AND STAGE           ◢◤___________________
    #           PRIMARY             ◥◣‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
    #           TABLES                ◥◣
    #                                   ◥◣
    #                                     ◥◣

    print(f"{'⎵' * 44}\nStaging Analytical Fact and Dimension Tables\n{'⎴' * 44}")
    print("▷ Processing NAL63P202601.csv: DOR NAL Data")
    nal_file = f"{RAW_DATA_DIR}/NAL63P202601.csv"
    raw_nal_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{nal_file}', header=True)"
        ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE stg_nal AS
    SELECT
        TRY_CAST(PARCEL_ID AS VARCHAR) AS parcel_id,
        TRY_CAST(ASMNT_YR AS VARCHAR) AS asmnt_year,
        TRY_CAST(ASMNT_YR AS INTEGER) AS asmnt_year,
        BAS_STRT AS basic_stratum,
        ATV_STRT AS active_stratum,
        TRY_CAST(GRP_NO AS VARCHAR) AS group_num,
        
        TRY_CAST(JV AS DOUBLE) AS jv_dor,
        TRY_CAST(JV_CHNG AS DOUBLE) AS jv_dor_change,
        TRY_CAST(JV_CHNG_CD AS VARCHAR) AS jv_dor_change_code,
        TRY_CAST(NCONST_VAL AS DOUBLE) AS new_const_val_last_yr,
        TRY_CAST(DEL_VAL AS DOUBLE) AS new_delete_val_last_yr,
        
        TRY_CAST(SPASS_CD AS VARCHAR) AS spass_code,
        TRY_CAST(PAR_SPLT AS VARCHAR) AS parcel_split,
        TRY_CAST(DISTR_CD AS VARCHAR) AS disaster_code,
        TRY_CAST(DISTR_YR AS INTEGER) AS disaster_year,
        TRY_CAST(DT_LAST_INSPT AS INTEGER) AS last_inspect_date,
        TRY_CAST(IMP_QUAL AS VARCHAR) AS impvmt_quality,
        TRY_CAST(MKT_AR AS VARCHAR) AS market_area_code,
        TRY_CAST(PUBLIC_LND AS VARCHAR) AS public_land,
    FROM read_csv_auto(
        '{nal_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False
    );
    """)
    save_and_log_staging(con, "stg_nal", raw_nal_count)

    print("▷ Processing Polk County GIS Parcel Data")
    gis_file = f"{RAW_DATA_DIR}/parcel/parcel.shp"
    raw_gis_count = con.execute(
        f"SELECT COUNT(*) FROM ST_Read('{gis_file}')"
        ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE stg_gis AS
    SELECT
       TRY_CAST(PARCELID AS VARCHAR) AS parcel_id,
       ST_X(ST_Centroid(geom)) AS longitude,
       ST_Y(ST_Centroid(geom)) AS latitude
    FROM ST_Read('{gis_file}');
    """)
    save_and_log_staging(con, "stg_gis", raw_gis_count)

    print("▷ Processing ftp_bldg.txt: Polk PA Data")
    bldg_file = f"{RAW_DATA_DIR}/ftp_bldg.txt"
    raw_bldg_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{bldg_file}', header=True, strict_mode=False)"
    ).fetchone()[0]
    
    con.execute(f"""
    CREATE OR REPLACE TABLE stg_bldg AS
    SELECT
        TRY_CAST(PARCEL_ID AS VARCHAR) AS parcel_id,
        TRY_CAST(BLD_NUM AS INTEGER) AS bld_num,
        TRY_CAST(STYLE AS VARCHAR) AS bldg_style,
        TRY_CAST(STORIES AS VARCHAR) AS bldg_stories,
        TRY_CAST(BLDSHAPE AS VARCHAR) AS bldg_shape,
        TRY_CAST(CLASS AS VARCHAR) AS bldg_class,
        BATH AS num_bathrooms,
        TRY_CAST(UNITS AS VARCHAR) AS total_units,
        BEDROOM AS num_bedrooms,
        FIREPLACE AS num_fireplaces,
        TRY_CAST(SUBSTRUCT AS VARCHAR) AS bldg_foundation_type,
        TRY_CAST(FRAME AS VARCHAR) AS bldg_frame_type,
        EFF_YEAR AS effective_year,
        YEARBUILT AS actual_year,
        TRY_CAST(EXTWALL AS VARCHAR) AS bldg_ext_wall_type,
        TRY_CAST(ROOFTYPE AS VARCHAR) AS bldg_roof_type,
        TRY_CAST(FLOORTYPE AS VARCHAR) AS bldg_floor_type,
        TRY_CAST(INTWALLS AS VARCHAR) AS bldg_int_wall_type,
        LIVINGAREA AS total_living_area,
        TOTALUNDERROOF AS total_under_roof,
    FROM read_csv_auto(
        '{bldg_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False
    );
    """)
    save_and_log_staging(con, "stg_bldg", raw_bldg_count)

    print("▷ Processing ftp_land.txt: Polk PA Data")
    land_file = f"{RAW_DATA_DIR}/ftp_land.txt"
    raw_land_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{land_file}', header=True, strict_mode=False)"
    ).fetchone()[0]
    
    con.execute(f"""
    CREATE OR REPLACE TABLE stg_land AS
    SELECT
        TRY_CAST(PARCEL_ID AS VARCHAR) AS parcel_id,
        TRY_CAST(LINENUM AS INTEGER) AS land_line_num,
        TRY_CAST(LND_TP AS VARCHAR) AS land_class,
        TRY_CAST(USECODE AS VARCHAR) AS land_use_code,
        TRY_CAST(USEDESC AS VARCHAR) AS land_uc_desc,
        TRY_CAST(FRONTAGE AS DECIMAL(12,2)) AS land_frontage,
        TRY_CAST(DEPTH AS DECIMAL(12,2)) AS land_depth,
        TRY_CAST(UNITS AS DECIMAL(20,4)) AS land_units,
        TRY_CAST(UNITTYPE AS VARCHAR) AS land_unit_type,
        TRY_CAST(UNITTPDSCR AS VARCHAR) AS land_unit_desc
    FROM read_csv_auto(
        '{land_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False
    );
    """)
    save_and_log_staging(con, "stg_land", raw_land_count)

    print("▷ Processing ftp_misc.txt: Polk PA Data")
    misc_file = f"{RAW_DATA_DIR}/ftp_misc.txt"
    raw_misc_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{misc_file}', header=True, strict_mode=False)"
    ).fetchone()[0]
    
    con.execute(f"""
    CREATE OR REPLACE TABLE stg_misc AS
    SELECT
        TRY_CAST(PARCEL_ID AS VARCHAR) AS parcel_id,
        TRY_CAST(BLDGNUM AS INTEGER) AS bld_num,
        TRY_CAST(LINENUM AS INTEGER) AS xfeature_num,
        TRY_CAST(CODE AS VARCHAR) AS xfeat_code,
        TRY_CAST(YRBUILT AS INTEGER) AS xfeat_yr_blt,
        TRY_CAST(LNGTH AS INTEGER) AS xfeat_length,
        TRY_CAST(WDTH AS INTEGER) AS xfeat_width
    FROM read_csv_auto(
        '{misc_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False
    );
    """)
    save_and_log_staging(con, "stg_misc", raw_misc_count)

    print("▷ Processing ftp_parcel.txt: Polk PA Data")
    parcel_file = f"{RAW_DATA_DIR}/ftp_parcel.txt"
    raw_parcel_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{parcel_file}', header=True, strict_mode=False)"
    ).fetchone()[0]
    
    con.execute(f"""
    CREATE OR REPLACE TABLE stg_parcel AS
    SELECT
        TRY_CAST(PARCEL_ID AS VARCHAR) AS parcel_id,
        TRY_CAST(DORUS_CODE AS VARCHAR) AS dor_use_code,
        TRY_CAST(NH_CD AS DECIMAL(10,2)) AS nbrhd_code,
        TRY_CAST(NH_DSCR AS VARCHAR) AS nbrhd_desc,
        TRY_CAST(HOMESTEAD AS DECIMAL(10,2)) AS homestead,
        TRY_CAST(CLS_LND_VAL AS INTEGER) AS ag_land_value,
        TRY_CAST(AG_CLASS AS VARCHAR) AS agri_class,
        TRY_CAST(VALUETYPE AS VARCHAR) AS valuation_code,
        TRY_CAST(VALUEDESC AS VARCHAR) AS valuation_desc,
        TRY_CAST(TOT_LND_VAL AS INTEGER) AS appr_land_value,
        TRY_CAST(TOT_BLD_VAL AS INTEGER) AS appr_bldg_value,
        TRY_CAST(TOT_XF_VAL AS INTEGER) AS appr_ext_feat_value,
        TRY_CAST(TOTALVAL AS INTEGER) AS total_value,
        TRY_CAST(RECONCILE AS VARCHAR) AS on_reconcile,
        TRY_CAST(ASSESSVAL AS DECIMAL(10,2)) AS assessed_value,
        TRY_CAST(TAXVAL AS DECIMAL(10,2)) AS base_tax_value,
        TRY_CAST(YR_CREATED AS INTEGER) AS year_created,
        TRY_CAST(YR_IMPROVED AS INTEGER) AS year_improved,
        TRY_CAST(LAST_INSP_DT AS TIMESTAMP) AS last_insp_date,
        TRY_CAST(TOT_ACREAGE AS DECIMAL(12,4)) AS total_acreage,
        TRY_CAST(PR_STRAP AS VARCHAR) AS prior_parcel_id
    FROM read_csv_auto(
        '{parcel_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False
    );
    """)
    save_and_log_staging(con, "stg_parcel", raw_parcel_count)

    print("▷ Processing ftp_permit.txt: Polk PA Data")
    permit_file = f"{RAW_DATA_DIR}/ftp_permit.txt"
    raw_permit_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{permit_file}', header=True, strict_mode=False)"
    ).fetchone()[0]
    
    con.execute(f"""
    CREATE OR REPLACE TABLE stg_permit AS
    SELECT
        TRY_CAST(PARCEL_ID AS VARCHAR) AS parcel_id,
        TRY_CAST(ID AS INTEGER) AS permit_id,
        TRY_CAST(AGENCY_NAME AS VARCHAR) AS permit_agency,
        TRY_CAST(PERMIT_NUM AS VARCHAR) AS permit_num,
        TRY_CAST(STATUS AS CHAR) AS permit_status,
        TRY_CAST(DSCR AS VARCHAR) AS permit_desc,
        TRY_CAST(PERMIT_TYPE AS VARCHAR) AS permit_type,
        TRY_CAST(ISSUE_DT AS TIMESTAMP) AS issue_date,
        TRY_CAST(FINAL_DT AS TIMESTAMP) AS final_insp_date,
        TRY_CAST(EST_VAL AS INTEGER) AS est_value,
        TRY_CAST(SITE_NUM AS VARCHAR) AS site_street_num,
        TRY_CAST(SITE_PFX AS CHAR) AS site_street_pfx,
        TRY_CAST(SITE_STR AS VARCHAR) AS site_street_name,
        TRY_CAST(SITE_TP AS VARCHAR) AS site_street_sfx,
        TRY_CAST(SITE_APT AS VARCHAR) AS site_apt_num
    FROM read_csv_auto(
        '{permit_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False
    );
    """)
    save_and_log_staging(con, "stg_permit", raw_permit_count)

    print("▷ Processing ftp_sales.txt: Polk PA Data")
    sales_file = f"{RAW_DATA_DIR}/ftp_sales.txt"
    raw_sales_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{sales_file}', header=True, strict_mode=False)"
    ).fetchone()[0]
    
    con.execute(f"""
    CREATE OR REPLACE TABLE stg_sales AS
    SELECT
        TRY_CAST(PARCEL_ID AS VARCHAR) AS parcel_id,
        TRY_CAST(LN_NUM AS INTEGER) AS sale_line_num,
        TRY_CAST(SALEDT AS TIMESTAMP) AS sale_date,
        TRY_CAST(PRICE AS INTEGER) AS sale_price,
        TRY_CAST(BOOK AS VARCHAR) AS book_num,
        TRY_CAST(PAGE AS VARCHAR) AS page_num,
        TRY_CAST(SALETYPE AS CHAR) AS sale_type,
        TRY_CAST(TRNS_CD AS VARCHAR) AS transact_code,
        TRY_CAST(INSTRTYP AS VARCHAR) AS instr_type,
        TRY_CAST(INSTRTYP_DSCR AS VARCHAR) AS instr_type_dscr
    FROM read_csv_auto(
        '{sales_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False
    );
    """)
    save_and_log_staging(con, "stg_sales", raw_sales_count)

    print("▷ Processing ftp_site.txt: Polk PA Data")
    site_file = f"{RAW_DATA_DIR}/ftp_site.txt"
    raw_site_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{site_file}', header=True, strict_mode=False)"
    ).fetchone()[0]
    
    con.execute(f"""
    CREATE OR REPLACE TABLE stg_site AS
    SELECT
        TRY_CAST(PARCEL_ID AS VARCHAR) AS parcel_id,
        TRY_CAST(LN_NUM AS INTEGER) AS site_address_id,
        TRY_CAST(BLD_NUM AS INTEGER) AS bld_num,
        TRY_CAST(STR AS VARCHAR) AS street,
        TRY_CAST(STR_PFX AS CHAR) AS street_prefix,
        TRY_CAST(STR_NUM AS INTEGER) AS street_number,
        TRY_CAST(STR_NUM_SFX AS CHAR) AS street_num_suffix,
        TRY_CAST(STR_SFX AS VARCHAR) AS street_suffix,
        TRY_CAST(STR_SFX_DIR AS CHAR) AS street_sfx_direction,
        TRY_CAST(STR_UNIT AS VARCHAR) AS street_unit,
        TRY_CAST(ZIP AS VARCHAR) AS zip_code,
        TRY_CAST(CITY AS VARCHAR) AS city
    FROM read_csv_auto(
        '{site_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False
    );
    """)
    save_and_log_staging(con, "stg_site", raw_site_count)  

    #                                     ◢◤
    #                                   ◢◤
    #           CREATE                ◢◤
    #           AND SAVE            ◢◤___________________
    #           LOOKUP              ◥◣‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
    #           TABLES                ◥◣
    #                                   ◥◣
    #                                     ◥◣

    print(f"{'⎵' * 22}\nCreating Lookup Tables\n{'⎴' * 22}")

    active_stratum_lookup_file = f"{LOOKUP_DIR}/active_stratum.txt"
    raw_active_stratum_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{active_stratum_lookup_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE active_stratum_lup AS
    SELECT
        Stratum_ AS stratum,
        TRY_CAST(TRIM(Definition_) AS VARCHAR) AS definition,
        TRY_CAST(TRIM(Included_in_Statistical_Analysis) AS BOOLEAN) AS incl_in_stat_analysis
    FROM read_csv_auto(
        '{active_stratum_lookup_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "active_stratum_lup", raw_active_stratum_count)

    bld_class_codes_file = f"{LOOKUP_DIR}/bld_class_codes.txt"
    raw_bld_class_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{bld_class_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE bld_class_codes_lup AS
    SELECT
        TRY_CAST(TRIM(Code) AS VARCHAR) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{bld_class_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "bld_class_codes_lup", raw_bld_class_codes_count)

    bld_frame_codes_file = f"{LOOKUP_DIR}/bld_frame_codes.txt"
    raw_bld_frame_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{bld_frame_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE bld_frame_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{bld_frame_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "bld_frame_codes_lup", raw_bld_frame_codes_count)

    bld_shape_codes_file = f"{LOOKUP_DIR}/bld_shape_codes.txt"
    raw_bld_shape_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{bld_shape_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE bld_shape_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{bld_shape_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "bld_shape_codes_lup", raw_bld_shape_codes_count)

    bld_style_codes_file = f"{LOOKUP_DIR}/bld_style_codes.txt"
    raw_bld_style_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{bld_style_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE bld_style_codes_lup AS
    SELECT
        TRY_CAST(TRIM(Code) AS VARCHAR) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{bld_style_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "bld_style_codes_lup", raw_bld_style_codes_count)

    disaster_codes_file = f"{LOOKUP_DIR}/disaster_codes.txt"
    raw_disaster_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{disaster_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE disaster_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Definition) AS VARCHAR) AS definition
    FROM read_csv_auto(
        '{disaster_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "disaster_codes_lup", raw_disaster_codes_count)

    dor_use_codes_file = f"{LOOKUP_DIR}/dor_use_code.csv"
    raw_dor_use_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{dor_use_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE dor_use_codes_lup AS
    SELECT
        LPAD(dor_use_code::VARCHAR, 4, '0') AS dor_use_code,
        TRY_CAST(dor_uc_category AS VARCHAR) AS dor_uc_category,
        TRY_CAST(dor_uc_cat_desc AS VARCHAR) AS dor_uc_cat_desc,
        TRY_CAST(dor_uc_desc AS VARCHAR) AS dor_uc_desc
    FROM read_csv_auto(
        '{dor_use_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False
    );
    """)
    save_and_log_staging(con, "dor_use_codes_lup", raw_dor_use_codes_count)

    extra_feat_codes_file = f"{LOOKUP_DIR}/extra_feat_codes.txt"
    raw_extra_feat_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{extra_feat_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE extra_feat_codes_lup AS
    SELECT
        TRY_CAST(TRIM(Code) AS VARCHAR) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{extra_feat_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "extra_feat_codes_lup", raw_extra_feat_codes_count)

    ext_wall_codes_file = f"{LOOKUP_DIR}/ext_wall_codes.txt"
    raw_ext_wall_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{ext_wall_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE ext_wall_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{ext_wall_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "ext_wall_codes_lup", raw_ext_wall_codes_count)

    floor_type_codes_file = f"{LOOKUP_DIR}/floor_type_codes.txt"
    raw_floor_type_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{floor_type_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE floor_type_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{floor_type_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "floor_type_codes_lup", raw_floor_type_codes_count)

    int_wall_codes_file = f"{LOOKUP_DIR}/int_wall_codes.txt"
    raw_int_wall_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{int_wall_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE int_wall_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{int_wall_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "int_wall_codes_lup", raw_int_wall_codes_count)

    jv_change_codes_file = f"{LOOKUP_DIR}/jv_change_codes.txt"
    raw_jv_change_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{jv_change_codes_file}', header=True, strict_mode=False, delim='|')"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE jv_change_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Definition) AS VARCHAR) AS definition
    FROM read_csv_auto(
        '{jv_change_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "jv_change_codes_lup", raw_jv_change_codes_count)

    permit_stat_cd_file = f"{LOOKUP_DIR}/permit_stat_cd.txt"
    raw_permit_stat_cd_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{permit_stat_cd_file}', header=True, strict_mode=False, delim='|')"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE permit_stat_cd_lup AS
    SELECT
        TRIM(CAST(STATUS_CODE AS VARCHAR)) AS code,
        TRY_CAST(TRIM(DESCRIPTION) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{permit_stat_cd_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "permit_stat_cd_lup", raw_permit_stat_cd_count)

    prop_tran_codes_file = f"{LOOKUP_DIR}/prop_tran_codes.txt"
    raw_prop_tran_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{prop_tran_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE prop_tran_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Definition) AS VARCHAR) AS definition,
        TRY_CAST(TRIM(Qualified_Sale) AS BOOLEAN) AS is_qualified_sale,
        TRY_CAST(TRIM(Incl_Sales_Ratio_Analysis) AS BOOLEAN) AS incl_in_sales_ratio
    FROM read_csv_auto(
        '{prop_tran_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "prop_tran_codes_lup", raw_prop_tran_codes_count)

    pub_land_codes_file = f"{LOOKUP_DIR}/pub_land_codes.txt"
    raw_pub_land_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{pub_land_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE pub_land_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{pub_land_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "pub_land_codes_lup", raw_pub_land_codes_count)

    roof_type_codes_file = f"{LOOKUP_DIR}/roof_type_codes.txt"
    raw_roof_type_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{roof_type_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE roof_type_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{roof_type_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "roof_type_codes_lup", raw_roof_type_codes_count)

    spass_codes_file = f"{LOOKUP_DIR}/spass_codes.txt"
    raw_spass_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{spass_codes_file}', header=True, strict_mode=False, delim='|')"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE spass_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Definition) AS VARCHAR) AS definition
    FROM read_csv_auto(
        '{spass_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "spass_codes_lup", raw_spass_codes_count)

    substruct_codes_file = f"{LOOKUP_DIR}/substruct_codes.txt"
    raw_substruct_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{substruct_codes_file}', header=True, strict_mode=False, delim='|')"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE substruct_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description
    FROM read_csv_auto(
        '{substruct_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "substruct_codes_lup", raw_substruct_codes_count)

    transact_codes_file = f"{LOOKUP_DIR}/transact_codes.txt"
    raw_transact_codes_count = con.execute(
        f"SELECT COUNT(*) FROM read_csv_auto('{transact_codes_file}', header=True, strict_mode=False)"
    ).fetchone()[0]

    con.execute(f"""
    CREATE OR REPLACE TABLE transact_codes_lup AS
    SELECT
        TRIM(CAST(Code AS VARCHAR)) AS code,
        TRY_CAST(TRIM(Description) AS VARCHAR) AS description,
        TRY_CAST(TRIM(Incl_Sales_Ratio_Analysis) AS BOOLEAN) AS incl_in_sales_ratio
    FROM read_csv_auto(
        '{transact_codes_file}',
        header=True,
        ignore_errors=True,
        strict_mode=False,
        delim='|'
    );
    """)
    save_and_log_staging(con, "transact_codes_lup", raw_transact_codes_count)
    print()

    #                                     ◢◤
    #                                   ◢◤
    #           CREATE                ◢◤
    #           AND STAGE           ◢◤___________________
    #           FINAL               ◥◣‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
    #           TABLES                ◥◣
    #                                   ◥◣
    #                                     ◥◣

    print(f"{'⎵' * 45}\nBuilding Analytical Fact and Dimension Tables\n{'⎴' * 45}")

    sql_dim_parcels = """
    WITH joined_parcels AS (
        SELECT
            p.parcel_id,
            SUBSTRING(p.parcel_id, 1, 6) AS rts,
            SUBSTRING(p.parcel_id, 1, 2) AS range_code,
            SUBSTRING(p.parcel_id, 3, 2) AS township_code,
            SUBSTRING(p.parcel_id, 5, 2) AS section_code,
            SUBSTRING(p.parcel_id, 7, 6) AS sub_code,
            SUBSTRING(p.parcel_id, 13, 6) AS parcel_code,
            g.latitude,
            g.longitude,
            p.dor_use_code,
            p.nbrhd_code,
            p.nbrhd_desc,
            p.homestead,
            p.agri_class,
            p.total_acreage,
            p.year_created,
            p.year_improved,
            p.prior_parcel_id,
            n.market_area_code,
            n.public_land,
            p.valuation_code,
            p.valuation_desc,
            p.ag_land_value,
            p.appr_land_value,
            p.appr_bldg_value,
            p.appr_ext_feat_value,
            p.total_value,
            n.jv_dor,
            n.jv_dor_change,
            n.jv_dor_change_code,
            n.new_const_val_last_yr,
            n.new_delete_val_last_yr,
            p.on_reconcile,
            p.assessed_value,
            p.base_tax_value,
            n.asmnt_year,
            n.basic_stratum,
            n.active_stratum,
            n.group_num,
            n.spass_code,
            n.parcel_split,
            n.disaster_code,
            n.disaster_year,
            n.impvmt_quality,
            COALESCE(
                p.last_insp_date, 
                CASE 
                    WHEN LPAD(CAST(n.last_inspect_date AS VARCHAR), 4, '0') = '0000' THEN NULL 
                    ELSE strptime(LPAD(CAST(n.last_inspect_date AS VARCHAR), 4, '0'), '%m%y') 
                END
            ) AS last_insp_date
        FROM stg_parcel p
        LEFT JOIN stg_nal n 
            ON p.parcel_id = n.parcel_id
        LEFT JOIN stg_gis g 
            ON p.parcel_id = g.parcel_id
    ),
    -- Create Strata and Groupings for PA Data
    permit_agg AS (
        SELECT 
            parcel_id,
            SUM(est_value) AS recent_permit_total_value,
            TRUE AS has_recent_final_inspect
        FROM stg_permit 
        WHERE final_insp_date >= (CAST('2026-08-07' AS DATE) - INTERVAL 6 MONTH)
        AND final_insp_date <= CAST('2026-08-07' AS DATE)
        GROUP BY parcel_id
    ),
    base_parcels AS (
        SELECT 
            jp.*,
            COALESCE(pmt.recent_permit_total_value, 0) AS recent_permit_total_value,
            COALESCE(pmt.has_recent_final_inspect, FALSE) AS has_recent_final_inspect,
            CASE
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) NOT IN ('50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65','66','67','68','69','97') 
                    AND COALESCE(pmt.has_recent_final_inspect, FALSE) 
                    AND COALESCE(pmt.recent_permit_total_value, 0) > jp.total_value THEN 11
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) IN ('01', '02', '04', '05') THEN 1
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) IN ('06', '08') THEN 2
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) IN ('50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65','66','67','68','69','97') 
                    AND COALESCE(pmt.has_recent_final_inspect, FALSE) 
                    AND COALESCE(pmt.recent_permit_total_value, 0) > jp.assessed_value THEN 12
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) IN ('50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65','66','67','68','69','97') 
                    AND jp.homestead <= 0 THEN 3
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) IN ('50','51','52','53','54','55','56','57','58','59','60','61','62','63','64','65','66','67','68','69','97') 
                    AND jp.homestead > 0 THEN 9
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) IN ('00', '07') THEN 4
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) IN ('10', '40', '99') THEN 5
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) IN ('03','11','12','13','14','15','16','17','18','19','20','21','22','23','24','25','26','27','28','29','30','31','32','33','34','35','36','37','38','39','41','42','43','44','45','46','47','48','49') THEN 6
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) IN ('70','71','72','73','74','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91','92','93','94','95','96','98') 
                    AND jp.base_tax_value > 0 THEN 7
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) IN ('70','71','72','73','74','75','76','77','78','79','80','81','82','83','84','85','86','87','88','89','90','91','92','93','94','95','96','98') 
                    AND jp.base_tax_value <= 0 THEN 10
                WHEN SUBSTRING(jp.dor_use_code, 1, 2) = '09' THEN 13
                ELSE NULL
            END AS my_basic_stratum
        FROM joined_parcels jp
        LEFT JOIN permit_agg pmt ON jp.parcel_id = pmt.parcel_id
    ),
    active_stratum_calc AS (
        SELECT 
            *,
            SUM(assessed_value) OVER (PARTITION BY my_basic_stratum) AS strat_assessed_val,
            SUM(CASE WHEN my_basic_stratum BETWEEN 1 AND 7 THEN assessed_value ELSE 0 END) OVER () AS total_1_7_assessed_val
        FROM base_parcels
    ),
    active_stratum_assign AS (
        SELECT 
            *,
            CASE
                WHEN my_basic_stratum BETWEEN 1 AND 7 AND (strat_assessed_val / NULLIF(total_1_7_assessed_val, 0)) < 0.05 THEN 8
                ELSE my_basic_stratum
            END AS my_active_stratum
        FROM active_stratum_calc
    ),
    group_thresholds AS (
        SELECT 
            *,
            SUM(total_value) OVER (PARTITION BY my_active_stratum) AS stratum_total_value,
            SUM(total_value) OVER (PARTITION BY my_active_stratum ORDER BY total_value ASC, parcel_id ASC) AS cumulative_stratum_value
        FROM active_stratum_assign
    ),
    group_5_6_assign AS (
        SELECT 
            *,
            CASE WHEN cumulative_stratum_value <= (stratum_total_value * 0.05) THEN TRUE ELSE FALSE END AS is_group_5,
            CASE 
                WHEN cumulative_stratum_value > (stratum_total_value * 0.05) 
                AND total_value >= ((stratum_total_value - (stratum_total_value * 0.05)) * 0.15) THEN TRUE 
                ELSE FALSE 
            END AS is_group_6
        FROM group_thresholds
    ),
    group_1_4_prep AS (
        SELECT 
            *,
            RANK() OVER (PARTITION BY my_active_stratum ORDER BY total_value ASC) AS raw_stratum_rank,
            SUM(CASE WHEN is_group_5 THEN 1 ELSE 0 END) OVER (PARTITION BY my_active_stratum) AS group_5_count,
            SUM(CASE WHEN is_group_6 THEN 1 ELSE 0 END) OVER (PARTITION BY my_active_stratum) AS group_6_count,
            COUNT(parcel_id) OVER (PARTITION BY my_active_stratum) AS total_stratum_count
        FROM group_5_6_assign
    )
    SELECT 
        parcel_id,
        rts,
        range_code,
        township_code,
        section_code,
        sub_code,
        parcel_code,
        latitude,
        longitude,
        dor_use_code,
        nbrhd_code,
        nbrhd_desc,
        homestead,
        agri_class,
        total_acreage,
        year_created,
        year_improved,
        prior_parcel_id,
        market_area_code,
        public_land,
        valuation_code,
        valuation_desc,
        ag_land_value,
        appr_land_value,
        appr_bldg_value,
        appr_ext_feat_value,
        total_value,
        jv_dor,
        jv_dor_change,
        jv_dor_change_code,
        new_const_val_last_yr,
        new_delete_val_last_yr,
        on_reconcile,
        assessed_value,
        base_tax_value,
        asmnt_year,
        basic_stratum,
        active_stratum,
        group_num,
        spass_code,
        parcel_split,
        disaster_code,
        disaster_year,
        impvmt_quality,
        last_insp_date,
        
        my_basic_stratum,
        my_active_stratum,
        has_recent_final_inspect,
        recent_permit_total_value,
        
        CASE
            WHEN is_group_5 THEN 5
            WHEN is_group_6 THEN 6
            ELSE 
                CASE 
                    WHEN (raw_stratum_rank - group_5_count)::FLOAT / NULLIF(total_stratum_count - group_5_count - group_6_count, 0) <= 0.25 THEN 1
                    WHEN (raw_stratum_rank - group_5_count)::FLOAT / NULLIF(total_stratum_count - group_5_count - group_6_count, 0) <= 0.50 THEN 2
                    WHEN (raw_stratum_rank - group_5_count)::FLOAT / NULLIF(total_stratum_count - group_5_count - group_6_count, 0) <= 0.75 THEN 3
                    ELSE 4
                END
        END AS my_group_number

    FROM group_1_4_prep;
    """
    export_and_log_final(con, sql_dim_parcels, "stg_parcel", "dim_parcels", f"{PARQUET_OUT_DIR}/dim_parcels.parquet")

    sql_fact_bldg = "SELECT * FROM stg_bldg;"
    export_and_log_final(con, sql_fact_bldg, "stg_bldg", "fact_bldg", f"{PARQUET_OUT_DIR}/fact_bldg.parquet")

    sql_fact_land = "SELECT * FROM stg_land;"
    export_and_log_final(con, sql_fact_land, "stg_land", "fact_land", f"{PARQUET_OUT_DIR}/fact_land.parquet")

    sql_fact_misc = "SELECT * FROM stg_misc;"
    export_and_log_final(con, sql_fact_misc, "stg_misc", "fact_misc", f"{PARQUET_OUT_DIR}/fact_misc.parquet")

    sql_fact_permit = "SELECT * FROM stg_permit;"
    export_and_log_final(con, sql_fact_permit, "stg_permit", "fact_permit", f"{PARQUET_OUT_DIR}/fact_permit.parquet")

    sql_fact_sales = "SELECT * FROM stg_sales;"
    export_and_log_final(con, sql_fact_sales, "stg_sales", "fact_sales", f"{PARQUET_OUT_DIR}/fact_sales.parquet")

    sql_fact_site = "SELECT * FROM stg_site;"
    export_and_log_final(con, sql_fact_site, "stg_site", "fact_site", f"{PARQUET_OUT_DIR}/fact_site.parquet")

    elapsed = round(time.time() - start_time, 2)
    end_message = f"ETL Pipeline Completed in {elapsed} seconds"
    print(f"{'⎵' * len(end_message)}\n{end_message}\n{'⎴' * len(end_message)}")
    

if __name__ == "__main__":
    run_etl()