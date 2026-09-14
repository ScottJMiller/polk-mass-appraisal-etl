# Mass Appraisal Analytics Data Dictionary

This data dictionary outlines the data architecture, schema definitions, and analytical transformations generated across the mass appraisal ETL and machine learning pipelines. All tables are saved as ZSTD-compressed Parquet files.

---

## Schema Architecture Overview

The data warehouse follows a dimensional star/snowflake schema centered around the primary geographic identifier `parcel_id`:

* **Primary Dimension:** `dim_parcels` integrates Florida Department of Revenue (DOR) Name-Address-Legal (NAL) tax roll metrics, local Property Appraiser (PA) cadastre data, and reprojected spatial centroids.
* **Machine Learning Dimension:** `dim_parcel_clusters` joins to `dim_parcels` via `parcel_id` to provide unsupervised segmentation groups.
* **Fact Tables:** Capture structural improvements (`fact_bldg`), land segments (`fact_land`), miscellaneous features (`fact_misc`), construction permits (`fact_permit`), historical transactions (`fact_sales`), and physical situs addresses (`fact_site`).
* **Lookup Tables:** Normalized code-to-description mapping tables for appraisal categories, building characteristics, and statutory audit classifications.

---

## Dimension Tables

### `dim_parcels`

* **File Path:** `./data/parquet/dim_parcels.parquet`
* **Grain:** One row per unique real estate parcel (`parcel_id`).
* **Coordinate Reference System:** WGS84 Decimal Degrees (`EPSG:4326`), reprojected from Florida West State Plane (`EPSG:2882`).

| Column Name | Data Type | Source / Logic | Description |
| --- | --- | --- | --- |
| `parcel_id` | VARCHAR | `stg_parcel` | Primary unique parcel identifier (Strap). |
| `rts` | VARCHAR | Chars 1–6 of `parcel_id` | Combined Public Land Survey System (PLSS) Range, Township, Section. |
| `range_code` | VARCHAR | Chars 1–2 of `parcel_id` | Cadastral Range identifier. |
| `township_code` | VARCHAR | Chars 3–4 of `parcel_id` | Cadastral Township identifier. |
| `section_code` | VARCHAR | Chars 5–6 of `parcel_id` | Cadastral Section identifier. |
| `sub_code` | VARCHAR | Chars 7–12 of `parcel_id` | Subdivision identification code. |
| `parcel_code` | VARCHAR | Chars 13–18 of `parcel_id` | Parcel block and lot/sequence number. |
| `latitude` | DOUBLE | Reprojected `stg_gis` | Parcel centroid latitude in WGS84 decimal degrees. |
| `longitude` | DOUBLE | Reprojected `stg_gis` | Parcel centroid longitude in WGS84 decimal degrees. |
| `dor_use_code` | VARCHAR | `stg_parcel` | Florida DOR 4-digit standardized property use classification. |
| `nbrhd_code` | DECIMAL(10,2) | `stg_parcel` | Internal appraisal neighborhood assessment code. |
| `nbrhd_desc` | VARCHAR | `stg_parcel` | Descriptive name of the appraisal neighborhood. |
| `homestead` | DECIMAL(10,2) | `stg_parcel` | Total dollar value of granted Homestead Exemption. |
| `agri_class` | VARCHAR | `stg_parcel` | Agricultural classification status code. |
| `total_acreage` | DECIMAL(12,4) | `stg_parcel` | Total parcel land area measured in acres. |
| `year_created` | INTEGER | `stg_parcel` | Calendar year the parcel record was created in the tax roll. |
| `year_improved` | INTEGER | `stg_parcel` | Year the primary structure or major improvement was completed. |
| `prior_parcel_id` | VARCHAR | `stg_parcel` | Parent parcel ID prior to subdivision, split, or combination. |
| `market_area_code` | VARCHAR | `stg_nal` | DOR mass appraisal economic market area code. |
| `public_land` | VARCHAR | `stg_nal` | Flag/code indicating governmental or public entity ownership. |
| `valuation_code` | VARCHAR | `stg_parcel` | Property appraiser internal valuation methodology code. |
| `valuation_desc` | VARCHAR | `stg_parcel` | Valuation model description (e.g., Market, Cost, Income). |
| `ag_land_value` | INTEGER | `stg_parcel` | Classified agricultural use land value. |
| `appr_land_value` | INTEGER | `stg_parcel` | Appraised market value of land. |
| `appr_bldg_value` | INTEGER | `stg_parcel` | Appraised market value of all primary structures. |
| `appr_ext_feat_value` | INTEGER | `stg_parcel` | Appraised market value of extra/miscellaneous features. |
| `total_value` | INTEGER | `stg_parcel` | Total appraised market/just value ($Land + Building + Extra$). |
| `jv_dor` | DOUBLE | `stg_nal` | Just value as submitted to Florida DOR on the NAL roll. |
| `jv_dor_change` | DOUBLE | `stg_nal` | Net dollar change in DOR just value compared to prior roll. |
| `jv_dor_change_code` | VARCHAR | `stg_nal` | Statutory explanation code for change in DOR just value. |
| `new_const_val_last_yr` | DOUBLE | `stg_nal` | Value added from new construction in prior assessment cycle. |
| `new_delete_val_last_yr` | DOUBLE | `stg_nal` | Value removed from demolitions/deletions in prior cycle. |
| `on_reconcile` | VARCHAR | `stg_parcel` | Roll reconciliation status indicator. |
| `assessed_value` | DECIMAL(10,2) | `stg_parcel` | Assessed value subject to statutory assessment limitation caps. |
| `base_tax_value` | DECIMAL(10,2) | `stg_parcel` | Base taxable value after applying all eligible exemptions. |
| `asmnt_year` | INTEGER | `stg_nal` | Assessment tax roll year. |
| `basic_stratum` | VARCHAR | `stg_nal` | DOR-assigned basic stratum for statistical sampling. |
| `active_stratum` | VARCHAR | `stg_nal` | DOR-assigned active stratum used in official ratio studies. |
| `group_num` | VARCHAR | `stg_nal` | DOR sampling group code. |
| `spass_code` | VARCHAR | `stg_nal` | Special assessment authority code. |
| `parcel_split` | VARCHAR | `stg_nal` | Flag identifying parcel split/merge activity. |
| `disaster_code` | VARCHAR | `stg_nal` | Catastrophic disaster relief classification code. |
| `disaster_year` | INTEGER | `stg_nal` | Year disaster damage occurred. |
| `impvmt_quality` | VARCHAR | `stg_nal` | Overall structural construction quality grade. |
| `last_insp_date` | TIMESTAMP | Coalesced | Date of last field inspection (derived from PA date or NAL MMYY). |
| `my_basic_stratum` | INTEGER | Derived (SQL CASE) | Custom statutory DOR basic stratum assignment (Stratum 1–13). |
| `my_active_stratum` | INTEGER | Derived (Window) | Custom active stratum; shifts stratums <5% of total value to Stratum 8. |
| `has_recent_final_inspect` | BOOLEAN | Derived (`stg_permit`) | True if parcel has a finalized permit within 6 months of audit date. |
| `recent_permit_total_value` | DOUBLE | Derived (`stg_permit`) | Sum of estimated permit values finalized in the 6-month window. |
| `my_group_number` | INTEGER | Derived (Window) | Value stratification group: 1–4 (value quartiles), 5 (bottom 5%), 6 (top outliers). |

---

### `dim_parcel_clusters`

* **File Path:** `./data/parquet/dim_parcel_clusters.parquet`
* **Grain:** One row per clustered parcel (`parcel_id`).
* **Model Source:** Scikit-Learn K-Means ($k$ determined dynamically via geometric Elbow Method on standardized features: `total_value`, `total_under_roof`, `market_area_code`, `impvmt_quality`, and two-digit DOR use code).

| Column Name | Data Type | Description |
| --- | --- | --- |
| `parcel_id` | VARCHAR | Foreign key linking to `dim_parcels[parcel_id]`. |
| `cluster_label` | VARCHAR | Categorical cluster assignment label (e.g., `Cluster 1`, `Cluster 2`). |

---

## Fact Tables

### `fact_bldg`

* **File Path:** `./data/parquet/fact_bldg.parquet`
* **Grain:** One row per building/structure per parcel (`parcel_id` + `bld_num`).

| Column Name | Data Type | Description |
| --- | --- | --- |
| `parcel_id` | VARCHAR | Foreign key linking to `dim_parcels[parcel_id]`. |
| `bld_num` | INTEGER | Sequential structure number on the parcel. |
| `bldg_style` | VARCHAR | Architectural building design code. |
| `bldg_stories` | VARCHAR | Number of stories/levels. |
| `bldg_shape` | VARCHAR | Perimeter irregularity/shape classification code. |
| `bldg_class` | VARCHAR | Structural framing class code. |
| `num_bathrooms` | DOUBLE | Total number of full and partial bathrooms. |
| `total_units` | VARCHAR | Total residential/commercial units within the structure. |
| `num_bedrooms` | DOUBLE | Total number of bedrooms. |
| `num_fireplaces` | DOUBLE | Count of fireplaces. |
| `bldg_foundation_type` | VARCHAR | Substructure foundation construction type code. |
| `bldg_frame_type` | VARCHAR | Structural frame construction material code. |
| `effective_year` | DOUBLE / INT | Effective construction year reflecting depreciation and upgrades. |
| `actual_year` | DOUBLE / INT | Actual chronological construction year built. |
| `bldg_ext_wall_type` | VARCHAR | Primary exterior wall finish material code. |
| `bldg_roof_type` | VARCHAR | Roof structural design and framing type code. |
| `bldg_floor_type` | VARCHAR | Subfloor and surface flooring material code. |
| `bldg_int_wall_type` | VARCHAR | Interior wall construction and partition material code. |
| `total_living_area` | DOUBLE | Finished, climate-controlled gross living area (Sq. Ft.). |
| `total_under_roof` | DOUBLE | Total footprint area under roof including porches and garages (Sq. Ft.). |

---

### `fact_land`

* **File Path:** `./data/parquet/fact_land.parquet`
* **Grain:** One row per land segment/line per parcel (`parcel_id` + `land_line_num`).

| Column Name | Data Type | Description |
| --- | --- | --- |
| `parcel_id` | VARCHAR | Foreign key linking to `dim_parcels[parcel_id]`. |
| `land_line_num` | INTEGER | Sequential land appraisal segment number. |
| `land_class` | VARCHAR | Land pricing classification code. |
| `land_use_code` | VARCHAR | Land-specific use category code. |
| `land_uc_desc` | VARCHAR | Description of specific land use. |
| `land_frontage` | DECIMAL(12,2) | Road or water frontage measured in linear feet. |
| `land_depth` | DECIMAL(12,2) | Average parcel segment depth measured in linear feet. |
| `land_units` | DECIMAL(20,4) | Total quantity of land units (Acreage, Square Feet, Front Feet). |
| `land_unit_type` | VARCHAR | Unit of measurement code (e.g., `AC`, `SF`, `FF`). |
| `land_unit_desc` | VARCHAR | Description of the pricing unit of measure. |

---

### `fact_misc`

* **File Path:** `./data/parquet/fact_misc.parquet`
* **Grain:** One row per extra feature or outbuilding (`parcel_id` + `xfeature_num`).

| Column Name | Data Type | Description |
| --- | --- | --- |
| `parcel_id` | VARCHAR | Foreign key linking to `dim_parcels[parcel_id]`. |
| `bld_num` | INTEGER | Associated building sequence number (if applicable). |
| `xfeature_num` | INTEGER | Sequential extra feature record number. |
| `xfeat_code` | VARCHAR | Extra feature item classification code (e.g., sheds, pools, docks). |
| `xfeat_yr_blt` | INTEGER | Year the extra feature was constructed/installed. |
| `xfeat_length` | INTEGER | Feature length in feet. |
| `xfeat_width` | INTEGER | Feature width in feet. |

---

### `fact_permit`

* **File Path:** `./data/parquet/fact_permit.parquet`
* **Grain:** One row per building permit event (`permit_id`).

| Column Name | Data Type | Description |
| --- | --- | --- |
| `parcel_id` | VARCHAR | Foreign key linking to `dim_parcels[parcel_id]`. |
| `permit_id` | INTEGER | Internal tracking primary key for the permit record. |
| `permit_agency` | VARCHAR | Issuing municipality or jurisdictional licensing authority. |
| `permit_num` | VARCHAR | Official regulatory permit tracking number. |
| `permit_status` | CHAR | Status code (e.g., `C` for Complete/Finaled). |
| `permit_desc` | VARCHAR | Narrative description of proposed construction or alteration. |
| `permit_type` | VARCHAR | Category of construction (e.g., New Construction, Alteration, Repair). |
| `issue_date` | TIMESTAMP | Date the permit was formally issued to the contractor/owner. |
| `final_insp_date` | TIMESTAMP | Date of successful certificate of occupancy or final field inspection. |
| `est_value` | INTEGER | Declared construction valuation or project cost estimate ($). |
| `site_street_num` | VARCHAR | Street number where work was permitted. |
| `site_street_pfx` | CHAR | Directional prefix for permitted address (e.g., `N`, `S`). |
| `site_street_name` | VARCHAR | Street name for permitted location. |
| `site_street_sfx` | VARCHAR | Street suffix (e.g., `RD`, `AVE`, `BLVD`). |
| `site_apt_num` | VARCHAR | Suite or apartment unit identifier. |

---

### `fact_sales`

* **File Path:** `./data/parquet/fact_sales.parquet`
* **Grain:** One row per recorded real estate conveyance transaction (`parcel_id` + `sale_line_num`).

| Column Name | Data Type | Description |
| --- | --- | --- |
| `parcel_id` | VARCHAR | Foreign key linking to `dim_parcels[parcel_id]`. |
| `sale_line_num` | INTEGER | Historical sequence number for transaction records. |
| `sale_date` | TIMESTAMP | Official date deed/instrument was executed. |
| `sale_price` | INTEGER | Recorded gross sales transaction price ($). |
| `book_num` | VARCHAR | Official county public records book number. |
| `page_num` | VARCHAR | Official county public records page number. |
| `sale_type` | CHAR | Sale qualification indicator. |
| `transact_code` | VARCHAR | Conveyance qualification and transfer code. |
| `instr_type` | VARCHAR | Legal instrument type code (e.g., `WD` for Warranty Deed). |
| `instr_type_dscr` | VARCHAR | Legal conveyance instrument description. |

---

### `fact_site`

* **File Path:** `./data/parquet/fact_site.parquet`
* **Grain:** One row per physical address point per parcel (`parcel_id` + `site_address_id`).

| Column Name | Data Type | Description |
| --- | --- | --- |
| `parcel_id` | VARCHAR | Foreign key linking to `dim_parcels[parcel_id]`. |
| `site_address_id` | INTEGER | Sequential address record ID for the parcel. |
| `bld_num` | INTEGER | Structure sequence number assigned to this situs address. |
| `street` | VARCHAR | Full formatted street name. |
| `street_prefix` | CHAR | Directional street prefix (`N`, `S`, `E`, `W`). |
| `street_number` | INTEGER | Street numeric house address. |
| `street_num_suffix` | CHAR | Letter or numeric suffix on street number (e.g., `1/2`). |
| `street_suffix` | VARCHAR | Street roadway designation (`AVE`, `ST`, `WAY`, `DR`). |
| `street_sfx_direction` | CHAR | Directional street suffix (`NW`, `SE`). |
| `street_unit` | VARCHAR | Unit, suite, apartment, or lot identifier. |
| `zip_code` | VARCHAR | 5-digit USPS Postal ZIP code. |
| `city` | VARCHAR | Situs municipal jurisdiction or postal city name. |

---

## Lookup and Reference Tables

All reference tables are stored in `./data/parquet/lookups/`. They provide human-readable definitions and analytical flags for coded fields across dimension and fact tables:

| Table Name | Source File | Key Field | Description Fields & Flags |
| --- | --- | --- | --- |
| `active_stratum_lup` | `active_stratum.txt` | `stratum` | `definition`, `incl_in_stat_analysis` (Boolean) |
| `bld_class_codes_lup` | `bld_class_codes.txt` | `code` | `description` (Structural frame classification) |
| `bld_frame_codes_lup` | `bld_frame_codes.txt` | `code` | `description` (Building frame material) |
| `bld_shape_codes_lup` | `bld_shape_codes.txt` | `code` | `description` (Perimeter design shape) |
| `bld_style_codes_lup` | `bld_style_codes.txt` | `code` | `description` (Architectural styling) |
| `disaster_codes_lup` | `disaster_codes.txt` | `code` | `definition` (Declared disaster identifiers) |
| `dor_use_codes_lup` | `dor_use_code.csv` | `dor_use_code` | `dor_uc_category`, `dor_uc_cat_desc`, `dor_uc_desc` |
| `extra_feat_codes_lup` | `extra_feat_codes.txt` | `code` | `description` (Outbuilding/feature item codes) |
| `ext_wall_codes_lup` | `ext_wall_codes.txt` | `code` | `description` (Exterior wall construction) |
| `floor_type_codes_lup` | `floor_type_codes.txt` | `code` | `description` (Flooring structure/materials) |
| `int_wall_codes_lup` | `int_wall_codes.txt` | `code` | `description` (Interior wall partitions) |
| `jv_change_codes_lup` | `jv_change_codes.txt` | `code` | `definition` (DOR Just Value change justifications) |
| `permit_stat_cd_lup` | `permit_stat_cd.txt` | `code` | `description` (Building permit completion status) |
| `prop_tran_codes_lup` | `prop_tran_codes.txt` | `code` | `definition`, `is_qualified_sale`, `incl_in_sales_ratio` |
| `pub_land_codes_lup` | `pub_land_codes.txt` | `code` | `description` (Public land authority ownership) |
| `roof_type_codes_lup` | `roof_type_codes.txt` | `code` | `description` (Roof structure geometry and covering) |
| `spass_codes_lup` | `spass_codes.txt` | `code` | `definition` (Special assessment taxing jurisdictions) |
| `substruct_codes_lup` | `substruct_codes.txt` | `code` | `description` (Foundation and substructure types) |
| `transact_codes_lup` | `transact_codes.txt` | `code` | `description`, `incl_in_sales_ratio` (Boolean) |