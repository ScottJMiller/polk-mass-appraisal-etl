# Data Dictionary & Schema Specifications

## 1. `fact_sales_ratio.parquet`
* **Grain:** One row per qualified market sale transaction ($1,512,349$ rows).
* **Business Purpose:** Evaluates assessment equity and market valuation accuracy.

| Field Name | Data Type | Description |
| :--- | :--- | :--- |
| `clean_parcel_id` | `VARCHAR` | Standardized 18-character joining key. |
| `property_city` | `VARCHAR` | Situs municipality. |
| `property_zip` | `VARCHAR` | Postal zip code. |
| `dor_use_code` | `VARCHAR` | State DOR land use classification. |
| `neighborhood_code` | `VARCHAR` | Micro-market appraisal modeling zone. |
| `sale_date` | `DATE` | Recorded deed transfer date. |
| `sale_year` | `INTEGER` | Calendar year of transfer. |
| `sale_price` | `DOUBLE` | Total consideration price recorded on deed ($\ge \$10,000$). |
| `just_value` | `DOUBLE` | Property Appraiser market valuation ($JV$). |
| `living_area_sqft` | `DOUBLE` | Total air-conditioned square footage. |
| `sales_ratio` | `DOUBLE` | IAAO Sales Ratio ($JV / \text{Sale Price}$). |
| `trans_code` | `VARCHAR` | Transaction qualification code. |
| `instrument_type` | `VARCHAR` | Instrument type (e.g., Warranty Deed). |
| `latitude` / `longitude` | `DOUBLE` | Spatial centroid coordinates. |

---

## 2. `fact_permit_audit.parquet`
* **Grain:** One row per high-value municipal permit issued since 2024 ($25,662$ rows).
* **Business Purpose:** Identifies unappraised capital improvements for assessment roll maintenance.

| Field Name | Data Type | Description |
| :--- | :--- | :--- |
| `clean_parcel_id` | `VARCHAR` | Standardized 18-character joining key. |
| `property_address` | `VARCHAR` | Situs street address. |
| `property_city` | `VARCHAR` | Municipal permit jurisdiction. |
| `permit_number` | `VARCHAR` | Municipal building permit identifier. |
| `issue_date` | `DATE` | Permit issuance date. |
| `permit_val` | `DOUBLE` | Declared construction job valuation ($\ge \$25,000$). |
| `permit_type` | `VARCHAR` | Classification (Addition, Pool, Commercial Buildout). |
| `permit_status_desc` | `VARCHAR` | Inspection status (Complete, Closed, Active). |
| `living_area_sqft` | `DOUBLE` | Recorded living area prior to reassessment. |
| `just_value` | `DOUBLE` | Current roll market value. |
| `latitude` / `longitude` | `DOUBLE` | Spatial centroid coordinates. |

---

## 3. `dim_parcels.parquet`
* **Grain:** One row per real property parcel ($435,650$ rows).
* **Business Purpose:** Master dimension table linking spatial, structural, and valuation records.

| Field Name | Data Type | Description |
| :--- | :--- | :--- |
| `clean_parcel_id` | `VARCHAR` | Primary key ($1 \rightarrow \infty$ relationship to fact tables). |
| `raw_parcel_id` | `VARCHAR` | Unformatted county parcel identifier. |
| `property_address` | `VARCHAR` | Primary situs address line. |
| `property_city` / `zip` | `VARCHAR` | Municipality and zip code. |
| `dor_use_code` / `pa_use_code` | `VARCHAR` | State and local land use codes. |
| `neighborhood_code` | `VARCHAR` | Valuation modeling district. |
| `sub_code` / `sub_name` | `VARCHAR` | 6-digit subdivision ID and plat name. |
| `actual_year_built` | `INTEGER` | Physical year of construction. |
| `effective_year_built` | `INTEGER` | Effective age year after renovations. |
| `living_area_sqft` | `DOUBLE` | Building square footage. |
| `building_count` | `INTEGER` | Count of primary structures. |
| `just_value` | `DOUBLE` | Unadjusted total market value ($JV$). |
| `assessed_val_school` | `DOUBLE` | Assessed value subject to school millage. |
| `taxable_val_school` | `DOUBLE` | Net taxable value after exemptions. |
| `land_value` | `DOUBLE` | Allocated land value component. |
| `bld_style_desc` | `VARCHAR` | Architectural building classification (Primary structure). |
| `exterior_wall_desc` | `VARCHAR` | Primary exterior wall material. |
| `latitude` / `longitude` | `DOUBLE` | Spatial centroid coordinates. |