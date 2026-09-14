"""
Performs K-Means clustering on the parcel dataset and saves the results to a
Parquet file.

The following features are used for clustering:
- total_value (numeric)
- total_under_roof (numeric)
- market_area_code (categorical)
- impvmt_quality (categorical)
- dor_use_code (categorical, first 2 characters)

The optimal number of clusters is determined using the Elbow Method -
in this case, a geometric approach to find the point of maximum curvature
in the inertia plot that doesn't require visual inspection.

The final output is a Parquet file containing the parcel_id
and the assigned cluster label.
"""

import duckdb
import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

print("⁘⁚⁝⁞ K-Means Clustering Pipeline ⁞⁝⁚⁘")

# Load and join data from existing parcel dimension and building fact tables
query = """
    SELECT 
        p.parcel_id,
        p.total_value,
        p.market_area_code,
        p.impvmt_quality,
        SUBSTRING(p.dor_use_code, 1, 2) AS dor_uc_2,
        COALESCE(SUM(b.total_under_roof), 0) AS total_under_roof,
    FROM read_parquet('./data/parquet/dim_parcels.parquet') p
    LEFT JOIN read_parquet('./data/parquet/fact_bldg.parquet') b 
        ON p.parcel_id = b.parcel_id
    GROUP BY 
        p.parcel_id, p.total_value, p.market_area_code, 
        p.impvmt_quality, SUBSTRING(p.dor_use_code, 1, 2)
"""
df = duckdb.query(query).df()

# Clean and impute missing values
df['total_value'] = df['total_value'].fillna(0)
df['total_under_roof'] = df['total_under_roof'].fillna(0)
df['impvmt_quality'] = df['impvmt_quality'].fillna('Vacant/None')
df['market_area_code'] = df['market_area_code'].fillna('Unknown')

# One-hot encode categoricals
categorical_features = ['market_area_code', 'impvmt_quality', 'dor_uc_2']
numerical_features = ['total_value', 'total_under_roof']

# Convert categories to strings for correct dummy generation
X_cat = pd.get_dummies(df[categorical_features].astype(str), drop_first=True)
X_num = df[numerical_features]
X = pd.concat([X_num, X_cat], axis=1)
print(f"Feature matrix shape: {X.shape}")

# Standardization
print("▷ Scaling features")
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Automated Elbow Method (Geometric Approach)
print("▷ Determining optimal cluster count (Automated Elbow Method)")
K_range = range(2, 11)
wcss = []

# Calculate inertia for each k
for k in K_range:
    kmeans_temp = KMeans(n_clusters=k, random_state=42, n_init='auto')
    kmeans_temp.fit(X_scaled)
    wcss.append(kmeans_temp.inertia_)

# Define the line connecting the first and last points of the inertia curve
x1, y1 = K_range[0], wcss[0]
x2, y2 = K_range[-1], wcss[-1]

distances = []
# Calculate perpendicular distance from each point to the line using 2D geometry formula
for i in range(len(K_range)):
    x0, y0 = K_range[i], wcss[i]
    
    # 2D distance from point (x0, y0) to line passing through (x1, y1) and (x2, y2)
    numerator = abs((x2 - x1) * (y1 - y0) - (x1 - x0) * (y2 - y1))
    denominator = np.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    
    distances.append(numerator / denominator)

# The point with the maximum distance to the line is the elbow
optimal_k = K_range[np.argmax(distances)]
print(f"Optimal number of clusters detected: {optimal_k}")

# Execute final K-Means clustering
print(f"▷ Fitting K-Means model with {optimal_k} clusters...")
kmeans_final = KMeans(n_clusters=optimal_k, random_state=42, n_init='auto')
df['cluster_id'] = kmeans_final.fit_predict(X_scaled)
df['cluster_label'] = 'Cluster ' + (df['cluster_id'] + 1).astype(str)

# Export final table
output_path = './data/parquet/dim_parcel_clusters.parquet'
dim_parcel_clusters = df[['parcel_id', 'cluster_label']]

dim_parcel_clusters.to_parquet(output_path, index=False)

print(f"⁘⁚⁝⁞ Script Complete: Cluster table saved to {output_path} ⁞⁝⁚⁘")