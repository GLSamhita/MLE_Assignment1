import os
from datetime import datetime
from pyspark.sql.functions import col

def process_feature_to_bronze(snapshot_date_str, bronze_base_dir, feature_name, raw_csv_path, spark):
    snapshot_date = datetime.strptime(snapshot_date_str, "%Y-%m-%d")
    
    bronze_feature_directory = os.path.join(bronze_base_dir, feature_name)
    if not os.path.exists(bronze_feature_directory):
        os.makedirs(bronze_feature_directory)

    df = spark.read.csv(raw_csv_path, header=True, inferSchema=True)

    df_filtered = df.filter(col('snapshot_date') == snapshot_date)
    
    print(f"{feature_name} - {snapshot_date_str} raw row count: {df.count()}, filtered row count: {df_filtered.count()}")

    partition_name = f"bronze_{feature_name}_{snapshot_date_str.replace('-', '_')}.csv"
    filepath = os.path.join(bronze_feature_directory, partition_name)

    df_filtered.toPandas().to_csv(filepath, index=False)
    print(f'Saved {feature_name} bronze to: {filepath}')
    return df_filtered