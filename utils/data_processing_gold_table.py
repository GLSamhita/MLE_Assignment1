# utils/data_processing_gold_features.py
import os
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

def process_feature_store_gold_table(snapshot_date_str, silver_features_dir, gold_feature_store_dir, spark):
    
    # Paths to silver tables for the given snapshot_date
    silver_attributes_path = os.path.join(silver_features_dir, "attributes", f"silver_attributes_{snapshot_date_str.replace('-', '_')}.parquet")
    silver_financials_path = os.path.join(silver_features_dir, "financials", f"silver_financials_{snapshot_date_str.replace('-', '_')}.parquet")
    silver_clickstream_path = os.path.join(silver_features_dir, "clickstream", f"silver_clickstream_{snapshot_date_str.replace('-', '_')}.parquet")

    df_attributes = spark.read.parquet(silver_attributes_path)
    df_financials = spark.read.parquet(silver_financials_path)
    df_clickstream = spark.read.parquet(silver_clickstream_path)

    join_condition = ["Customer_ID", "snapshot_date"]
    
    df_gold = df_clickstream.join(df_financials, join_condition, "left") \
                           .join(df_attributes, join_condition, "left")

    print(f'Gold feature store for {snapshot_date_str} - joined count: {df_gold.count()}')
    
    # save gold table
    if not os.path.exists(gold_feature_store_dir):
        os.makedirs(gold_feature_store_dir)
        
    partition_name = f"gold_feature_store_{snapshot_date_str.replace('-', '_')}.parquet"
    filepath = os.path.join(gold_feature_store_dir, partition_name)
    df_gold.write.mode("overwrite").parquet(filepath)
    
    print(f'Saved gold feature store row count: {df_gold.count()}')
    
    return df_gold