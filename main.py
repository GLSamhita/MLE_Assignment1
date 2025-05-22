import os
import glob
import pandas as pd
from datetime import datetime, timedelta
import pyspark

import utils.data_processing_bronze_table
import utils.data_processing_silver_table
import utils.data_processing_gold_table


# Initialize SparkSession
spark = pyspark.sql.SparkSession.builder \
    .appName("dev_feature_store") \
    .master("local[*]") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

# Set log level to ERROR to hide warnings
spark.sparkContext.setLogLevel("ERROR")


# set up config
start_date_str = "2023-01-01" 
end_date_str = "2024-12-01" 

# generate list of dates to process
def generate_first_of_month_dates(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    first_of_month_dates = []
    current_date = datetime(start_date.year, start_date.month, 1)
    while current_date <= end_date:
        first_of_month_dates.append(current_date.strftime("%Y-%m-%d"))
        if current_date.month == 12:
            current_date = datetime(current_date.year + 1, 1, 1)
        else:
            current_date = datetime(current_date.year, current_date.month + 1, 1)
    return first_of_month_dates

dates_str_lst = generate_first_of_month_dates(start_date_str, end_date_str)
print("Processing for dates:", dates_str_lst)

#BRONZE
# Create bronze datalake for Features
bronze_features_directory = "datamart/bronze/features/"

# Define feature names and their CSV paths
features_config = {
    "attributes": "data/features_attributes.csv",
    "financials": "data/features_financials.csv",
    "clickstream": "data/feature_clickstream.csv"
}
# Process each feature for each date
for date_str in dates_str_lst:
    print(f"\nProcessing Bronze Tables for {date_str}")
    for feature_name, raw_csv_path in features_config.items():
        print(f"\n {feature_name}")
        utils.data_processing_bronze_table.process_feature_to_bronze(date_str, bronze_features_directory, feature_name, raw_csv_path, spark)

#SILVER
# Create silver datalake for Features
silver_features_directory = "datamart/silver/features/"
# Subdirectories for attributes, financials, clickstream will be created by functions if not exist

# Run silver backfill for Features
for date_str in dates_str_lst:
    print(f"\nProcessing Features Silver for {date_str}")
    print("\n ATTRIBUTES")
    utils.data_processing_silver_table.process_silver_attributes(date_str, bronze_features_directory, silver_features_directory, spark)
    print("\n FEATURES")
    utils.data_processing_silver_table.process_silver_financials(date_str, bronze_features_directory, silver_features_directory, spark)
    print("\n CLICKSTREAM")
    utils.data_processing_silver_table.process_silver_clickstream(date_str, bronze_features_directory, silver_features_directory, spark)

#GOLD
# Create gold datalake for Feature Store
gold_feature_store_directory = "datamart/gold/feature_store/"
if not os.path.exists(gold_feature_store_directory):
    os.makedirs(gold_feature_store_directory)

# Run gold backfill for Feature Store
for date_str in dates_str_lst:
    print(f"\nProcessing Gold Feature Store for {date_str}")
    utils.data_processing_gold_table.process_feature_store_gold_table(date_str, silver_features_directory, gold_feature_store_directory, spark)

feature_files_list = [os.path.join(gold_feature_store_directory, os.path.basename(f)) for f in glob.glob(os.path.join(gold_feature_store_directory, '*.parquet'))]
if feature_files_list:
    df_features_gold = spark.read.option("header", "true").parquet(*feature_files_list)
    print("Feature Store Gold - row_count:", df_features_gold.count())
    df_features_gold.printSchema()
else:
    print("No Parquet files found in Gold Feature Store directory.")

spark.stop()