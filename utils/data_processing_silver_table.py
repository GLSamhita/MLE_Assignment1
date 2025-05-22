# utils/data_processing_silver_features.py
import os
import re
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType, DoubleType
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType, FloatType, DateType

#attributes table

def process_silver_attributes(snapshot_date_str, bronze_features_dir, silver_features_dir, spark):
    bronze_file_path = os.path.join(bronze_features_dir, "attributes", f"bronze_attributes_{snapshot_date_str.replace('-', '_')}.csv")
    silver_attributes_directory = os.path.join(silver_features_dir, "attributes")
    if not os.path.exists(silver_attributes_directory):
        os.makedirs(silver_attributes_directory)

    df = spark.read.csv(bronze_file_path, header=True, inferSchema=True)
    print(f'Loaded row count: {df.count()}')

    df = (
        df
        #Customer_ID: ensure starts with 'CUS_'
        .withColumn("Customer_ID", F.when(F.col("Customer_ID").startswith("CUS_"), F.col("Customer_ID"))
                                      .otherwise(F.concat(F.lit("CUS_"), F.col("Customer_ID"))))
        
        #Age: remove underscores, convert to int, drop values not in range 18 - 99
        .withColumn(
            "Age",
            F.regexp_replace("Age", "_", "")          
             .cast(IntegerType())                      
        )
        .filter((F.col("Age") >= 18) & (F.col("Age") < 100))  
    )

    # Data Type Casting
    df = df.withColumn("Customer_ID", F.col("Customer_ID").cast(StringType())) \
           .withColumn("Age", F.col("Age").cast(IntegerType())) \
           .withColumn("Occupation", F.col("Occupation").cast(StringType())) \
           .withColumn("snapshot_date", F.to_date(F.col("snapshot_date"), "yyyy-MM-dd"))

    # Name and SSN not relevant features - removing for security
    df = df.select("Customer_ID", "Age", "Occupation", "snapshot_date")

    # save silver table
    partition_name = f"silver_attributes_{snapshot_date_str.replace('-', '_')}.parquet"
    filepath = os.path.join(silver_attributes_directory, partition_name)
    df.write.mode("overwrite").parquet(filepath)
    print(f'Saved row count: {df.count()}')
    return df

#financials table

def process_silver_financials(snapshot_date_str, bronze_features_dir, silver_features_dir, spark):
    bronze_file_path = os.path.join(bronze_features_dir, "financials", f"bronze_financials_{snapshot_date_str.replace('-', '_')}.csv")
    silver_financials_directory = os.path.join(silver_features_dir, "financials")
    if not os.path.exists(silver_financials_directory):
        os.makedirs(silver_financials_directory)

    df = spark.read.csv(bronze_file_path, header=True, inferSchema=True) 
    print(f'Loaded row count: {df.count()}')

    df = (
        df
        #Customer_ID: ensure starts with 'CUS_'
        .withColumn("Customer_ID", F.when(F.col("Customer_ID").startswith("CUS_"), F.col("Customer_ID"))
                                    .otherwise(F.concat(F.lit("CUS_"), F.col("Customer_ID"))))

        #Annual_Income: remove underscores
        .withColumn("Annual_Income", F.regexp_replace("Annual_Income", "_", ""))

        #Num_of_Loan: remove underscores
        .withColumn("Num_of_Loan", F.regexp_replace("Num_of_Loan", "_", "").cast(FloatType()))

        #Num_of_Delayed_Payment: remove underscore, cast to float
        .withColumn("Num_of_Delayed_Payment", F.regexp_replace("Num_of_Delayed_Payment", "_", "").cast(FloatType()))

        #Changed_Credit_Limit: remove underscores, greater than 0
        .withColumn("Changed_Credit_Limit", F.regexp_replace("Changed_Credit_Limit", "_", "").cast(FloatType()))

        #Outstanding_Debt: remove underscores
        .withColumn("Outstanding_Debt", F.regexp_replace("Outstanding_Debt", "_", ""))

        #Credit_History_Age: "X Years Y Months" -> extract X, replace NA with 0
        .withColumn("Credit_History_Age", F.when(F.col("Credit_History_Age").isNull(), 0)
                                        .otherwise(F.split(F.col("Credit_History_Age"), " ").getItem(0)))

        #Amount_invested_monthly: replace NA with 0, remove underscores
        .withColumn("Amount_invested_monthly", F.when(F.col("Amount_invested_monthly").isNull(), 0)
                                                .otherwise(F.col("Amount_invested_monthly")))
        .withColumn("Amount_invested_monthly", F.regexp_replace("Amount_invested_monthly", "_", ""))

        #Payment_Behaviour: replace '!@9#%8' with 'Unknown'
        .withColumn("Payment_Behaviour", F.when(F.col("Payment_Behaviour") == "!@9#%8", "Unknown")
                                        .otherwise(F.col("Payment_Behaviour")))

        #Monthly_Balance: remove underscores, cast to float
        .withColumn("Monthly_Balance", F.regexp_replace("Monthly_Balance", "_", "").cast(FloatType()))

        #Adding new features on customer behaviour (flags)
        .withColumn("Is_Min_Payment_Only", F.when(F.col("Payment_of_Min_Amount") == "Yes", 1).otherwise(0))
        .withColumn("Frequent_Late_Payer", F.when(F.col("Num_of_Delayed_Payment") > 6, 1).otherwise(0)) #late payments for half a year
    )

    # Apply .filter() to drop invalid customer data
    df = (
        df
        .filter(F.col("Monthly_Inhand_Salary") >= 0)
        .filter(F.col("Num_Bank_Accounts") >= 0)
        .filter(F.col("Num_Credit_Card") >= 0)
        .filter((F.col("Interest_Rate") >= 1) & (F.col("Interest_Rate") <= 100))
        .filter(F.col("Num_of_Loan") >= 0)
        .filter(F.col("Delay_from_due_date") >= 0)
        .filter(F.col("Changed_Credit_Limit") > 0)
        .filter(F.col("Num_of_Delayed_Payment") >= 0)
        .filter(F.col("Num_Credit_Inquiries") >= 0)
        .filter((F.col("Credit_Utilization_Ratio") >= 0) & (F.col("Credit_Utilization_Ratio") <= 100))
        .filter(F.col("Monthly_Balance") >= 0)   
    )
    # Data Type Casting
    df = (
        df
        .withColumn("Customer_ID", F.col("Customer_ID").cast(StringType()))
        .withColumn("Annual_Income", F.col("Annual_Income").cast(FloatType()))
        .withColumn("Monthly_Inhand_Salary", F.col("Monthly_Inhand_Salary").cast(FloatType()))
        .withColumn("Num_Bank_Accounts", F.col("Num_Bank_Accounts").cast(IntegerType()))
        .withColumn("Num_Credit_Card", F.col("Num_Credit_Card").cast(IntegerType()))
        .withColumn("Interest_Rate", F.col("Interest_Rate").cast(FloatType()))
        .withColumn("Num_of_Loan", F.col("Num_of_Loan").cast(FloatType()))
        .withColumn("Type_of_Loan", F.col("Type_of_Loan").cast(StringType()))
        .withColumn("Delay_from_due_date", F.col("Delay_from_due_date").cast(IntegerType()))
        .withColumn("Num_of_Delayed_Payment", F.col("Num_of_Delayed_Payment").cast(IntegerType()))
        .withColumn("Changed_Credit_Limit", F.col("Changed_Credit_Limit").cast(FloatType()))
        .withColumn("Num_Credit_Inquiries", F.col("Num_Credit_Inquiries").cast(FloatType()))
        .withColumn("Credit_Mix", F.col("Credit_Mix").cast(StringType()))
        .withColumn("Outstanding_Debt", F.col("Outstanding_Debt").cast(FloatType()))
        .withColumn("Credit_Utilization_Ratio", F.col("Credit_Utilization_Ratio").cast(FloatType()))
        .withColumn("Credit_History_Age", F.col("Credit_History_Age").cast(IntegerType()))
        .withColumn("Payment_of_Min_Amount", F.col("Payment_of_Min_Amount").cast(StringType()))
        .withColumn("Total_EMI_per_month", F.col("Total_EMI_per_month").cast(FloatType()))
        .withColumn("Amount_invested_monthly", F.col("Amount_invested_monthly").cast(FloatType()))
        .withColumn("Payment_Behaviour", F.col("Payment_Behaviour").cast(StringType()))
        .withColumn("Monthly_Balance", F.col("Monthly_Balance").cast(FloatType()))
        .withColumn("snapshot_date", F.to_date("snapshot_date", "yyyy-MM-dd"))
    )

    # Save silver table
    partition_name = f"silver_financials_{snapshot_date_str.replace('-', '_')}.parquet"
    filepath = os.path.join(silver_financials_directory, partition_name)
    df.write.mode("overwrite").parquet(filepath)
    print(f'Saved row count: {df.count()}')
    return df

def process_silver_clickstream(snapshot_date_str, bronze_features_dir, silver_features_dir, spark):
    bronze_file_path = os.path.join(bronze_features_dir, "clickstream", f"bronze_clickstream_{snapshot_date_str.replace('-', '_')}.csv")
    silver_clickstream_directory = os.path.join(silver_features_dir, "clickstream")
    if not os.path.exists(silver_clickstream_directory):
        os.makedirs(silver_clickstream_directory)

    df = spark.read.csv(bronze_file_path, header=True, inferSchema=True)
    print(f'Loaded row count: {df.count()}')

    df = ( 
        df
        # 0. Customer_ID: ensure starts with 'CUS_'
        .withColumn("Customer_ID", F.when(F.col("Customer_ID").startswith("CUS_"), F.col("Customer_ID"))
                                      .otherwise(F.concat(F.lit("CUS_"), F.col("Customer_ID"))))
    )

    # Cast all fe_X columns to IntegerType
    for i in range(1, 21):
        col_name = f"fe_{i}"
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))
    df = df.withColumn("Customer_ID", F.col("Customer_ID").cast(StringType()))
    df = df.withColumn("snapshot_date", F.to_date(F.col("snapshot_date"), "yyyy-MM-dd"))

    # Save silver table
    partition_name = f"silver_clickstream_{snapshot_date_str.replace('-', '_')}.parquet"
    filepath = os.path.join(silver_clickstream_directory, partition_name)
    df.write.mode("overwrite").parquet(filepath)
    print(f'Saved row count: {df.count()}')
    return df