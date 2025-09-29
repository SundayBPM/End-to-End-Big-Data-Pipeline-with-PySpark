import logging
import os
from pyspark.sql import SparkSession

logger = logging.getLogger("etl.extract")

def extract_from_s3(spark: SparkSession, bucket: str, path: str):
    s3_path = f"s3a://{bucket}/{path}"

    df = spark.read.format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load(s3_path)

    return df

def extract_local(spark: SparkSession):

    local_path = r"D:\Repo\End-to-End-Big-Data-Pipeline-with-PySpark\data\2019-Oct.csv"

    logger.info(f"Extracting data from local file: {local_path} ...")
    df = spark.read.format("csv").load(local_path,
                                    header=True,
                                    inferSchema=True)
    
    logger.info(f"Extracted {df.count()} rows from local CSV")

    return df

def extract_example(spark: SparkSession):

    df = extract_local(spark).sample(fraction=0.001, seed=42)

    return df

