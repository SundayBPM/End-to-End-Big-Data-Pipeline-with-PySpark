import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id, col, to_timestamp, coalesce, sum as s_sum


logger = logging.getLogger("etl.transform")

def create_id_col(df: DataFrame)->DataFrame:
    logger.info(f"Create {df.count()} id from local CSV")
    df = df.withColumn("id", monotonically_increasing_id())
    df = df[["id"] + df.columns[:-1]]
    logger.info(f"Success Create {df.count()} id from local CSV ✅")

    return df

def check_missing_values(df):
    logger.info(f"Start checking missing values")
    missing_counts = df.select([
        s_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns
    ]).collect()[0].asDict()
    logger.info(f"Success check missing values ✅")
    return missing_counts

def check_duplicate(df:DataFrame, columns:list):
    logger.info(f"Start checking duplicates values from {columns}")
    duplicate_df = df.groupBy(columns).count().filter("count > 1")
    # duplicate_df.show()
    
    dup_count = duplicate_df.count()

    logger.info(f"Success checking duplicate with {dup_count} rows found duplicated ✅")
    return dup_count, duplicate_df

def check_domain_values(df:DataFrame, column:str, allowed_values:list):
    logger.info(f"Start checking domain values from {column}")
    invalid_df = df.filter(~col(column).isin(allowed_values))
    invalid_count = invalid_df.count()

    invalid_values = [row[column] for row in invalid_df.select(column).distinct().collect()]

    logger.info(f"Success checking domain with {invalid_count} rows found invalid ✅")
    return invalid_count, invalid_values, invalid_df

def check_range(df:DataFrame):
    logger.info(f"Start checking range values")
    invalid_df = df.filter(f"price < 0")
    invalid_count = invalid_df.count()
    logger.info(f"Success checking range value with {invalid_count} rows found invalid ✅")
    return invalid_count, invalid_df

def parsed_time(df:DataFrame)->DataFrame:
    logger.info(f"Start checking range values")
    df = df.withColumn("parsed_time", to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss"))
    logger.info(f"Success parsed time ✅")
    return df

def missing_category_code(df:DataFrame)->DataFrame:
    df_mapping = df.filter(col("category_code").isNotNull())\
                    .select("product_id", col("category_code").alias("category_code_map"))\
                    .dropDuplicates(["product_id"])

    df_filled  = df.join(df_mapping, on="product_id", how="left")\
            .withColumn("category_code",
                        coalesce(df['category_code'], df_mapping['category_code_map']))\
            .drop("category_code_map")

    # df_filled .show(truncate=False)
    return df_filled

def missing_brand(df:DataFrame)->DataFrame:
    df_mapping = df.filter(col("brand").isNotNull())\
                    .select("product_id", col("brand").alias("brand_map"))\
                    .dropDuplicates(["product_id"])

    df_filled  = df.join(df_mapping, on="product_id", how="left")\
            .withColumn("brand",
                        coalesce(df['brand'], df_mapping['brand_map']))\
            .drop("brand_map")

    # df_filled .show(truncate=False)
    return df_filled