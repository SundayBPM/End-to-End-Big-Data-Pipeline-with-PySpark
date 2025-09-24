import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import monotonically_increasing_id

logger = logging.getLogger("etl.extract")

def create_id_col(df: DataFrame)->DataFrame:

    logger.info(f"Create {df.count()} id from local CSV")
    df = df.withColumn("id", monotonically_increasing_id())
    df = df[["id"] + df.columns[:-1]]
    logger.info(f"Success Create {df.count()} id from local CSV ✅")

    return df