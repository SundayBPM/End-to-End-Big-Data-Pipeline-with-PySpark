import os
import logging
from pyspark.sql import DataFrame

logger = logging.getLogger("etl.load")

def load_to_mysql(df:DataFrame, table_name:str, repartition:int):
    DB_URL = os.getenv("DB_URL")
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    df.repartition(repartition).write.format('jdbc').options(
        url=f"jdbc:mysql://{DB_URL}/{DB_NAME}",
        driver = "com.mysql.cj.jdbc.Driver",
        dbtable = table_name,
        user = DB_USER,
        batchsize = "1500",
        password = DB_PASS
    ).mode("overwrite").save()

    logger.info(f"Successfully loaded data into {table_name}")
