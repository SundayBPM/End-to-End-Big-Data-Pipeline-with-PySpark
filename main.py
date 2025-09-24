import logging
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from etl.extract import extract_local
from etl.transform import create_id_col
from etl.load import load_to_mysql

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log", mode='a', encoding='utf-8')
    ]
)

load_dotenv()

logger = logging.getLogger("main")


def get_spark_session():
    spark = SparkSession.builder\
                        .master("local[4]")\
                        .appName("spark-porto")\
                        .config("spark.jars",r"D:\projek\E-commers behavior PySpark\mysql-connector-j-8.1.0.jar")\
                        .config("spark.ui.port", "4050")\
                        .getOrCreate()
    return spark

def main():
    # Create session
    spark = get_spark_session()
    logger.info("Pipeline started 🚀")


    # Extract data
    df = extract_local(spark=spark)
    
    # Transform data
    df = create_id_col(df)
    df.show()

    dim_product = df.select(["product_id","category_id","category_code","brand"])\
                    .distinct()
    dim_product = dim_product.dropDuplicates(["product_id","category_id","category_code"])

    dim_user = df.select("user_id").distinct()

    dim_session = df.select(["user_session", "user_id"])

    fact_event = df.select("id","event_time","event_type","product_id","price","user_id","user_session")


    
    # Load to Data Warehouse
    load_to_mysql(dim_user, "users", 4)
    
    logging.info("ETL Pipeline finished successfully ✅")


if __name__ == "__main__":
    main()