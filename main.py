import logging
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from etl.extract import extract_local, extract_example, extract_from_s3
from etl.transform import create_id_col, check_duplicate, check_domain_values, check_range, parsed_time, check_missing_values, missing_category_code, missing_brand
from etl.load import load_to_mysql

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(r"logs\pipeline.log", mode='a', encoding='utf-8')
    ]
)

load_dotenv()

logger_main = logging.getLogger("main")



def get_spark_session():
    spark = SparkSession.builder\
                        .master("local[4]")\
                        .appName("spark-porto")\
                        .config("spark.jars",r"spark_jars\mysql-connector-j-8.1.0.jar")\
                        .config("spark.ui.port", "4050")\
                        .getOrCreate()
    return spark

def main():
    # Create session
    spark = get_spark_session()
    logger_main.info("Pipeline started 🚀")

    try:
        # Extract data
        # df = extract_from_s3(spark, "sunday-bucket-test", "2019-Oct.csv")
        df = extract_local(spark=spark)
        # df = extract_example(spark=spark)
        
        # Validate the data
        df = create_id_col(df)
        df.show()

        check_missing = check_missing_values(df)

        dup_count, duplicate_df = check_duplicate(df=df, columns=df.columns)

        inva_count, inva_val, inva_df = check_domain_values(df=df, column='event_type', allowed_values=["view","cart","purchase"])

        inva_count_price, inva_df_price = check_range(df=df) 

        df = parsed_time(df=df)

        # Handling missing and invalid Value
        if check_missing['category_code'] > 0 :
            logger_main.info(f"Terdapat {check_missing['category_code']} missing value pada kolom category_code")
            df = missing_category_code(df=df)
        else:
            logger_main.info("Tidak ditemukan missing value pada category_code ✅")
        
        if check_missing['brand'] > 0 :
            logger_main.info(f"Terdapat {check_missing['brand']} missing value pada kolom brand")
            df = missing_brand(df=df)
        else:
            logger_main.info("Tidak ditemukan missing value pada brand ✅")



        dim_product = df.select(["product_id",
                                 "category_id",
                                 "category_code",
                                 "brand"])\
                        .distinct()
        dim_product = dim_product.dropDuplicates(["product_id",
                                                  "category_id",
                                                  "category_code"])

        dim_user = df.select("user_id").distinct()

        dim_session = df.select(["user_session", "user_id"])

        fact_event = df.select("id","event_time","event_type","product_id","price","user_id","user_session")


        
        # Load to Data Warehouse
        load_to_mysql(df=dim_product, table_name="products", mode="overwrite",repartition= 1, batchsize='1500')
        load_to_mysql(df=dim_user, table_name="users", mode="overwrite",repartition= 4, batchsize='1000')
        load_to_mysql(df=dim_session, table_name="session", mode="overwrite",repartition= 4, batchsize='1000')
        load_to_mysql(df=fact_event, table_name="transactions", mode="overwrite",repartition= 10, batchsize='1500')
        logger_main.info("ETL Pipeline finished successfully ✅")

    except Exception as e:
        logger_main.error(f"ETL pipeline failed: {e}")
    
    finally:
        logger_main.info("Stopping Spark session 🛑")
        spark.stop()


if __name__ == "__main__":
    main()