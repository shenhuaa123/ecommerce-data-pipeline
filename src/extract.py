import os
import sys
from pyspark.sql import SparkSession

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(CURRENT_DIR)
sys.path.append(BASE_DIR)
from config import (
    ORDERS_FILE,
    CUSTOMERS_FILE,
    PRODUCTS_FILE,
    REGIONS_FILE
)

def create_spark_session(app_name="电子商务数据管道"):
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = SparkSession.builder \
                        .appName(app_name) \
                        .master("local[*]") \
                        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    return spark

def read_csv(spark, file_path):
    df = spark.read \
            .option("header", "true") \
            .option("encoding", "UTF-8") \
            .option("inferSchema", "true") \
            .csv(file_path)
    
    for old_name in df.columns:
        new_name = old_name.replace("\ufeff", "").strip()
        df = df.withColumnRenamed(old_name, new_name)
    
    return df

def load_raw_data(spark):
    orders_df = read_csv(spark, ORDERS_FILE)
    customers_df = read_csv(spark, CUSTOMERS_FILE)
    products_df = read_csv(spark, PRODUCTS_FILE)
    regions_df = read_csv(spark, REGIONS_FILE)

    return orders_df, customers_df, products_df, regions_df