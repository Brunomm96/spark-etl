from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, broadcast
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, TimestampType
from dotenv import load_dotenv
from pyspark.sql.functions import col
import os

load_dotenv()

spark = SparkSession.builder \
    .appName("Spark - MSSQL") \
    .config("spark.jars", "/opt/spark/jars/mssql.jar") \
    .getOrCreate()

class Utils:

    def __init__(self, source_id):  
        self.source_id = source_id

        jdbcHostname = os.getenv(f"{source_id.upper()}_DB_HOST")
        jdbcDatabase = os.getenv(f"{source_id.upper()}_DB_NAME")
        jdbcPort = os.getenv(f"{source_id.upper()}_DB_PORT")
        jdbcUsername = os.getenv(f"{source_id.upper()}_DB_USER")
        jdbcPassword = os.getenv(f"{source_id.upper()}_DB_PASS")
        
        self.jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=false"
        
        self.connectionProperties = {
            "user": os.getenv(f"{source_id.upper()}_DB_USER"),    
            "password": os.getenv(f"{source_id.upper()}_DB_PASS"),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }


    
    def get_sk_dimension(self, df_main, table_name, column_to_compare_df, column_to_compare_bd, sk_column, column_join, *other_columns):
        df_dimensoes = spark.read.jdbc(url=self.jdbcUrl, table=table_name, properties=self.connectionProperties)

        join_condition = col(column_to_compare_df) == col(column_to_compare_bd)
        df_main = df_main.join(df_dimensoes, join_condition)

        selected_columns = [col(sk_column), col(column_join)]

        if other_columns:
            selected_columns.extend(col(column) for column in other_columns)

        df_result = df_main.select(*selected_columns)

        return df_result