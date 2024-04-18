from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json, to_date, when, hour, minute, second, date_format
from pyspark.sql.types import *
from utils.utils import Utils 
import os
from dotenv import load_dotenv

class RefugoStreamProcessor:
    def __init__(self, kafka_server, topic, jdbc_url, connection_properties):
        self.spark = SparkSession.builder \
        .appName("Kafka Stream Example") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.jars", "/opt/spark/jars/mssql.jar") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "400") \
        .getOrCreate()
        self.kafka_server = kafka_server
        self.topic = topic
        self.jdbc_url = jdbc_url
        self.connection_properties = connection_properties

    def create_streaming_dataframe(self):
        schema_refugos = StructType() \
            .add("data_apontamento", TimestampType()) \
            .add("refugo_quantidade", StringType()) \
            .add("nr_ordem_producao", StringType()) \
            .add("codigo_refugo", StringType()) \
            .add("codigo_unidade_producao", StringType()) \
            .add("codigo_operador", StringType()) \
            .add("id_op_apontamento", IntegerType())

        df_stream_refugos = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 1000000) \
            .load() \
            .selectExpr("CAST(value AS STRING) as value") \
            .select(from_json(col("value"), schema_refugos).alias("data")).select("data.*")

        return df_stream_refugos

    def etl_process(self, df):
        df.createOrReplaceTempView("df_view")

        utils = Utils('Client2')

        df = df.withColumn('fk_data_refugo', to_date(df['data_apontamento']))
        df = df.withColumn(
            'hora_refugo',
            when(col('data_apontamento').isNull(), None)
            .otherwise(date_format(col('data_apontamento'), 'HH:mm:ss'))
        )

        df_hora_inicio = df.select('hora_refugo', 'nr_ordem_producao')
        df_data = df.select('fk_data_refugo', 'nr_ordem_producao')
        quantidade = df.select('refugo_quantidade', 'nr_ordem_producao')
        
        sk_colaborador_producao = utils.get_sk_dimension(df, 'Dim_Colaborador', 'codigo_operador', 'codigo', 'sk_colaborador', 'nr_ordem_producao')
        sk_unidade_producao = utils.get_sk_dimension(df, 'Dim_Unidade_Producao', 'codigo_unidade_producao', 'codigo', 'sk_unidade_producao', 'nr_ordem_producao')
        sk_refugo = utils.get_sk_dimension(df, 'Dim_Refugo', 'codigo_refugo', 'codigo', 'sk_refugo', 'nr_ordem_producao')
        sk_data_apontamento = utils.get_sk_dimension(df, 'Dim_Calendario', 'fk_data_refugo', 'DATA', 'sk_calendario', 'nr_ordem_producao')
        sk_data_apontamento = sk_data_apontamento.select(col('sk_calendario').alias('fk_data_refugo'), col('nr_ordem_producao'))

        join_unidade_producao = sk_unidade_producao.join(sk_refugo, 'nr_ordem_producao')
        join_data_apontamento = sk_data_apontamento.join(join_unidade_producao, 'nr_ordem_producao')
        join_hora_inicio = df_hora_inicio.join(join_data_apontamento, 'nr_ordem_producao')
        join_colaborador = sk_colaborador_producao.join(join_hora_inicio, 'nr_ordem_producao')
        join = quantidade.join(join_colaborador, 'nr_ordem_producao')


        join.show()
    
        df_subset = join.select(
                        "sk_colaborador",
                        "sk_unidade_producao",
                        "sk_refugo",
                        "fk_data_refugo",
                        "hora_refugo",                     
                        "nr_ordem_producao",
                        "refugo_quantidade"
        ).withColumnRenamed("refugo_quantidade", "quantidade_refugada") \
        .withColumnRenamed("nr_ordem_producao", "nr_producao")\
        .withColumnRenamed("sk_colaborador", "fk_colaborador_producao") \
        .withColumnRenamed("sk_unidade_producao", "fk_unidade_producao") \
        .withColumnRenamed("sk_refugo", "fk_refugo") \

        df_main = df_subset.dropDuplicates()

        df_main.show()

        print('PASSOU')

        df_main.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "Fato_Producao") \
        .option("user", connection_properties["user"]) \
        .option("password", connection_properties["password"]) \
        .option("driver", connection_properties["driver"]) \
        .mode("append") \
        .save() 

        return df_main

    def start_streaming(self):
        query = self.create_streaming_dataframe().writeStream \
            .outputMode("append") \
            .format("console") \
            .foreachBatch(self.etl_process) \
            .start()

        query.awaitTermination()

if __name__ == "__main__":
    topic = "Client2_Refugos_Teste"
    kafka_server = os.getenv("KAFKA_SERVER")
    jdbcHostname = os.getenv("Client2_DB_HOST")
    jdbcDatabase = os.getenv("Client2_DB_NAME")
    jdbcPort = os.getenv("Client2_DB_PORT")
    jdbcUsername = os.getenv("Client2_DB_USER")
    jdbcPassword = os.getenv("Client2_DB_PASS")
    
    jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=false"
    
    connectionProperties = {
        "user": os.getenv("Client2_DB_USER"),    
        "password": os.getenv("Client2_DB_PASS"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }


    processor = RefugoStreamProcessor(kafka_server, topic, jdbc_url, connection_properties)
    processor.start_streaming()