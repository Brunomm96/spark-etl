from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, from_json, to_date, when, hour, minute, second, format_string, from_unixtime, unix_timestamp, expr
from pyspark.sql.types import StructType, StringType, TimestampType
from utils.utils import Utils 
import os
from dotenv import load_dotenv

load_dotenv()


class KafkaStreamProcessor:

    def __init__(self, kafka_server, topic, jdbcUrl, connectionProperties):
        self.spark = SparkSession.builder \
        .appName("Kafka Stream Example") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .config("spark.jars", "/opt/spark/jars/mssql.jar") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "800") \
        .getOrCreate()
        self.kafka_server = kafka_server
        self.topic = topic
        self.jdbc_url = jdbcUrl
        self.connection_properties = connectionProperties
        self.spark.sparkContext.setLogLevel("ERROR")


    def create_streaming_dataframe(self):
        schema_paradas = StructType() \
            .add("ordem_producao", StringType()) \
            .add("unidade_producao", StringType()) \
            .add("parada_codigo", StringType()) \
            .add("data_inicio", TimestampType()) \
            .add("data_fim", TimestampType())

        df_stream_paradas = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_server) \
            .option("subscribe", self.topic) \
            .option("startingOffsets", "latest") \
            .option("maxOffsetsPerTrigger", 1000000) \
            .load() \
            .selectExpr("CAST(value AS STRING) as value") \
            .select(from_json(col("value"), schema_paradas).alias("data")).select("data.*")

        return df_stream_paradas
    
    def etl_process(self, df, epoch_id):
        utils = Utils('Client2')
        df.show()

        df = df.withColumn('data_inicio_data', to_date(df['data_inicio']))
        df = df.withColumn(
            'hora_inicio_parada',
            when(col('data_inicio').isNull(), None)
            .otherwise(format_string("%02d:%02d:%02d", hour(col('data_inicio')), minute(col('data_inicio')), second(col('data_inicio'))))
        )

        df = df.withColumn('data_fim_data', to_date(df['data_fim']))
        df = df.withColumn(
            'hora_fim_parada',
            when(col('data_fim').isNull(), None)
            .otherwise(format_string("%02d:%02d:%02d", hour(col('data_fim')), minute(col('data_fim')), second(col('data_fim'))))
        )

        df_hora_inicio = df.select('hora_inicio_parada', 'ordem_producao', 'data_inicio_data')
        df_hora_fim = df.select('hora_fim_parada', 'ordem_producao', 'data_fim_data')
        print('--------------TESTE DF--------------')
        df.show()
        print('--------------TESTE DF--------------')

        sk_unidade_producao = utils.get_sk_dimension(df, 'Dim_Unidade_Producao', 'unidade_producao', 'codigo', 'sk_unidade_producao', 'ordem_producao')
     
        sk_unidade_producao.show()
        sk_data_inicio = utils.get_sk_dimension(df, 'Dim_Calendario', 'data_inicio_data', 'DATA', 'sk_calendario', 'ordem_producao')
        sk_data_inicio = sk_data_inicio.select(col('sk_calendario').alias('sk_calendario_inicio'), col('ordem_producao'), )
        sk_data_fim = utils.get_sk_dimension(df, 'Dim_Calendario', 'data_fim_data', 'DATA', 'sk_calendario', 'ordem_producao')
        sk_data_fim = sk_data_fim.select(col('sk_calendario').alias('sk_calendario_fim'), col('ordem_producao'))

        sk_parada = utils.get_sk_dimension(df, 'Dim_Parada', 'parada_codigo', 'codigo', 'sk_parada', 'ordem_producao', 'codigo', 'oee_sn')

        join_parada = sk_parada.select('sk_parada', 'ordem_producao', 'codigo', 'oee_sn')
        join_unidade_producao = sk_unidade_producao.join(join_parada, 'ordem_producao')
        join_data_fim = sk_data_fim.join(join_unidade_producao, 'ordem_producao')
        join_data_inicio = sk_data_inicio.join(join_data_fim, 'ordem_producao')
        join_hora_inicio = df_hora_inicio.join(join_data_inicio, 'ordem_producao')
        join_hora_fim = df_hora_fim.join(join_hora_inicio, 'ordem_producao')

        """"
        A condição abaixo atribui a coluna 'tempo_sem_programacao' a diferença de tempo que ficou parado caso o codigo seja '130' que é 'SEM PROGRAMAÇÃO DE PRODUÇÃO'
        Outra verificação que faz é se for no mesmo dia ((col('sk_calendario_inicio') == col('sk_calendario_fim'))) ele faz somente as diferença somente entre as horas
        caso os dias forem diferentes ele pega a diferença de datas também.
        """
        
        df_tempo_sem_programacao = join_hora_fim.withColumn(
            'tempo_sem_programacao',
            F.when(
                (col('codigo') == '1102') & (col('sk_calendario_inicio') == col('sk_calendario_fim')),
                (unix_timestamp("hora_fim_parada", "HH:mm:ss") - unix_timestamp("hora_inicio_parada", "HH:mm:ss")) / 60
            ).otherwise(
                F.when(
                    (col('codigo') == '1102') & (col('sk_calendario_inicio') != col('sk_calendario_fim')),
                    (unix_timestamp("data_fim_data", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("data_inicio_data", "yyyy-MM-dd HH:mm:ss")) / 60
                )
            )
        )            

        df_join = df_tempo_sem_programacao.withColumn(
            'tempo_perdido',
            F.when(
                 (col('oee_sn') == 'N') & (col('sk_calendario_inicio') == col('sk_calendario_fim')),
                (unix_timestamp("hora_fim_parada", "HH:mm:ss") - unix_timestamp("hora_inicio_parada", "HH:mm:ss")) / 60
            ).otherwise(
                F.when(
                  (col('sk_calendario_inicio') != col('sk_calendario_fim')),
                    (unix_timestamp("data_fim_data", "yyyy-MM-dd HH:mm:ss") - unix_timestamp("data_inicio_data", "yyyy-MM-dd HH:mm:ss")) / 60
                )
            )
        )
               
        df_main = df_join.select(
            "sk_unidade_producao",
            "sk_calendario_inicio",
            "sk_calendario_fim",
            "sk_parada",
            "ordem_producao",
            "hora_inicio_parada",
            "hora_fim_parada", 
            "tempo_sem_programacao", 
            "tempo_perdido"
        ).withColumnRenamed("sk_calendario_inicio", "fk_data_parada_inicio") \
            .withColumnRenamed("sk_calendario_fim", "fk_data_parada_fim") \
            .withColumnRenamed("sk_unidade_producao", "fk_unidade_producao") \
            .withColumnRenamed("sk_parada", "fk_parada_producao") \
            .withColumnRenamed("ordem_producao", "nr_producao")

        df_main = df_main.dropDuplicates()

        df_main.show()
        df_main.write \
        .format("jdbc") \
        .option("url", jdbcUrl) \
        .option("dbtable", "Fato_Producao") \
        .option("user", connectionProperties["user"]) \
        .option("password", connectionProperties["password"]) \
        .option("driver", connectionProperties["driver"]) \
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


#RETIRAR AS CREDENCIAIS
if __name__ == "__main__":
    # topic = "Client2_Paradas_Teste"
    # kafka_server = os.getenv("KAFKA_SERVER")
    # jdbcHostname = os.getenv("Client2_DB_HOST")
    # jdbcDatabase = os.getenv("Client2_DB_NAME")
    # jdbcPort = os.getenv("Client2_DB_PORT")
    # jdbcUsername = os.getenv("Client2_DB_USER")
    # jdbcPassword = os.getenv("Client2_DB_PASS")
    
    # jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=false"
    
    # connectionProperties = {
    #     "user": os.getenv("Client2_DB_USER"),    
    #     "password": os.getenv("Client2_DB_PASS"),
    #     "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    # }


    # processor = KafkaStreamProcessor(kafka_server, topic, jdbcUrl, connectionProperties)
    # processor.start_streaming()
    kafka_server = "10.130.0.164:9092"
    topic = "Client2_Parada_Teste"
    jdbcUrl = "jdbc:sqlserver://186.193.228.29:4022;database=datawarehouse_Client2_teste;encrypt=false"
    connectionProperties = {
        "user": 'datawake',
        "password": '8QPH407v',
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    processor = KafkaStreamProcessor(kafka_server, topic, jdbcUrl, connectionProperties)
    processor.start_streaming()
