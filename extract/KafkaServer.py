from pyspark.sql import SparkSession, DataFrame
from dotenv import load_dotenv
from pyspark.sql.functions import from_json
import os

load_dotenv()

class KafkaServer:

    def __init__(self) -> None:
        self.spark_session = SparkSession.builder \
            .appName("Spark - Kafka") \
            .getOrCreate()
        
        self.SERVER_KAFKA = os.environ.get('KAFKA_SERVER')


    def get_df(self, schema, topic: str,  stream : False, time_extract = "latest") -> DataFrame:
        """
         Args:   
            Para carga streaming o paramêtro time_extract vem como default.
            Para carga completa passar o paramêtro time_extract="earliest".
        """
        offsets = {"Unipac_Pompeia_Consumo": {"0": 10370}}
        if stream:
            reader = self.spark_session.readStream \
                    .option("startingOffsets", str(offsets).replace("'", "\""))
        else:
            reader = self.spark_session.read

        kafka_load = reader \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.SERVER_KAFKA) \
            .option("subscribe", topic) \
            .load()
        
        df = kafka_load.selectExpr("CAST(value as STRING)")

        df = df.select(from_json(df.value, schema).alias("data")) \
            .select("data.*")
        
        return df
    
    def get_df_streaming(self, schema, topic: str):
        if not schema or len(schema) == 0:
            raise ValueError("O esquema (schema) não foi fornecido ou está vazio.")

        try:
            streaming_df = (
            self.spark_session.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.SERVER_KAFKA)
            .option("subscribe", topic)
            .load()
            .selectExpr("CAST(value AS STRING)") 
            .select(from_json("value", schema).alias("data"))
            .select("data.*")
            )
            streaming_df.awaitTermination()

        except Exception as e:
            print(f"Erro ao ler streaming do Kafka: {str(e)}")


        return streaming_df
