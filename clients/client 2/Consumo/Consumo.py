from pyspark.sql.types import *
from extract.KafkaServer import KafkaServer
from datetime import datetime

class Consumo:
    TOPIC = 'Client1_Consumo'

    SCHEMA = StructType([
        StructField("etiqueta_codigo",StringType()),
        StructField("operacao_nome",StringType()),
        StructField("ordem_producao",StringType()),
        StructField("unidade_producao_id",StringType()),
        StructField("data",TimestampType()),
        StructField("quantidade",FloatType()),
        StructField("origem",StringType()),
        StructField("destino",StringType()),
        StructField("quantidade_peca",IntegerType()),
        StructField("turno_codigo_arg",StringType()),
        StructField("codigo_produto",StringType())
    ])
    
    def extractConsumo(self):
        
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=True)
        return df


    def __init__(self):
        pass