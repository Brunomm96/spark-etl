from pyspark.sql.types import *
from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class ParadasDD:
    TOPIC = 'Client2_Paradas'
        
    SCHEMA = StructType() \
        .add("ordem_producao", StringType()) \
        .add("unidade_producao", StringType()) \
        .add("parada_codigo", StringType()) \
        .add("data_inicio", TimestampType()) \
        .add("data_fim", TimestampType())
    
    @classmethod
    def extractParadasDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)        
        df = df.select("*")  
        return df

df_stg = ParadasDD.extractParadasDD()


if __name__ == "__main__":
    pass    
        