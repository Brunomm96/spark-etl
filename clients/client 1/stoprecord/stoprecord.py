from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class StopRecordDD:
    TOPIC = 'Client2_Debezium_.datadriven_Client2.dbo.dw_stop_record'
       
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("code", StringType()),
        StructField("description", StringType()),
        StructField("category_id", LongType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType()),
        StructField("registro_ega", StringType()),
        StructField("pnp_pp", StringType()),
        StructField("auto_fechamento", StringType()),
        StructField("descontar_producao", StringType()),
        StructField("previsto", StringType()),
        StructField("tempo_maximo_pp", StringType()),
        StructField("production_unit_id", LongType()),
        StructField("help_chain_id", LongType()),
        StructField("provided", StringType())
    ])

    @classmethod
    def extractStopRecordDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = StopRecordDD.extractStopRecordDD()


if __name__ == "__main__":
    pass    
        