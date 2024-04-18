from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class ShiftDD:
    TOPIC = 'Client2_Debezium_.datadriven_Client2.dbo.dw_shift'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("code", StringType()),
        StructField("description", StringType()),
        StructField("hour_begin", StringType()),
        StructField("hour_end", StringType()),
        StructField("journey", StringType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType())
    ])

    @classmethod
    def extractShiftDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = ShiftDD.extractShiftDD()


if __name__ == "__main__":
    pass    
        