from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class SystranschemasDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.systranschemas'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("tabid", IntegerType()),
        StructField("startlsn", StringType()),
        StructField("endlsn", StringType()),
        StructField("typeid", IntegerType())
    ])

    @classmethod
    def extractSystranschemasDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = SystranschemasDD.extractSystranschemasDD()


if __name__ == "__main__":
    pass    
        