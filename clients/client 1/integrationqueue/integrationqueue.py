from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class IntegrationQueueDD:
    TOPIC = 'Client2_Debezium_.datadriven_Client2.dbo.dw_integration_queue'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("type_integration", StringType()),
        StructField("data", StringType()),
        StructField("status", StringType()),
        StructField("plant", StringType()),
        StructField("work_center", StringType()),
        StructField("unique_key", StringType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType()),
        StructField("flag", StringType())
    ])

    @classmethod
    def extractIntegrationQueueDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = IntegrationQueueDD.extractIntegrationQueueDD()


if __name__ == "__main__":
    pass    
        