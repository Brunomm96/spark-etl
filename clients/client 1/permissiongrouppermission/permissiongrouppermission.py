from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class PermissionGroupPermissionDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_permission_group_permission'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("permission_group_id", LongType()),
        StructField("permission_id", LongType())
    ])

    @classmethod
    def extractPermissionGroupPermissionDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = PermissionGroupPermissionDD.extractPermissionGroupPermissionDD()


if __name__ == "__main__":
    pass    
        