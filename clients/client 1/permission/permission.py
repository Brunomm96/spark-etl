from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class PermissionDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_permission'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("static_id", StringType()),
        StructField("description", StringType()),
        StructField("description_menu", StringType()),
        StructField("link_page", StringType()),
        StructField("access_key_technical_sheet", StringType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType()),
        StructField("can_insert", IntegerType()),
        StructField("can_edit", IntegerType()),
        StructField("can_view", IntegerType()),
        StructField("can_delete", IntegerType())
    ])

    @classmethod
    def extractPermissionDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = PermissionDD.extractPermissionDD()


if __name__ == "__main__":
    pass    
        