from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class AttributeDD:
    TOPIC = 'Client2_Debezium_.datadriven_Client2.dbo.dw_attribute'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("attribute", StringType()),
        StructField("title", StringType()),
        StructField("description", StringType()),
        StructField("active", IntegerType()),
        StructField("type", StringType()),
        StructField("size", IntegerType()),
        StructField("precision", IntegerType()),
        StructField("list_options", StringType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType()),
        StructField("editable", IntegerType()),
        StructField("attribute_group_id", LongType()),
        StructField("required", IntegerType()),
        StructField("microtime_id", IntegerType())
    ])

    @classmethod
    def extractAttributeDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = AttributeDD.extractAttributeDD()


if __name__ == "__main__":
    pass    
        