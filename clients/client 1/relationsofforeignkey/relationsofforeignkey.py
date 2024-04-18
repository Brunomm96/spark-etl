from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class RelationsOfForeignKeyDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_relations_of_foreign_key'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("dw_table", StringType()),
        StructField("dw_table_key", StringType()),
        StructField("dw_foreign_key", StringType()),
        StructField("dw_reference_table", StringType()),
        StructField("dw_reference_key", StringType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType())
    ])

    @classmethod
    def extractRelationsOfForeignKeyDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = RelationsOfForeignKeyDD.extractRelationsOfForeignKeyDD()


if __name__ == "__main__":
    pass    
        