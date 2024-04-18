from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class ProductionUnitDD:
    TOPIC = 'Client2_Debezium_.datadriven_Client2.dbo.dw_production_unit'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("name", StringType()),
        StructField("code", StringType()),
        StructField("abbreviation", StringType()),
        StructField("production_unit_id_parent", IntegerType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType()),
        StructField("production_unit_type_id", LongType()),
        StructField("status", IntegerType())
    ])

    @classmethod
    def extractProductionUnitDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = ProductionUnitDD.extractProductionUnitDD()


if __name__ == "__main__":
    pass    
        