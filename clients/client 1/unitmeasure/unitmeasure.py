from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class UnitMeasureDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_unit_measure'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("code_unit_measure", StringType()),
        StructField("description", StringType()),
        StructField("code_unit_measure_to", StringType()),
        StructField("type_multiplier", StringType()),
        StructField("value_conversion", FloatType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType())
    ])

    @classmethod
    def extractUnitMeasureDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = UnitMeasureDD.extractUnitMeasureDD()


if __name__ == "__main__":
    pass    
        