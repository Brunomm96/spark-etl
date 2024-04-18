from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class AttributeValuesDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_attribute_values'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("technical_sheet_id", LongType()),
        StructField("attribute_id", LongType()),
        StructField("value_string", StringType()),
        StructField("value_number", FloatType()),
        StructField("value_boolean", IntegerType()),
        StructField("value_date", StringType()),
        StructField("value_list", StringType()),
        StructField("record_access_key", StringType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType()),
        StructField("flag_integration", StringType())
    ])

    @classmethod
    def extractAttributeValuesDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = AttributeValuesDD.extractAttributeValuesDD()


if __name__ == "__main__":
    pass    
        