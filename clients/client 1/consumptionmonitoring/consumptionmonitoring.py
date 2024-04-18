from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class ConsumptionMonitoringDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_consumption_monitoring'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("product_code", StringType()),
        StructField("component_code", StringType()),
        StructField("tolerance_quantity", FloatType()),
        StructField("tolerance_percent_min", FloatType()),
        StructField("tolerance_percent_max", FloatType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType()),
        StructField("active", IntegerType()),
        StructField("help_chain_id", LongType()),
        StructField("totalize", IntegerType())
    ])

    @classmethod
    def extractConsumptionMonitoringDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = ConsumptionMonitoringDD.extractConsumptionMonitoringDD()


if __name__ == "__main__":
    pass    
        