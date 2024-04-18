from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class ConfigGoalProductDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_config_goal_product'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("product_code", StringType()),
        StructField("goal_default", FloatType()),
        StructField("goal_calculated", FloatType()),
        StructField("qty_delivery_to_calc", FloatType()),
        StructField("percent_outlier", FloatType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType()),
        StructField("last_process", StringType()),
        StructField("help_chain_id", LongType())
    ])

    @classmethod
    def extractConfigGoalProductDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = ConfigGoalProductDD.extractConfigGoalProductDD()


if __name__ == "__main__":
    pass    
        