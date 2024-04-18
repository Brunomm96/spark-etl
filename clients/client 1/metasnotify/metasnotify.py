from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class MetasNotifyDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_metas_notify'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("meta_id", IntegerType()),
        StructField("date_notify", StringType()),
        StructField("config_goal_product_id", LongType()),
        StructField("date_insert", StringType()),
        StructField("access_key", StringType()),
        StructField("help_chain_id", LongType()),
        StructField("production_unit_id", LongType()),
        StructField("product_code", StringType()),
        StructField("product_new_piece_hour", FloatType()),
        StructField("current_product_piece_hour", FloatType())
    ])

    @classmethod
    def extractMetasNotifyDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = MetasNotifyDD.extractMetasNotifyDD()


if __name__ == "__main__":
    pass    
        