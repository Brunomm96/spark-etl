from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class ActionConfigEmailDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_action_config_email'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("description", StringType()),
        StructField("send_to", StringType()),
        StructField("subject", StringType()),
        StructField("message", StringType()),
        StructField("help_chain_action_id", LongType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType())
    ])

    @classmethod
    def extractActionConfigEmailDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = ActionConfigEmailDD.extractActionConfigEmailDD()


if __name__ == "__main__":
    pass    
        