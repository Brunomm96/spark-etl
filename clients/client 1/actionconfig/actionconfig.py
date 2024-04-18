from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class ActionConfigDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_action_config'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("help_chain_action_id", LongType()),
        StructField("action_config_telegram_id", LongType()),
        StructField("action_config_email_id", LongType())
    ])

    @classmethod
    def extractActionConfigDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = ActionConfigDD.extractActionConfigDD()


if __name__ == "__main__":
    pass    
        