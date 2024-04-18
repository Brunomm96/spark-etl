from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class HelpChainEventQueueDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_help_chain_event_queue'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("status", StringType()),
        StructField("help_chain_action_id", LongType()),
        StructField("stop_record_id", LongType()),
        StructField("shift_id", LongType()),
        StructField("time_wait", LongType()),
        StructField("user_id_insert", LongType()),
        StructField("date_insert", StringType()),
        StructField("user_id_update", LongType()),
        StructField("date_update", StringType()),
        StructField("access_key", StringType()),
        StructField("loss_record_id", LongType()),
        StructField("production_unit_id", LongType()),
        StructField("level_group_key", StringType()),
        StructField("date_time_notify_begin", StringType()),
        StructField("date_time_notify_end", StringType())
    ])

    @classmethod
    def extractHelpChainEventQueueDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = HelpChainEventQueueDD.extractHelpChainEventQueueDD()


if __name__ == "__main__":
    pass    
        