from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class LossMonitoringOutOfBoundDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_loss_monitoring_out_of_bound'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("appointment_date", StringType()),
        StructField("production_order_id", LongType()),
        StructField("product_code", StringType()),
        StructField("production_unit_id", LongType()),
        StructField("refugo_id", LongType()),
        StructField("tolerance_quantity", FloatType()),
        StructField("loss_quantity", FloatType()),
        StructField("date_insert", StringType()),
        StructField("access_key", StringType()),
        StructField("help_chain_id", IntegerType())
    ])

    @classmethod
    def extractLossMonitoringOutOfBoundDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = LossMonitoringOutOfBoundDD.extractLossMonitoringOutOfBoundDD()


if __name__ == "__main__":
    pass    
        