from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class ConsumptionMaterialOutOfBoundDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_consumption_material_out_of_bound'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("id", LongType()),
        StructField("appointment_date", StringType()),
        StructField("production_order", StringType()),
        StructField("product_code", StringType()),
        StructField("component_code", StringType()),
        StructField("tolerance_percent_min", FloatType()),
        StructField("tolerance_percent_max", FloatType()),
        StructField("tolerance_quantity", FloatType()),
        StructField("appointed_amount", FloatType()),
        StructField("planned_quantity", FloatType()),
        StructField("amount_revenue", FloatType()),
        StructField("quantity_per_piece", FloatType()),
        StructField("help_chain_id", LongType()),
        StructField("tolerance_value_min", FloatType()),
        StructField("tolerance_value_max", FloatType()),
        StructField("date_insert", StringType()),
        StructField("access_key", StringType()),
        StructField("consumo_material_apontamento_id", IntegerType())
    ])

    @classmethod
    def extractConsumptionMaterialOutOfBoundDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = ConsumptionMaterialOutOfBoundDD.extractConsumptionMaterialOutOfBoundDD()


if __name__ == "__main__":
    pass    
        