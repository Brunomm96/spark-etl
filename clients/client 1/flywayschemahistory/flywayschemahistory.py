from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class FlywaySchemaHistoryDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.flyway_schema_history'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("installed_rank", IntegerType()),
        StructField("version", StringType()),
        StructField("description", StringType()),
        StructField("type", StringType()),
        StructField("script", StringType()),
        StructField("checksum", IntegerType()),
        StructField("installed_by", StringType()),
        StructField("installed_on", TimestampType()),
        StructField("execution_time", IntegerType()),
        StructField("success", BooleanType())
    ])

    @classmethod
    def extractFlywaySchemaHistoryDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = FlywaySchemaHistoryDD.extractFlywaySchemaHistoryDD()


if __name__ == "__main__":
    pass    
        