from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class UserAppPermissionGroupDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_user_app_permission_group'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("user_app_id", LongType()),
        StructField("permission_group_id", LongType())
    ])

    @classmethod
    def extractUserAppPermissionGroupDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = UserAppPermissionGroupDD.extractUserAppPermissionGroupDD()


if __name__ == "__main__":
    pass    
        