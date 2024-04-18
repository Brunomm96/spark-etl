from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class OauthClientDetailsDD:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.oauth_client_details'
        
    SCHEMA = StructType([
        StructField("before", StringType()),
        StructField("after", StringType()),
        StructField("client_id", StringType()),
        StructField("resource_ids", StringType()),
        StructField("client_secret", StringType()),
        StructField("scope", StringType()),
        StructField("authorized_grant_types", StringType()),
        StructField("web_server_redirect_uri", StringType()),
        StructField("authorities", StringType()),
        StructField("access_token_validity", IntegerType()),
        StructField("refresh_token_validity", IntegerType()),
        StructField("additional_information", StringType()),
        StructField("autoapprove", StringType())
    ])

    @classmethod
    def extractOauthClientDetailsDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = OauthClientDetailsDD.extractOauthClientDetailsDD()


if __name__ == "__main__":
    pass    
        