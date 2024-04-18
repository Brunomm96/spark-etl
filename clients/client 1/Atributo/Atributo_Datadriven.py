from pyspark.sql.types import *
from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json


class Dw_Attribute:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_attribute'

    SCHEMA = StructType([
        StructField("before",StringType()),
        StructField("after",StringType()),
        StructField("id",StringType()),
        StructField("attribute",StringType()),
        StructField("title",StringType()),
        StructField("description",StringType()),
        StructField("active",StringType()),
        StructField("type",StringType()),
        StructField("size",StringType()),
        StructField("precision",StringType()),
        StructField("list_options",StringType()),
        StructField("user_id_insert",StringType()),
        StructField("date_insert",StringType()),
        StructField("user_id_update",StringType()),
        StructField("date_update",StringType()),
        StructField("access_key",StringType()),
        StructField("editable",StringType()),
        StructField("attribute_group_id",StringType()),
        StructField("required",StringType()),
        StructField("microtime_id",StringType()),
        StructField("source",StringType()),
        StructField("version",StringType()),
        StructField("connector",StringType()),
        StructField("name",StringType()),
        StructField("ts_ms",StringType()),
        StructField("snapshot",StringType()),
        StructField("db",StringType()),
        StructField("sequence",StringType()),
        StructField("schema",StringType()),
        StructField("table",StringType()),
        StructField("change_lsn",StringType()),
        StructField("commit_lsn",StringType()),
        StructField("event_serial_no",StringType()),
        StructField("op",StringType()),
        StructField("ts_ms",StringType()),
        StructField("transaction",StringType())
    ])

    @classmethod
    def extractDwAttribute(self):

        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")

        df = df.select(from_json(df.after, self.SCHEMA).alias("data")) \
            .select("data.*")
        
        df.show()
        return df

df_stg = Dw_Attribute.extractDwAttribute()


if __name__ == "__main__":
        pass



