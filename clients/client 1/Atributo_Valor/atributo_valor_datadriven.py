from pyspark.sql.types import *
from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json


class Dw_Attribute_Value:
    TOPIC = 'Client2_Debezium.datadriven_Client2.dbo.dw_attribute_values'

    SCHEMA = StructType([
        StructField("before",StringType()),
        StructField("after",StringType()),
        StructField("id",StringType()),
        StructField("technical_sheet_id",StringType()),
        StructField("attribute_id",StringType()),
        StructField("value_string",StringType()),
        StructField("value_number",StringType()),
        StructField("value_boolean",StringType()),
        StructField("value_date",StringType()),
        StructField("value_list",StringType()),
        StructField("record_access_key",StringType()),
        StructField("user_id_insert",StringType()),
        StructField("date_insert",StringType()),
        StructField("user_id_update",StringType()),
        StructField("date_update",StringType()),
        StructField("access_key",StringType()),
        StructField("flag_integration",StringType()),
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
    def extractDwAttributeValue(self):

        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")

        df = df.select(from_json(df.after, self.SCHEMA).alias("data")) \
            .select("data.*")
        
        return df


if __name__ == 'main':
    df_stg = Dw_Attribute_Value.extractDwAttributeValue()
    df_stg.show()



