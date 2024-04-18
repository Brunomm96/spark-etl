from pyspark.sql.types import *
from extract.KafkaServer import KafkaServer
class AtributoProcessoDD:
    TOPIC = 'Client2_Atributo'

    SCHEMA = StructType() \
        .add("id_microtempo", IntegerType()) \
        .add("etiqueta_valor", StringType()) \
        .add("nr_ordem_producao", StringType()) \
        .add("microtempo_nome", StringType()) \
        .add("microtempo_seq", StringType()) \
        .add("microtempo_id_planta", StringType()) \
        .add("operacao_nome", StringType()) \
        .add("operacao_seq", StringType()) \
        .add("data_atributo", TimestampType()) \
        .add("atributo_nome", StringType()) \
        .add("atributo_codigo", StringType()) \
        .add("valor", StringType()) \
        .add("produto_codigo", StringType()) \
        .add("codigo_unidade_producao", StringType())

    @classmethod
    def extractAtributoProcessoDD(cls):
        kafka_server = KafkaServer()
        df = kafka_server.get_df(cls.SCHEMA, cls.TOPIC, stream=False)
        df = df.select("*")
        return df
