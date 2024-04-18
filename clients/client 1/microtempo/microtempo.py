from pyspark.sql.types import *
from extract.KafkaServer import KafkaServer


class MicrotempoDD:
    TOPIC = 'Client2_Microtempo'

    SCHEMA = StructType() \
    .add("operacao_nome", StringType()) \
    .add("microtempo_nome", StringType()) \
    .add("nr_ordem_producao", StringType()) \
    .add("turno_codigo", StringType()) \
    .add("colaborador_codigo", StringType()) \
    .add("ciclo_nro", StringType()) \
    .add("id", IntegerType()) \
    .add("datahorainicio", TimestampType()) \
    .add("datahorafinal", TimestampType()) \
    .add("etiqueta_valor", StringType())

    @classmethod
    def extractMicrotempoDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)        
        df = df.select("*")  
        return df


