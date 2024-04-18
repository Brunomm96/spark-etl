from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class RefugosDD:
    TOPIC = 'Client2_Refugos'
        
    schema_refugos = StructType() \
            .add("data_apontamento", TimestampType()) \
            .add("refugo_quantidade", StringType()) \
            .add("nr_ordem_producao", StringType()) \
            .add("codigo_refugo", StringType()) \
            .add("codigo_unidade_producao", StringType()) \
            .add("codigo_operador", StringType()) \
            .add("id_op_apontamento", IntegerType())
        
    @classmethod
    def extractRefugosDD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = RefugosDD.extractRefugosDD()


if __name__ == "__main__":
    pass    
        