from pyspark.sql.functions import col, from_json, concat_ws, udf, explode, split, expr, monotonically_increasing_id
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from integrationqueue.integrationqueue import IntegrationQueueDD
from database.Upsert import Upsert
import base64


class Dim_Materia_Prima:
    def __init__(self):
        self.spark = SparkSession.builder.appName("IntegrationQueueDD").getOrCreate()
        self.client_name = 'Client2'
        self.table_name = 'Dim_Materia_Prima'

    def extract(self):
        try:
            data = IntegrationQueueDD.extractIntegrationQueueDD()
        except Exception as e:
            self.log_error("Error during data extraction: {}".format(str(e)))
            raise
        return data

    def transform(self, data):
        def decode_base64(data):
            return base64.b64decode(data).decode('utf-8')

        decode_base64_udf = udf(decode_base64, StringType())

        data_encoded = data.withColumn("decodedData", decode_base64_udf(col("data")))

        json_schema = ArrayType(StructType([
            StructField("seq", ArrayType(StructType([
                StructField("operacoes", ArrayType(StructType([
                    StructField("componentes", ArrayType(StructType([
                        StructField("componente", StringType()),
                        StructField("denominacao", StringType()),
                    ]))),  
                ]))),  
            ]))),
        ]))

        df_queue = data_encoded.withColumn("jsonData", from_json(col("decodedData"), json_schema))

        nome = df_queue.select(explode("jsonData.seq").alias("seq")) \
            .select(explode("seq.operacoes").alias("operacoes")) \
            .select(explode("operacoes.componentes").alias("componentes")) \
            .select(concat_ws(", ", "componentes.denominacao").alias("denominacao_as_string"))
        split_nome = nome.withColumn("denominacao_split", split("denominacao_as_string", ", "))
        resultado_nome = split_nome.select(explode("denominacao_split").alias("nome"))
        resultado_nome.show(truncate=False)

        codigo = df_queue.select(explode("jsonData.seq").alias("seq")) \
            .select(explode("seq.operacoes").alias("operacoes")) \
            .select(explode("operacoes.componentes").alias("componentes")) \
            .select(concat_ws(", ", "componentes.componente").alias("denominacao_as_string"))
        split_codigo = codigo.withColumn("denominacao_split", split("denominacao_as_string", ", "))
        resultado_codigo = split_codigo.select(explode("denominacao_split").alias("codigo"))
        resultado_codigo = resultado_codigo.withColumn("codigo", expr(r"regexp_extract(codigo, '0*([0-9]+)$', 1)"))

        resultado_nome = resultado_nome.withColumn("index", monotonically_increasing_id())
        resultado_codigo = resultado_codigo.withColumn("index", monotonically_increasing_id())
        combined_result = resultado_nome.join(resultado_codigo, "index").drop("index")
        combined_result = combined_result.select('codigo', 'nome').dropDuplicates(['codigo'])
        df = combined_result.filter(col("codigo") != "")

        return df

    def load(self, data):
        upsert = Upsert(data,self.table_name,self.client_name,'codigo')
        upsert.upsert(data,'codigo')

    def etl(self):
        try:
            extracted_data_dict = self.extract()
        except:

            raise Exception("data extraction failed")

        try:
            transformed_data_dict = self.transform(extracted_data_dict)
        except:
            raise Exception("data transformation failed")

        try:
            self.load(transformed_data_dict)
        except:
            raise Exception("data load failed")

        return {"ETL performed successfully"}

if __name__ == "__main__":
   produto_dim = Dim_Materia_Prima()
   dim_Materia_Prima = produto_dim.etl()
