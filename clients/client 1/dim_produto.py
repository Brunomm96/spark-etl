from pyspark.sql.functions import col, from_json, concat, udf, explode, lit, expr, concat_ws, monotonically_increasing_id
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql import SparkSession
from integrationqueue.integrationqueue import IntegrationQueueDD
from attribute.attribute import AttributeDD
from attributevalues.attributevalues import AttributeValuesDD
from database.Upsert import Upsert
import base64


class Dim_Produto:
    def __init__(self):
        self.spark = SparkSession.builder.appName("IntegrationQueueDD").getOrCreate()
        self.client_name = 'Client2'
        self.table_name = 'Dim_Produto'
        self.spark.sparkContext.setLogLevel("ERROR")


    def extract(self):
        try:
            integration_queue = IntegrationQueueDD.extractIntegrationQueueDD()
            attribute = AttributeDD.extractAttributeDD()
            attribute_values = AttributeValuesDD.extractAttributeValuesDD()
        except Exception as e:
            self.log_error("Error during data extraction: {}".format(str(e)))
            raise
        return integration_queue, attribute, attribute_values 


    def transform(self, integration_queue, attribute, attribute_values):
        def decode_base64(data):
            return base64.b64decode(data).decode('utf-8')

        decode_base64_udf = udf(decode_base64, StringType())

        data_enconde = integration_queue.withColumn("decodedData", decode_base64_udf(col("data")))


        json_schema = ArrayType(StructType([
                StructField("seq", StringType()),
                StructField("rgMaterial", StringType()),
                StructField("descricaoProduto", StringType())
        ]))

        df_queue = data_enconde.withColumn("jsonData", from_json(col("decodedData"), json_schema))

        codigo = df_queue.select("jsonData.seq", explode(col("jsonData.rgMaterial")).alias("codigo"))
        codigo = codigo.withColumn("codigo", expr(r"regexp_extract(codigo, '0*([0-9]+)$', 1)"))


        df_temp = df_queue.select("jsonData.seq", explode(col("jsonData.rgMaterial")).alias("nome_temp"))
        df_temp = df_temp.withColumn("nome_temp", expr(r"regexp_extract(nome_temp, '0*([0-9]+)$', 1)"))
        df_temp = df_temp.withColumn("nome", concat(lit("RG "), col("nome_temp")))

        descricao = df_queue.select("jsonData.seq", explode(col("jsonData.descricaoProduto")).alias("descricao"))

        df = codigo.join(df_temp, "seq")
        df = df.join(descricao, "seq")
        
        df = df.select('codigo', 'nome', 'descricao').dropDuplicates(['codigo'])

        ftRG = attribute.join(attribute_values, attribute_values.attribute_id == attribute.id) \
                .where((attribute.attribute == 'productKey_str') | (attribute.attribute == 'productKey'))\
                .select('value_string').withColumnRenamed('value_string','codigo')
        
        ftRG = ftRG.withColumn("nome", concat(lit("RG "), col("codigo")))
        
        ftRGDesc = attribute.join(attribute_values, attribute_values.attribute_id == attribute.id) \
                .where((attribute.attribute == 'productDescription_str') | (attribute.attribute == 'productDescription'))\
                .select('value_string').withColumnRenamed('value_string', 'descricao')      
        
        ftRG = ftRG.withColumn("index", monotonically_increasing_id())
        ftRGDesc = ftRGDesc.withColumn("index", monotonically_increasing_id())
        aux_rg = ftRG.join(ftRGDesc, "index").drop("index")

        result = aux_rg.union(df)
        result = result.dropDuplicates(['codigo'])
        result.write.csv('join', header=True, mode='overwrite')

        return result

    def load(self, data):
        upsert = Upsert(data,self.table_name,self.client_name,'left')
        upsert.upsert(data,'codigo')

    def etl(self):
        try:
            extracted_data_dict = self.extract()
        except:

            raise Exception("data extraction failed")

        try:
            transformed_data_dict = self.transform(*extracted_data_dict)
        except:
            raise Exception("data transformation failed")

        try:
            self.load(transformed_data_dict)
        except:
            raise Exception("data load failed")

        return {"ETL performed successfully"}

if __name__ == "__main__":
   produto_dim = Dim_Produto()
   dim_produto = produto_dim.etl()
