from pyspark.sql.functions import col, from_json, concat, udf, explode, lit, expr, when
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql import SparkSession
from stoprecord.stoprecord import StopRecordDD
from category.category import CategoryDD
from database.Upsert import Upsert
import base64


class Dim_Parada:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Shift").getOrCreate()
        self.client_name = 'Client2'
        self.table_name = 'Dim_Parada'
        self.StopRecordDD = StopRecordDD.extractStopRecordDD()
        self.Category = CategoryDD.extractCategoryDD()

    def extract(self):
        try:
            data_stop_record = self.StopRecordDD
            data_category = self.Category
        except Exception as e:
            print("Erro durante a extração de dados: {}".format(str(e)))
            raise
        return data_stop_record, data_category


    def transform(self, data_stop_record, data_category):
        df_stop = data_stop_record.withColumnRenamed("id", "natural_key") \
        .withColumnRenamed("code", "codigo") \
        .withColumnRenamed("description", "nome") \
        .withColumnRenamed("pnp_pp", "tipo") \
        .withColumnRenamed("auto_fechamento", "oee_sn") \
        
        data_cat = data_category.withColumnRenamed("description", "categoria") \
        .withColumnRenamed("id", "id")

        df_parada = df_stop.alias('stop').join(
            data_cat.alias('category'),
            (col('stop.category_id') == col('category.id')),
            'inner'
        ).withColumn("oee_sn", when(col("descontar_producao") == "SIM", "S").otherwise("N")).select('stop.natural_key', 'stop.codigo', 'stop.nome', 'stop.tipo','category.categoria', 'oee_sn')
    
        return df_parada

    def load(self, data):
        upsert = Upsert(data,self.table_name,self.client_name,'codigo')
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
   parada_dim = Dim_Parada()
   dim_parada = parada_dim.etl()
