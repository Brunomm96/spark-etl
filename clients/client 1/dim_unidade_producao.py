
from pyspark.sql.functions import col, from_json, concat, udf, explode, lit, expr
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql import SparkSession
from productionunit.productionunit import ProductionUnitDD
from database.Upsert import Upsert


class Dim_Unidade_Producao:
    def __init__(self):
        self.spark = SparkSession.builder.appName("ProducaoUnit").getOrCreate()
        self.client_name = 'Client2'
        self.table_name = 'Dim_Unidade_Producao'

    def extract(self):
        try:
            data = ProductionUnitDD.extractProductionUnitDD()
        except Exception as e:
            self.log_error("Error during data extraction: {}".format(str(e)))
            raise
        return data
    

    def transform(self, data):
        df_uni_prod = data.withColumnRenamed("id", "natural_key") \
        .withColumnRenamed("code", "codigo") \
        .withColumnRenamed("name", "nome") \
        .withColumnRenamed("production_unit_id_parent", "unidade_pai") \

    

        df_uni_prod = df_uni_prod.select('natural_key', 'codigo', 'nome', 'unidade_pai')
        df_uni_prod.show()

        return df_uni_prod

    def load(self, data):
        upsert = Upsert(data,self.table_name,self.client_name,'codigo')
        upsert.upsert(data,'codigo')


    def etl(self):
        try:
            extracted_data_dict = self.extract()
            extracted_data_dict.show()
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
   unidade_producao_dim = Dim_Unidade_Producao()
   dim_unidade_producao = unidade_producao_dim.etl()
