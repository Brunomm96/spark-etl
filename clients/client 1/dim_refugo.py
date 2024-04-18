from pyspark.sql.functions import col, from_json, concat, udf, explode, lit, expr
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql import SparkSession
from lossrecord.lossrecord import LossRecordDD
from database.Upsert import Upsert
import base64


class Dim_Refugo:
    def __init__(self):
        self.spark = SparkSession.builder.appName("LossRecord").getOrCreate()
        self.client_name = 'Client2'
        self.table_name = 'Dim_Refugo'

    def extract(self):
        try:
            data = LossRecordDD.extractLossRecordDD()
        except Exception as e:
            self.log_error("Error during data extraction: {}".format(str(e)))
            raise
        return data


    def transform(self, data):
        df_refugo = data.withColumnRenamed("description", "nome") \
        .withColumnRenamed("code", "codigo")     

        df_refugo = df_refugo.select('nome', 'codigo').dropDuplicates(['codigo'])
        return df_refugo

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
   refugo_di = Dim_Refugo()
   dim_refugo = refugo_di.etl()
