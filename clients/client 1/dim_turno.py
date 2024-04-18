from pyspark.sql.functions import col, from_json, concat, udf, explode, lit, expr
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql import SparkSession
from shift.shift import ShiftDD
from database.Upsert import Upsert
import base64


class Dim_Turno:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Shift").getOrCreate()
        self.client_name = 'Client2'
        self.table_name = 'Dim_Turno'

    def extract(self):
        try:
            data = ShiftDD.extractShiftDD()
        except Exception as e:
            self.log_error("Error during data extraction: {}".format(str(e)))
            raise
        return data


    def transform(self, data):
        df_turno = data.withColumnRenamed("id", "natural_key") \
        .withColumnRenamed("code", "codigo") \
        .withColumnRenamed("description", "descricao") \
        .withColumnRenamed("hour_begin", "hora_inicio") \
        .withColumnRenamed("hour_end", "hora_fim") \
        .withColumnRenamed("journey", "jornada")

    

        df_turno = df_turno.select('natural_key', 'codigo', 'descricao', 'hora_inicio', 'hora_fim', 'jornada')

        return df_turno

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
   turno_dim = Dim_Turno()
   dim_turno = turno_dim.etl()
