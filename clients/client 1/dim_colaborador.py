from pyspark.sql.functions import col, when
from pyspark.sql import SparkSession
from employee.employee import EmployeeDD
from database.Upsert import Upsert

class DimColaborador:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Employee").getOrCreate()
        self.client_name = 'Client2'
        self.table_name = 'Dim_Colaborador'

    def extract(self):
        try:
            data = EmployeeDD.extractEmployeeDD()
        except Exception as e:
            self.log_error("Error during data extraction: {}".format(str(e)))
            raise
        return data


    def transform(self, data):
        df_colab = data.withColumnRenamed("name", "nome").withColumnRenamed('id', 'natural_key').withColumnRenamed("re", "codigo").withColumn(
            "status",
            when(col("active") == "SIM", "ATIVO")
            .when(col("active") == "NAO", "INATIVO")
            .otherwise("")
        )

        df = df_colab.select('nome', 'codigo', 'natural_key', 'status').distinct()
      
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
    colaborador_dim = DimColaborador()
    dim_colab = colaborador_dim.etl()
