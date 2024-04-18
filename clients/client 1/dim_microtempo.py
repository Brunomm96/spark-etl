from pyspark.sql.functions import col, from_json, concat, udf, explode, lit, expr
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql import SparkSession
from attributegroup.attributegroup import AttributeGroupDD
from attribute.attribute import AttributeDD
from database.Upsert import Upsert


class Dim_Microtempo:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Microtempo").getOrCreate()
        self.client_name = 'Client2'
        self.table_name = 'Dim_Microtempo'
        self.AttributeGroupDD = AttributeGroupDD.extractAttributeGroupDD()
        self.AttributeDD = AttributeDD.extractAttributeDD()
        self.spark.sparkContext.setLogLevel("ERROR")


    def extract(self):
        try:
            data_attr_group = self.AttributeGroupDD
            data_attr = self.AttributeDD
        except Exception as e:
            print("Erro durante a extração de dados: {}".format(str(e)))
            raise
        return data_attr_group, data_attr

    def transform(self, data_attr_group, data_attr):
        data_attr_group = data_attr_group.select('id', 'description')
        data_attr = data_attr.select('*')

        df_microtempo = data_attr_group.alias('group').join(
            data_attr.alias('attr'),
            (col('group.id') == col('attr.attribute_group_id')) & (col('group.id') == 1),
            'inner'
        ).select('attr.id', 'attr.title')

        df_result = df_microtempo.withColumnRenamed("id", "codigo") \
                                     .withColumnRenamed("title", "nome") 
        df_result = df_result.select('codigo', 'nome')
        return df_result

    def load(self, data):
        upsert = Upsert(data,self.table_name,self.client_name,'codigo')
        upsert.upsert(data,'codigo')

    def etl(self):
        try:
            extracted_data_dict = self.extract()
            transformed_data_dict = self.transform(*extracted_data_dict)
        except Exception as e:
            print("Erro durante o processo ETL: {}".format(str(e)))
            raise
        try:
            self.load(transformed_data_dict)
        except:
            raise Exception("data load failed")

        return {"ETL performed successfully"}

if __name__ == "__main__":
   microtempo_dim = Dim_Microtempo()
   dim_microtempo = microtempo_dim.etl()
