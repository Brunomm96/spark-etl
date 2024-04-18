from pyspark.sql.functions import col, when, regexp_replace, lit
from pyspark.sql import SparkSession
from attribute.attribute import AttributeDD
from database.Upsert import Upsert


class Dim_Atributo:
    def __init__(self):
        self.spark = SparkSession.builder.appName("Shift").getOrCreate()
        self.client_name = 'Client2'
        self.table_name = 'Dim_Atributo'

    def extract(self):
        try:
            data = AttributeDD.extractAttributeDD()
        except Exception as e:
            self.log_error("Error during data extraction: {}".format(str(e)))
            raise
        return data


    def transform(self, data):
        df_atributo = data.withColumnRenamed("attribute", "nome") \
            .withColumn("classe", when(col("nome").like("%_bin%"), "categórico")
                        .when(col("nome").like("%_ana%"), "continuo")
                        .when(col("nome").like("%_int%"), "discreto")
                        .otherwise("Sem registro")) \
            .withColumn("tipo_atributo", lit("processo")) \
            .withColumn("codigo", regexp_replace(regexp_replace(regexp_replace(col("description"), " MAX", ""), " MIN", ""), "_set|_min|_max", "_val"))

        df_atributo = df_atributo.withColumn("nome", regexp_replace(
            regexp_replace(
                regexp_replace(col("nome"), '_set', '_val'),
                '_min', '_val'),
            '_max', '_val'
        ))

        df_atributo = df_atributo.withColumn(
            "classe",
            when(col("classe") == "continuo", "continua").otherwise(col("classe"))
        )
        df_atributo.show()

        df_atributo = df_atributo.select('codigo','nome', 'classe', 'tipo_atributo').dropDuplicates(['nome'])       
        df_atributo.show(truncate=False)
        return df_atributo

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
   atributo_dim = Dim_Atributo()
   dim_Atributo = atributo_dim.etl()
