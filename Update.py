import pandas as pd
from pyspark.sql.functions import when, lit
from pyspark.sql import SparkSession, DataFrame
from sqlalchemy import create_engine, MetaData, Table, update
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
import os
from dotenv import load_dotenv

load_dotenv()

class Update:
    def __init__(self, table_name, client):
        self.engine = self.connection(client)
        self.table_name = table_name
        self.client = client
        self.spark = SparkSession.builder.appName("UPSERT").getOrCreate()

    def connection(self, client):
        host = os.getenv(f'{client}_DB_HOST')
        port = os.getenv(f'{client}_DB_PORT')
        database = os.getenv(f'{client}_DB_NAME')
        username = os.getenv(f'{client}_DB_USER')
        password = os.getenv(f'{client}_DB_PASS')

        connection_string = f'mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server'

        engine = create_engine(connection_string)
        return engine
    
    def read_table(self, table_name):
        engine = self.connection(self.client)
        sql_query = f"SELECT * FROM {table_name}"
        result = engine.execute(sql_query)
        return result
    
    def upsert(self, df_source, key_column):
        
        if not isinstance(df_source, DataFrame):
            print("A função não recebeu um Dataframe de origem para classificar o upsert.")
            return None
        
        data_destiny = self.read_table(self.table_name)
        data_destiny = [dict(row) for row in data_destiny]

        df_destiny = self.spark.createDataFrame(data_destiny)

        if(df_source.columns != df_destiny.columns):
            print("Estrutura dos dataframes estão divergentes para classificar o UPSERT.")
            return None

        df_diff = df_source.subtract(df_destiny)
        df_diff = df_diff.alias("source").join(df_destiny.alias("destiny"), df_source[key_column] == df_destiny[key_column], "left")

        df_upsert = df_diff.withColumn("upsert", when(df_diff["destiny.codigo"].isNull(), "INSERT").otherwise("UPDATE"))      

        return df_upsert


    def update(self, spark_dataframe, columns, key_column_df, key_column_bd):
        try:
            pandas_df = spark_dataframe.toPandas()

            data = pandas_df.values.tolist()
            metadata = MetaData()
            table = Table(self.table_name, metadata, autoload_with=self.engine)

            connection = self.engine.connect()

            if connection:
                print("Conexão bem-sucedida!")

            for _, row in pandas_df.iterrows():
                update_values = {column: row[column] for column in columns}

                update_statement = (
                    update(table)
                    .where(getattr(table.c, key_column_bd) == row[key_column_df])
                    .values(**update_values)
                )
                connection.execute(update_statement)

            print("Dados atualizados na tabela.")

            connection.close()

        except Exception as e:
            print("Erro ao conectar:", e)

# Example usage:
table_name_to_update = 'Dim_Parada'
key_column_to_update = 'codigo'
client_name = 'UNIPAC_LIMEIRA'

update_instance = Update(table_name_to_update, client_name)
spark = SparkSession.builder.appName("Exemplo").getOrCreate()
schema = StructType([
    StructField("nome", StringType(), True),
    StructField("codigo", StringType(), True),
    StructField("descricao", StringType(), True)
])

data_to_update = [
    {'nome': 'TESTE PYSPARK M15SCE SMNBR UPL 2250357', 'codigo': '1230015', 'descricao': '2006 M15SCE SMNBR UPL 2250357'},
]

spark_df = spark.createDataFrame(data_to_update, schema=schema)

columns_to_update = ['nome', 'codigo', 'descricao']


update_instance.update(spark_df, columns_to_update, key_column_df='codigo', key_column_bd=key_column_to_update)
