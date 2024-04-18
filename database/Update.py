import pandas as pd
from pyspark.sql.functions import when, lit
from pyspark.sql import SparkSession, DataFrame
from sqlalchemy import create_engine, MetaData, Table, update
import os
import json
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
    
    def readTableByKey(self, table_name, key_column, key):
        engine = self.connection(self.client)
        filter_condition = f"WHERE {key_column} IN ({', '.join(map(str, key))})"
        sql_query = f"SELECT * FROM {table_name} {filter_condition}"
        result = engine.execute(sql_query)
        return result
    
    def upsert(self, df_source, key_column):
        
        if not isinstance(df_source, DataFrame):
            print("A função não recebeu um Dataframe de origem para classificar o upsert.")
            return None
        
        keys = df_source.select(key_column).rdd.flatMap(lambda x: x).collect()
        
        columns_name_source = df_source.columns
        
        data_destiny = self.readTableByKey(self.table_name, key_column, keys)
        columns_name_destiny = list(data_destiny.keys())

        data_destiny = list(data_destiny.fetchall())
        df_pd = pd.DataFrame(data_destiny, columns=columns_name_destiny)
        df_pd = df_pd.fillna(value="")
        df_destiny = self.spark.createDataFrame(df_pd)
        df_destiny = df_destiny.select(columns_name_source)

        if key_column not in columns_name_destiny or key_column not in columns_name_source:
            print("A coluna chave não existe nos dataframes de origem e destino.")

        columns_name_source = set(columns_name_source)
        columns_name_destiny = set(columns_name_destiny)
        print(columns_name_source)
        print(columns_name_destiny)

        if not columns_name_source.issubset(columns_name_destiny):
            print("Colunas de origem estão divergentes da coluna de destino.")

        df_diff = df_source.subtract(df_destiny)

        if df_diff.isEmpty():
            print("DADOS DE ORIGEM E DESTINO JÁ ESTÃO SINCRONIZADOS")

        df_diff = df_diff.alias("source").join(df_destiny.alias("destiny"), df_source[key_column] == df_destiny[key_column], "left")

        df_upsert = df_diff.withColumn("upsert", when(df_diff[f"destiny.{key_column}"].isNull(), "INSERT").otherwise("UPDATE"))

        df_insert = df_upsert.select("source.*").filter(df_upsert["upsert"]=="INSERT")

        df_update = df_upsert.select("source.*").filter(df_upsert["upsert"]=="UPDATE")

        if not df_update.isEmpty():
            cols = df_update.columns
            df_update.show()
            cols.remove(key_column)
            self.update(df_update,cols,key_column,key_column)

        return df_insert
        

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
