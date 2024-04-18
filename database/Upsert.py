import pandas as pd
from database.Sql import Database
from pyspark.sql import DataFrame
from pyspark.sql.functions import when, lit
from dotenv import load_dotenv

load_dotenv()

class Upsert:
    def __init__(self, dataframe, table_name, client_name, key_column):
        self.client_name = client_name.upper()
        self.table_name = table_name
        self.dataframe = dataframe
        self.key_column = key_column


    def run(self, df_source, key_column):
        pass

    def getDf(self):
        pass


    def upsert(self, df_source, key_column):
        if not isinstance(df_source, DataFrame):
            print("A função não recebeu um Dataframe de origem para classificar o upsert.")
            return None
        
        keys = df_source.select(key_column).rdd.flatMap(lambda x: x).collect()
        
        db = Database(self.client_name)
        df_destiny = db.readDataByKey(self.table_name, key_column, keys)
        columns_name_source = df_source.columns
        columns_name_destiny = df_destiny.columns              

        if key_column not in columns_name_destiny or key_column not in columns_name_source:
            print("A coluna chave não existe nos dataframes de origem e destino.")
            return None

        list_columns_name_source = set(df_source.columns)         
        list_columns_name_destiny = set(df_destiny.columns)     

        if not list_columns_name_source.issubset(list_columns_name_destiny):
            print("Colunas de origem estão divergentes da coluna de destino.")
            return None

        df_destiny = df_destiny.select(columns_name_source)
        df_diff = df_source.subtract(df_destiny)          

        if df_diff.isEmpty():
            print("DADOS DE ORIGEM E DESTINO JÁ ESTÃO SINCRONIZADOS")
            return None

        if not df_destiny.isEmpty():
            df_diff = df_diff.alias("source").join(df_destiny.alias("destiny"), df_source[key_column] == df_destiny[key_column], "left")

            df_upsert = df_diff.withColumn("upsert", when(df_diff[f"destiny.{key_column}"].isNull(), "INSERT").otherwise("UPDATE"))
            df_upsert.show()

            df_insert = df_upsert.select("source.*").filter(df_upsert["upsert"]=="INSERT")
        
        if df_destiny.isEmpty():
            df_insert = df_source
            db.write_data(df_insert, self.table_name)
            return None

        if not df_insert.isEmpty():
            print("INSERT")
            db.write_data(df_insert, self.table_name)

        df_update = df_upsert.select("source.*").filter(df_upsert["upsert"]=="UPDATE")

        if not df_update.isEmpty():
            print("UPDATE")
            db.update_data(df_update,columns_name_source,self.table_name,key_column,key_column)
            
