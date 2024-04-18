from pyspark.sql import SparkSession
from dotenv import load_dotenv
from Update import Update
import os

load_dotenv()

spark = SparkSession.builder \
    .appName("Spark - MSSQL") \
    .config("spark.jars", "/opt/spark/jars/mssql.jar") \
    .getOrCreate()

class Database:
    def __init__(self, source_id):  
        self.source_id = source_id

        jdbcHostname = os.getenv(f"{source_id.upper()}_DB_HOST")
        jdbcDatabase = os.getenv(f"{source_id.upper()}_DB_NAME")
        jdbcPort = os.getenv(f"{source_id.upper()}_DB_PORT")
        jdbcUsername = os.getenv(f"{source_id.upper()}_DB_USER")
        jdbcPassword = os.getenv(f"{source_id.upper()}_DB_PASS")
        
        self.jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};encrypt=false"
        
        self.connectionProperties = {
            "user": os.getenv(f"{source_id.upper()}_DB_USER"),    
            "password": os.getenv(f"{source_id.upper()}_DB_PASS"),
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        }

    def read_existing_data(self, table_name):
        return spark.read.jdbc(url=self.jdbcUrl, table=table_name, properties=self.connectionProperties)
    
    def readDataByKey(self, table_name, key_column, key_value):
        delimit = "','"
        filter_condition = f"WHERE {key_column} IN ('{delimit.join(map(str, key_value))}')"
        sql_query = f"(SELECT * FROM {table_name} {filter_condition}) as query"
        df_table = spark.read.jdbc(url=self.jdbcUrl, table=sql_query, properties=self.connectionProperties)
        return df_table
        
    def update_data(self, data=None, columns=None, table_name=None, key_column_df=None, key_column_bd=None):
        update_instance = Update(table_name, self.source_id)
        update_instance.update(data, columns, key_column_df, key_column_bd)

    def write_data(self, data, table_name):
        data.write.jdbc(url=self.jdbcUrl, table=table_name, mode="append", properties=self.connectionProperties)


        

# table_name_to_update = 'Dim_Produto'
# key_column_to_update = 'codigo'
# client_name = 'UNIPAC_LIMEIRA'

# database = Database(client_name)
# data_to_update = [
#     {'descricao': 'CRP20 M15SCE SMNBR UPL 2250357', 'codigo': '1230015', 'nome': 'RG - 1230015'},
#     {'descricao': 'CRP20 M15SCE SFLNT FMC 22020826', 'codigo': '1240715', 'nome': 'RG - 1240715'},
#     {'descricao': 'CRP20 M15SCE SMNBR SYNG 4081117', 'codigo': '1246357', 'nome': 'RG - 1246357'},
#     {'descricao': 'CRP20 M15SCE SMNBR OUROFINO 50000910', 'codigo': '1276451', 'nome': 'RG - 1276451'},
#     {'descricao': 'CRP20 M15SCE SMNBR OUROFINO 50005162', 'codi
        # go': '1282585', 'nome': 'RG - 1282585'},
#     {'descricao': 'CRP20 M15SCE SMNBR SUMITOMO 30040291', 'codigo': '1284542', 'nome': 'RG - 1284542'},
#     {'descricao': 'CRP20 M15SCE SMNBR ISK 50004787', 'codigo': '1293439', 'nome': 'RG - 1293439'},
#     {'descricao': 'CRP20 M15SCE SMNBR SYNG 4151966', 'codigo': '1298546', 'nome': 'RG - 1298546'},
#     {'descricao': 'CRP20 M15SCE SMNBR REC SYNG 4151966', 'codigo': '1298563', 'nome': 'RG - 1298563'},
#     {'descricao': 'CRP20 M15SCE SMNNT SPL SYNG 4151964', 'codigo': '1298567', 'nome': 'RG - 1298567'},
#     {'descricao': 'CRP20 M15SCE SFLNT SYNG 4151964', 'codigo': '1298573', 'nome': 'RG - 1298573'},
#     {'descricao': 'CRP20 M15SCE SMNBR SYNG 4151966', 'codigo': '1298599', 'nome': 'RG - 1298599'},
#     {'descricao': 'CRP20 M15SCE SMNNT ALIMENTOS WILSON', 'codigo': '1299260', 'nome': 'RG - 1299260'},
#     {'descricao': 'CRP20 M15SCE SMNBR UBYFOL 040020004', 'codigo': '1305786', 'nome': 'RG - 1305786'},
#     {'descricao': 'CRP20 M15SCE SMNBR AMTEC', 'codigo': '1311475', 'nome': 'RG - 1311475'},
#     {'descricao': 'CRP20 M15SCE SMNBR SPL UPL 2250658', 'codigo': '1320090', 'nome': 'RG - 1320090'},
#     {'descricao': 'CRP20 M15SCE SMNBR GREEN SYNG 4151966', 'codigo': '1322682', 'nome': 'RG - 1322682'},
#     {'descricao': 'CRP20 M15SCE SMNBR GREEN SYNG 4151966', 'codigo': '1322686', 'nome': 'RG - 1322686'}
# ]
# columns_to_update = ['nome', 'codigo']

# database.write_data(data_to_update, table_name_to_update)
