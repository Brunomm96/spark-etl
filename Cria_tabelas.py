import os
import datetime
import pymssql
import pandas as pd
from dotenv import load_dotenv
import warnings
warnings.filterwarnings('ignore', message="pandas only support SQLAlchemy connectable")

load_dotenv()

clientes = [
    {"cliente_name": "unipac_limeira", "topico_name": "Unipac_Limeira", "SQL_SERVER_DATABASE": "datadriven_unipac_limeira"},
    
    # Adicione mais dicionários conforme necessário
]

# Processa cada conjunto de valores
for cliente in clientes:
    
    cliente_name = cliente["cliente_name"]
    topico_name = cliente["topico_name"]
    
    SQL_SERVER_DATABASE = cliente["SQL_SERVER_DATABASE"]
    SQL_SERVER_HOST = os.getenv("SQL_SERVER_HOST")
    SQL_SERVER_PORT = os.getenv("SQL_SERVER_PORT")
    SQL_SERVER_USER = os.getenv("SQL_SERVER_USER")
    SQL_SERVER_PASSWORD = os.getenv("SQL_SERVER_PASSWORD")
    cnxn = pymssql.connect(server=SQL_SERVER_HOST, port=SQL_SERVER_PORT, database=SQL_SERVER_DATABASE, user=SQL_SERVER_USER, password=SQL_SERVER_PASSWORD)


    # Função para mapear tipos de dados do SQL para Python/PySpark
    def map_data_type(sql_type):
        if sql_type in ['varchar', 'char', 'text', 'nvarchar', 'nchar', 'ntext']:
            return "StringType()"
        elif sql_type in ['int', 'smallint', 'tinyint']:
            return "IntegerType()"
        elif sql_type == 'bigint':
            return "LongType()"
        elif sql_type in ['float', 'real']:
            return "FloatType()"
        elif sql_type in ['decimal', 'numeric']:
            return "DecimalType()"
        elif sql_type == 'bit':
            return "BooleanType()"
        elif sql_type in ['datetime', 'smalldatetime']:
            return "TimestampType()"
        else:
            return "StringType()"  # Tipo padrão se não houver correspondência

    # Função para formatar o nome da classe
    def format_class_name(name):
        if name.startswith("dw_"):
            name = name[3:]
        return ''.join(word.capitalize() for word in name.split('_'))

    # Obtendo a lista de tabelas
    query_tables = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'dbo' AND table_type = 'BASE TABLE' 
    ORDER BY table_name;
    """
    tabelas_df = pd.read_sql(query_tables, cnxn)

    # Loop por cada tabela
    for index, row in tabelas_df.iterrows():
        table_name = row['table_name']
        class_name = format_class_name(table_name)

        # Montar a consulta para obter os nomes das colunas e tipos de dados
        query_columns = f"""
        select 'before' as COLUMN_NAME, 'varchar' as DATA_TYPE union all 
        select 'after' as COLUMN_NAME, 'varchar' as DATA_TYPE union all 
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}';
        """
        df = pd.read_sql(query_columns, cnxn)

        importacoes_e_classe = f"""from pyspark.sql.types import *
import sys
sys.path.insert(0, "/opt/spark/work-dir")

from extract.KafkaServer import KafkaServer
from pyspark.sql.functions import from_json

class {class_name}DD:
    TOPIC = '{topico_name}_Debezium.datadriven_{cliente_name}.dbo.{table_name}'
        """

        # Construir o SCHEMA com base nos nomes das colunas e tipos de dados
        SCHEMA = "    SCHEMA = StructType([\n" + ",\n".join([f'        StructField("{row["COLUMN_NAME"]}", {map_data_type(row["DATA_TYPE"])})' for index, row in df.iterrows()]) + "\n    ])"

        fim = f"""
    @classmethod
    def extract{class_name}DD(self):
        df = KafkaServer()
        df = df.get_df(self.SCHEMA, self.TOPIC, stream=False)
        df = df.selectExpr("CAST(after as STRING)")
        df = df.select(from_json(df.after, self.SCHEMA).alias("data")).select("data.*")
        df.show()
        return df

df_stg = {class_name}DD.extract{class_name}DD()


if __name__ == "__main__":
    pass    
        """

        script_completo = importacoes_e_classe + "\n" + SCHEMA + "\n" + fim

        formatted_table_name = format_class_name(table_name).lower()
        pasta = rf"clients/{cliente_name}/{formatted_table_name}"
        arquivo = f"{formatted_table_name}.py"

        # Criar a pasta se ela não existir
        if not os.path.exists(pasta):
            os.makedirs(pasta)

        # Caminho completo do arquivo
        caminho_completo = os.path.join(pasta, arquivo)

        # Verificar se o arquivo já existe
        if os.path.exists(caminho_completo):
            data_atual = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
            novo_nome = f"{formatted_table_name}_{data_atual}.py"
            os.rename(caminho_completo, os.path.join(pasta, novo_nome))

        # Criar o novo arquivo e escrever o conteúdo
        with open(caminho_completo, 'w') as f:
            f.write(script_completo)

        print(f"Arquivo {arquivo} criado.")

    # Fechar a conexão
    cnxn.close()




