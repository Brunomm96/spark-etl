from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_json, to_date, when, when, hour, minute, second, format_string
from pyspark.sql.types import *
from atributo_processo.atributo_processo import AtributoProcessoDD
from microtempo.microtempo import MicrotempoDD
from utils.utils import Utils 
 

# Inicializar a sessão Spark

spark = SparkSession.builder \
    .appName("Kafka Stream Example") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .config("spark.jars", "/home/matheusql/Executaveis/Spark_Kafka/drivers/mssql.jar") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()



class Fato_Producao:
    def __init__(self):
        
        self.spark = SparkSession.builder.appName("Producao").getOrCreate()
        self.client_name = 'Client2'
        self.table_name = 'Fato_Producao'
        self.AtributoProcessoDD = AtributoProcessoDD.extractAtributoProcessoDD()
        self.MicrotempoDD = MicrotempoDD.extractMicrotempoDD()

    def extract(self):
        try:
            df_atributo_processo = self.AtributoProcessoDD
            df_microtempo = self.MicrotempoDD
        except Exception as e:
            self.log_error("Error during data extraction: {}".format(str(e)))
            raise

        return df_atributo_processo, df_microtempo
    

    def transform(self, df_atributo_processo, df_microtempo):    
    
    
        df_microtempo.show()
        df_atributo_processo.show()

        df_microtempo = (df_microtempo
                        .withColumnRenamed("microtempo_nome", "microtempo1")
                        .withColumnRenamed("etiqueta_valor", "etiqueta_valor1")
                        .withColumnRenamed("nr_ordem_producao", "nr_ordem_producao1"))


        df_joined = df_atributo_processo.join(df_microtempo, df_atributo_processo.id_microtempo == df_microtempo.id)


        df_data_inicio = df_joined.withColumn('datahorainicio_date', to_date(df_joined['datahorainicio']))
        df_joined = df_joined.withColumn(
            'hora_inicio', 
            when(col('datahorainicio').isNull(), 
                None) 
                .otherwise(format_string(
                    "%02d:%02d:%02d", 
                    hour(col('datahorainicio')), 
                    minute(col('datahorainicio')), 
                    second(col('datahorainicio'))
                ))
        )     
        
        
        df_data_fim = df_joined.withColumn('datahorafinal_date', to_date(df_joined['datahorafinal']))
        df_joined = df_joined.withColumn(
            'hora_fim', 
            when(col('datahorafinal').isNull(), 
                None) 
                .otherwise(format_string(
                    "%02d:%02d:%02d", 
                    hour(col('datahorafinal')), 
                    minute(col('datahorafinal')), 
                    second(col('datahorafinal'))
                ))
        )     

        df_hora_inicio = df_joined.select('datahorainicio', 'nr_ordem_producao')
        df_hora_fim = df_joined.select('datahorafinal', 'nr_ordem_producao')
         

        utils = Utils('Client2')
        sk_turno = utils.get_sk_dimension(df_joined, 'Dim_Turno', 'turno_codigo', 'codigo', 'sk_turno', 'nr_ordem_producao')
        sk_unidade_producao = utils.get_sk_dimension(df_joined, 'Dim_Unidade_Producao', 'codigo_unidade_producao', 'codigo', 'sk_unidade_producao', 'nr_ordem_producao')
        sk_data_inicio = utils.get_sk_dimension(df_data_inicio, 'Dim_Calendario', 'datahorainicio', 'DATA', 'sk_calendario', 'nr_ordem_producao')
        sk_data_inicio = sk_data_inicio.select(col('sk_calendario').alias('sk_calendario_inicio'), col('nr_ordem_producao'))

        sk_data_fim = utils.get_sk_dimension(df_data_fim, 'Dim_Calendario', 'datahorafinal', 'DATA', 'sk_calendario', 'nr_ordem_producao')
        sk_data_fim = sk_data_fim.select(col('sk_calendario').alias('sk_calendario_fim'), col('nr_ordem_producao'))
        
        sk_produto = utils.get_sk_dimension(df_joined, 'Dim_Produto', 'produto_codigo', 'codigo', 'sk_produto', 'nr_ordem_producao')
        sk_atributo = utils.get_sk_dimension(df_joined, 'Dim_Atributo', 'atributo_codigo', 'codigo', 'sk_atributos', 'nr_ordem_producao')
        sk_microtempo = utils.get_sk_dimension(df_joined, 'Dim_Microtempo', 'microtempo_nome', 'nome', 'sk_microtempo', 'nr_ordem_producao')
        sk_colaborador = utils.get_sk_dimension(df_joined, 'Dim_Colaborador', 'colaborador_codigo', 'codigo', 'sk_colaborador', 'nr_ordem_producao')       
        
        join_turno = sk_turno.select('sk_turno', 'nr_ordem_producao')
        join_unidade_producao = sk_unidade_producao.join(join_turno, 'nr_ordem_producao')
        join_data_fim = sk_data_fim.join(join_unidade_producao, 'nr_ordem_producao')
        join_data_inicio = sk_data_inicio.join(join_data_fim, 'nr_ordem_producao')
        join_hora_inicio = df_hora_inicio.join(join_data_inicio, 'nr_ordem_producao')
        join_produto = sk_produto.join(join_hora_inicio, 'nr_ordem_producao')
        join_atributo = sk_atributo.join(join_produto, 'nr_ordem_producao')
        join_microtempo = sk_microtempo.join(join_atributo, 'nr_ordem_producao')
        join_colaborador = sk_colaborador.join(join_microtempo, 'nr_ordem_producao')

        join_colaborador.limit(10).show()


    #     # Selecionar um subconjunto de colunas de df_joined
        
    #     df_subset = df_joined_colaborador.select(
    #                     "fk_unidade_producao", 
    #                     "fk_turno_producao", 
    #                     "fk_data_inicio", 
    #                     "fk_data_fim",
    #                     "fk_produto_producao",
    #                     "fk_atributos_producao",
    #                     "fk_microtempo_producao",
    #                     "fk_colaborador_producao",
    #                     "etiqueta_valor",                    
    #                     "nr_ordem_producao",
    #                     "hora_inicio",
    #                     "hora_fim",
    #                     "ciclo_nro",
    #                     "valor"
    #     ).withColumnRenamed("nr_ordem_producao", "nr_producao")

    #     # Aplicar deduplicação no subconjunto de colunas
    #     df_subset = df_subset.dropDuplicates()

    #     # Exibir o resultado
    #     df_subset.show()     
                        
    #     # Escrever o DataFrame no banco de dados
    #     df_subset.write \
    #         .format("jdbc") \
    #         .option("url", jdbc_url) \
    #         .option("dbtable", "Fato_Producao") \
    #         .option("user", connection_properties["user"]) \
    #         .option("password", connection_properties["password"]) \
    #         .option("driver", connection_properties["driver"]) \
    #         .mode("append") \
    #         .save()                   

    #     print("Total de valores ... :", df_subset.count())

    # query = df_stream_atributo_processo.writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .foreachBatch(process_batch) \
    #     .start()

    # query.awaitTermination()


    def etl(self):
        try:
            extracted_data_dict = self.extract()
        except:

            raise Exception("data extraction failed")

        try:
            transformed_data_dict = self.transform(*extracted_data_dict)
        except:
            raise Exception("data transformation failed")

        # try:
        #     self.load(transformed_data_dict)
        # except:
        #     raise Exception("data load failed")

        return {"ETL performed successfully"}

if __name__ == "__main__":
   producao_fato = Fato_Producao()
   fato_producao = producao_fato.etl()
