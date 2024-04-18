import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import google.generativeai as genai
from extract.Sql import Database

os.environ['GOOGLE_API_KEY'] = "AIzaSyBkgl6vhJ9Oqs5PWUjfW7MTZPqu86rq930"
genai.configure(api_key=os.environ['GOOGLE_API_KEY'])
model = genai.GenerativeModel('gemini-pro')

client_name = 'UNIPAC_LIMEIRA'

spark = SparkSession.builder.appName("example").getOrCreate()

database = Database(client_name)

query = """SELECT TOP 25
cast(concat(concat(data_inicio.DATA, ' '), Fato_Producao.hora_inicio) as datetime2) AS data_inicio,
cast(concat(concat(data_fim.DATA, ' '), Fato_Producao.hora_fim) as datetime2) AS data_fim,
Fato_Producao.valor,
Fato_Producao.ciclo_nro,
Fato_Producao.nr_producao,
Dim_Colaborador_teste.nome as colaborador_teste_nome,
Dim_Microtempo.nome_operacao,
Dim_Microtempo.nome as nome_microtempo,
Dim_Unidade_Producao_teste.nome,
Dim_Unidade_Producao_teste.codigo as unidade_producao_codigo,
Dim_Turno.codigo as turno_codigo,
Dim_Turno.descricao,
Dim_Turno.jornada,
Dim_Turno.hora_inicio,
Dim_Turno.hora_fim,
Dim_Atributo.codigo as atributo_codigo,
Dim_Atributo.classe,
Dim_Atributo.nome as nome_atributo,
Dim_Atributo.tipo_atributo,
Dim_Materia_Prima.codigo as materia_prima_codigo,
Dim_Materia_Prima.nome as nome_materia_prima

FROM Fato_Producao
LEFT OUTER JOIN Dim_Calendario AS data_inicio ON (data_inicio.sk_calendario = fk_data_inicio)
LEFT OUTER JOIN Dim_Calendario AS data_fim ON (data_fim.sk_calendario = fk_data_fim)
LEFT OUTER JOIN Dim_Colaborador_teste ON (Dim_Colaborador_teste.sk_colaborador = Fato_Producao.fk_colaborador_producao)
LEFT OUTER JOIN Dim_Microtempo ON (Dim_Microtempo.sk_microtempo = Fato_Producao.fk_microtempo_producao)
LEFT OUTER JOIN Dim_Unidade_Producao_teste ON (Dim_Unidade_Producao_teste.sk_unidade_producao = Fato_Producao.fk_unidade_producao)
LEFT OUTER JOIN Dim_Turno ON (Dim_Turno.sk_turno = Fato_Producao.fk_turno_producao)
LEFT OUTER JOIN Dim_Atributo ON (Dim_Atributo.sk_atributos = Fato_Producao.fk_atributos_producao)
LEFT OUTER JOIN Dim_Materia_Prima ON (Dim_Materia_Prima.sk_materia_prima = Fato_Producao.fk_materia_prima_producao)
where data_inicio.DATA is not null
and data_inicio.DATA between '2023-08-01' and '2023-08-02'"""

result = database.select_data(query)
json_data = result.toJSON().collect()

formatted_data = "[" + ",".join(json_data) + "]"
print('************************')
print(formatted_data)
print('************************')


user_question = f"descreva sobre os dados e quais analises sao possiveis fazer: {formatted_data}"

response = model.generate_content(user_question)
print('************************')
print(response.text)
print('************************')

