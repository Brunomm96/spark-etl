# Use a imagem base Python
FROM apache/spark-py

USER root

# Define o diretório de trabalho no container
WORKDIR /opt/spark/work-dir

RUN python3 -m pip install --upgrade pip
# Copie o arquivo de requisitos e instale as dependênciasx
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN apt-get update && \
    apt-get install -y curl && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/9/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

RUN pip3 install --user pyodbc


# Adicione o diretório do projeto ao PYTHONPATH
ENV PYTHONPATH=/opt/spark/work-dir
 
# Copie o arquivo .jar pra pasta de drivers do docker
COPY drivers/mssql.jar /opt/spark/jars
 
# Exponha a porta 5000
EXPOSE 4040

# Comando para iniciar a aplicação Flask usando Gunicorn
# CMD ["/opt/spark/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1","clients/unipac_limeira/teste.py"]
# CMD ["/opt/spark/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1","clients/unipac_limeira/Atributo_Valor/Atributo_Valor_Datadriven.py"]
# CMD ["/opt/spark/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1","clients/unipac_limeira/dim_colaborador.py"]
# CMD ["/opt/spark/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1", "extract/Sql.py"]
# CMD ["/opt/spark/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1", "Update.py"]
CMD ["/opt/spark/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1","clients/unipac_limeira/fato_producao.py"]

