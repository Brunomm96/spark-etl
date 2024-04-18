from confluent_kafka import Consumer, KafkaError
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

# Configurações do consumidor Kafka
kafka_conf = {
    'bootstrap.servers': '10.130.0.164:9092',  # substitua pelo endereço do seu servidor Kafka
    'group.id': 'Sarah_Testes',
    'auto.offset.reset': 'latest'
}

# Tópico a ser consumido
topic = 'Sarah_Testes'

# Configurações do Spark
spark = SparkSession.builder.appName("KafkaConsumer").getOrCreate()

# Função para criar um DataFrame a partir de mensagens Kafka
def process_message(value):
    return (value, )

# Crie o consumidor Kafka
consumer = Consumer(kafka_conf)
consumer.subscribe([topic])

try:
    while True:
        msg = consumer.poll(1.0)  # Aguarde até 1 segundo por mensagens

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Processar a mensagem
        value = msg.value().decode('utf-8')
        print('Nova mensagem: {}'.format(value))

        # Criar DataFrame PySpark
        data = [process_message(value)]
        schema = ["value"]
        df = spark.createDataFrame(data, schema=schema)

        # Exibir DataFrame
        df.show()

except KeyboardInterrupt:
    pass

finally:
    # Feche o consumidor e a sessão do Spark
    consumer.close()
    spark.stop()
