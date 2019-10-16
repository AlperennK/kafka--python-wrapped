from json import loads, dumps
from kafka import KafkaConsumer


consumer = KafkaConsumer('topic_name_here',
     bootstrap_servers=['0.serveraddress.com:9092'],
     auto_offset_reset='latest',
     enable_auto_commit=False,
     value_deserializer=lambda x:loads(x.decode('utf-8')))



for message in consumer:
    print("Message ", message.value)
