from time import sleep

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

value_schema = avro.load('ValueSchema.avsc')
key_schema = avro.load('KeySchema.avsc')
key = {"id": 1}
value = {"id": "1", "name": "aaa"}

avroProducer = AvroProducer(
    {'bootstrap.servers': '0.dual.kafka.qa-fxenv.com:9092', 'schema.registry.url': 'https://avro-schemaregistry.rnd-env.com'},
    default_key_schema=key_schema, default_value_schema=value_schema)

for i in range(0, 10):
    value = {"name": "Yuva", "id": i}
    avroProducer.produce(topic='test_kafka_alp', value=value, key=key, key_schema=key_schema, value_schema=value_schema)
    sleep(0.01)
    print(i)
