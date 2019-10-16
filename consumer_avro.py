import io
import base64
import avro.io
from kafka import KafkaConsumer


schema_path = "users.avsc"
schema = avro.schema.Parse(open(schema_path).read())


def from_avro(msg):
    msg_decoded=base64.b64decode(msg.decode('ascii'))
    bytes_reader = io.BytesIO(msg_decoded)
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema, schema)

    user1 = reader.read(decoder)
    return user1


consumer = KafkaConsumer('topic_name_here',
                         bootstrap_servers=['0.serveraddress.com:9092',
                                            '1.serveraddress.com:9092',
                                            '2.serveraddress.com:9092'],
                         auto_offset_reset='earliest',
                         group_id='my_group',
                         value_deserializer=from_avro
                         )


for msg in consumer:
    print(msg)
