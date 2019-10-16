from kafka import KafkaProducer
import json
import datetime


def send_event(server, topic):
    producer = KafkaProducer(bootstrap_servers=['0.serveraddress.com:9092'], value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    date = datetime.datetime.now().isoformat()
    data = {
        "body"

    }

    headers = [
        ("key", "value"),
        ("key", "value"),
        ("created", date),
    ]

    producer.send(topic, headers=headers, value=data)
    print("event produced")
    producer.flush()


if __name__ == "__main__":
    send_event('broker_url',
               "topic_name")
