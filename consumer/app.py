import sys
import json

from datetime import timedelta, datetime
from time import sleep
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient

from mailing import send_order_ack_email

KAFKA_ADDRESS = 'localhost:29092'


next_email_topic = {
    'emails': 'emails_retry_5m',
    'emails_retry_5m': 'emails_retry_30m',
    'emails_retry_30m': 'emails_failed'
}

next_email_offset = {
    'emails': timedelta(minutes=5),
    'emails_retry_5m': timedelta(minutes=30),
    'emails_retry_30m': None
}


def get_sleep_time(future):
    now = datetime.now()
    if now > future:
        return 0
    return (future - now).seconds


def main():
    current_topic = sys.argv[1]
    next_topic = next_email_topic[current_topic]
    mongo = MongoClient("mongodb://localhost:27017/")
    mongo_col = mongo["kafka_test"]["emails"]
    producer = KafkaProducer(bootstrap_servers=KAFKA_ADDRESS,
                             value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'))
    consumer = KafkaConsumer(current_topic, bootstrap_servers=KAFKA_ADDRESS,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    for consumer_record in consumer:
        message = consumer_record.value
        message['send_offset'] = datetime.fromisoformat(message['send_offset'])
        sleep(get_sleep_time(message['send_offset']))
        if not send_order_ack_email(message['email'], 'Order acknowledgement', message['products']):
            next_offset = datetime.now() + next_email_offset[current_topic]
            message['send_offset'] = next_offset
            producer.send(next_topic, value=message)
        else:
            message['sent'] = datetime.now().isoformat()
            mongo_col.insert_one(message)
    producer.close()


if __name__ == '__main__':
    main()
