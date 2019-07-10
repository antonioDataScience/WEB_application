from kafka import KafkaProducer, KafkaConsumer
from json import dumps
import json

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda x: dumps(x).encode('utf-8'))
consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'], client_id = 'streaming_data_backward client',value_deserializer = lambda m: json.loads(m.decode('ascii')))