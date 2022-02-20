from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('quickstart-events', value=b"Message again")
producer.flush()