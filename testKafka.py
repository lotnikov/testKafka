import threading, time, logging, json, configparser
from kafka import KafkaProducer, KafkaConsumer

# producer thread
def do_send():
    logging.info("Producer thread started")
    producer = KafkaProducer(bootstrap_servers=[kafka_url])
    counter = 0

    while True:
        counter += 1

        # create message
        msg = {}
        msg["n"] = counter
        msg["message"] = "Message "+str(counter)
        msg_text = json.dumps(msg, indent=4)

        # break json for every 10-th message
        if counter % timeout == 0:
            msg_text = msg_text + "{"

        producer.send(topic_main, value=bytes(msg_text,'ascii'))
        producer.flush()
        time.sleep(1) # one second delay

# consumer thread
def do_receive():
    consumer = KafkaConsumer(topic_main, bootstrap_servers=[kafka_url])
    logging.info("Consumer thread started")
    for mess in consumer:
        try:
            message = json.loads(mess.value)
            logging.info(mess)
        except ValueError:

            # collect error message info
            err = "topic: " + str(mess.topic) + \
                  " partition: " + str(mess.partition) + \
                  " offset: " + str(mess.offset) + \
                  " key: " + str(mess.key) + \
                  " value: " + str(mess.value)

            errthread = threading.Thread(target=do_log_error, name='thread_log_error', args=(err,))
            errthread.start()

#logging thread
def do_log_error(err):

    # write to logs
    logging.error(err)

    # send to error topic
    producer_err = KafkaProducer(bootstrap_servers=[kafka_url])
    producer_err.send(topic_err, value=bytes(err, 'ascii'))
    producer_err.flush()

    # send to prometheus
    # ...

if __name__ == '__main__':
    logging.basicConfig(filename="/home/kafka/tz.log", level=logging.INFO)

    #read config
    config = configparser.ConfigParser()
    config.read('./testKafka.ini')
    topic_main = config['DEFAULT']['topic_main']
    topic_err = config['DEFAULT']['topic_err']
    timeout = int(config['DEFAULT']['timeout'])
    kafka_url = config['DEFAULT']['kafka_url']

    print("Staring with:")
    print("Kafka URL: ", kafka_url)
    print("Broken message period (seconds): ", timeout)
    print("Main Kafka topic", topic_main)
    print("Kafka topic for broken messages", topic_err)

    #start two threads (send, receive)
    t_receive = threading.Thread(target=do_receive, name='thread_receive', args=())
    t_send = threading.Thread(target=do_send, name='thread_send', args=())
    t_receive.start()
    t_send.start()
