# testKafka project

Application reads settings from config file, then starts two threads: 
* one is to read new messages from configured kafka topic (consumer thread),*
* another is to write messages to the same topic (producer thread). 

Messages are written every second, but every 10-th message has broken JSON format. Consumer thread tries to parse the message as JSON, in case of broken JSON it starts additional logging thread which
* puts the message into logfile 
* sends it to configured error topic of the same kafka server.

## Dependencies
* python version 3.9 or later
* kafka-python PyPi package 

## Things to add
* push to Prometheus (logging thread)
* application daemonize (to be configured using systemd as a standard linux daemon, with start/stop/status commands support)

## How to use 
* set properties testKafka.ini
```ini
[DEFAULT]
kafka_url = localhost:9092
topic_main = topic-main
topic_err = topic-err
timeout = 10
```
* run main.py