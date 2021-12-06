import logging
import ssl
import time
import json
import sys
import datetime
"""logging.basicConfig(level=logging.DEBUG)"""

from kafka import KafkaConsumer
from kafka import TopicPartition

def consumer_func():
    """
    This method will connect to the Kafka broker, consume messages from the Topic, parse the messages, and write to 22 CSVs.

    """

    print("Connected to Kafka Consumer at %s: " % datetime.datetime.now())
    #Create a Kafka Consumer object, which reads all the messages in the queue, one at a time
    try:
        topic = 'cbm.edw.cloud.data'
        consumer = KafkaConsumer(bootstrap_servers='messaging-rtp3.cisco.com:9093',
                                 security_protocol='SSL',
                                 ssl_check_hostname=True,
                                 auto_offset_reset='earliest',
                                 ssl_certfile='cbmedwdc.cer.pem',
                                 ssl_keyfile='cbmedwdc.key.pem',
                                 ssl_password = 'password',
                                 enable_auto_commit=False
                                 #value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                                 )

        """consumer = KafkaConsumer(bootstrap_servers='messaging-rtp3.cisco.com:9093', auto_offset_reset='earliest',
                                 group_id='cbmedwco',enable_auto_commit=False,
                              value_deserializer=lambda m: json.loads(m.decode('utf-8'))) """
    except Exception as e:
        print("Failed to Connect to Kafka Broker")
        print(str(e))
        sys.exit(1)

    consumer.subscribe(['cbm.edw.cloud.data'])
    '''consumer.assign([TopicPartition('cbm.edw.cloud.data', 1)])'''
    '''consumer.poll(timeout_ms=1000, max_records=1)'''
    consumer.poll(0.1)

    for message in consumer:
        print('Received message: {0}'.format(message.value))



    count=0
    print("Started parsing at %s: " % datetime.datetime.now())
    print(consumer)
    print
    for message in consumer:
        try:
            '''temp_message = message'''
            print(message.value)

            if (count == 10):
                break

        except Exception as e:
            pass

        finally:
            pass

    consumer.close(False)
    print("Closed Kafka Consumer at %s: " % datetime.datetime.now())


if __name__ == "__main__":

    start_time = time.time()
    print ("Started script at %s: " % datetime.datetime.now())
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    consumer_func()

    print("Finished script at %s: " % datetime.datetime.now())
    print("--- %s seconds ---" % (time.time() - start_time))
