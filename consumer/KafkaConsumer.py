import json
import os
import logging
from kafka import KafkaProducer,KafkaConsumer
from SQLClient import  SQLService

logging.getLogger().setLevel(logging.INFO)

consumer_client = KafkaConsumer(
    'wt-topic-0',
    group_id='wt-consumers',
    bootstrap_servers=["localhost:9092"],
    auto_offset_reset='earliest',  # Start from the beginning of the topic
    enable_auto_commit=False  # Disable auto-commit
)

produce_client= KafkaProducer(bootstrap_servers=["localhost:9092"], acks=1)

logging.info("strating processing...")
try:
    try:
        for msg in consumer_client:
            message= msg.value.decode('utf-8')
            json_obj=json.loads(message)
            print(json_obj)
            if SQLService().update_sql_tables(json_obj):
                logging.info("Order Id {} is successfully pushed".format(json_obj["order_id"]))
            else:
                response = produce_client.send("wt-dl-0", value=json.dumps(json_obj).encode('utf-8'))
                metadata = response.get(timeout=10)
                logging.info(f'Message delivered to topic:{metadata.topic} in Partition: {metadata.partition} with Offset: {metadata.offset}')

    except Exception as ex:
        logging.error(ex)
    finally:
        consumer_client.commit()

except Exception as ex:
    logging.error(ex)
finally:
    consumer_client.close()
    produce_client.close()

