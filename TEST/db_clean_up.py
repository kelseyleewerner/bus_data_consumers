from confluent_kafka import Consumer
from datetime import datetime, date, timedelta
import json
import pandas as pd
import numpy as np
import re
import psycopg2
import io
import os


# Consumes messages containing sensor reading data from Kafka on Confluence Cloud
# The consumer.py file in the  code sample provided by Confluence Cloud was used as a template for this work:
#   https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/python.html#avro-and-confluent-cloud-schema-registry


# Instantiate Consumer and subscribe to topic
consumer_config = {
  'bootstrap.servers' : os.environ['BOOTSTRAP_SERVERS'],
  'security.protocol' : os.environ['SECURITY_PROTOCOL'],
  'sasl.mechanisms' : os.environ['SASL_MECHANISM'],
  'sasl.username' : os.environ['SASL_USERNAME'],
  'sasl.password' : os.environ['SASL_PASSWORD']
}
consumer_config['group.id'] = 'group_0'
consumer_config['auto.offset.reset'] = 'earliest'
consumer = Consumer(consumer_config)
topic = ['breadcrumb_readings']
consumer.subscribe(topic)


# Establish connection to Postgres database
db_config = {
  'host' : os.environ['DB_HOST'],
  'database' : os.environ['DB_NAME'],
  'user' : os.environ['DB_USER'],
  'password' : os.environ['DB_PASSWORD']
}
db_connection = psycopg2.connect(
  host = db_config['host'],
  database = db_config['database'],
  user = db_config['user'],
  password = db_config['password']
)
db_connection.autocommit = True


# Initialize data to use for breadcrumb data validations and transformations before loading into database
trip_ids = []
ready_for_database = True

# Receive messages with individual sensor readings from Kafka
try:
  while True:
    message = consumer.poll(1.0)

    if message is None:
      print('Waiting for message...')
      continue
    elif message.error():
      print(F"ERROR: {message.error()}")
    else:
      print('.', end='')

      message_json = json.loads(message.value())

      # Breadcrumb data validation before storing in database
      message_series = pd.Series(data=message_json)

      # If breadcrumb data passes validations, it can be transformed and stored in database
      if ready_for_database:
        # Every trip has a unique ID
        if not message_series['EVENT_NO_TRIP'] in trip_ids:
          trip_ids.append(message_series['EVENT_NO_TRIP'])

          print(F"{len(trip_ids)} ")

          with db_connection.cursor() as cursor:
            cursor.execute(F"""
              DELETE FROM breadcrumb
              WHERE trip_id={message_series['EVENT_NO_TRIP']};
            """)

          # Transform and load appropriate data from message_series into Trip table in database
          with db_connection.cursor() as cursor:
            cursor.execute(F"""
              DELETE FROM trip
              WHERE trip_id={message_series['EVENT_NO_TRIP']};
            """)

except KeyboardInterrupt:
  pass
finally:
  consumer.close()

