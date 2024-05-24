from confluent_kafka import Consumer
from datetime import datetime, date
import json
import pandas as pd
import numpy as np
import psycopg2
import io
import os


# Consumes messages containing stop event data from Kafka on Confluence Cloud
# The consumer.py file in the code sample provided by Confluence Cloud was used as a template for this work:
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
topic = ['stop_events']
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


# Initialize data to use for stop event data validations and transformations before loading into database
today = date.today()
trip_ids = []
log_file_path = F"/home/werner/log_files/stop_event_consumer_logs-{today.strftime('%Y-%m-%d')}.txt"
load_to_database = True
last_message_series = None
verify_batch_count = False
batch_day = today.weekday()
daily_batch_count = [0, 0, 0, 0, 0, 0, 0]
last_week_batch_count = [0, 0, 0, 0, 0, 0, 0]
same_daily_batch = True
merge_trip_tables = False
database_entries_waiting = False
merge_tables_counter = 0


# Receive messages with individual stop events from Kafka
try:
  while True:
    message = consumer.poll(1.0)

    if message is None:
      print('Waiting for message...')

      if database_entries_waiting:
        merge_tables_counter += 1

      if merge_tables_counter >= 10:
        merge_tables_counter = 0
        merge_trip_tables = True

    elif message.error():
      print(F"ERROR: {message.error()}")

    else:
      print('.', end='')

      message_json = json.loads(message.value())
      message_str = json.dumps(message_json)

      # Initializing new daily values when messages are received on a new day
      current_day = date.today()
      if today != current_day:
        today = date.today()
        trip_ids = []
        log_file_path = F"/home/werner/log_files/stop_event_consumer_logs-{today.strftime('%Y-%m-%d')}.txt"
        same_daily_batch = False

        batch_day = today.weekday()
        if batch_day == 0:
          verify_batch_count = True
          last_week_batch_count = daily_batch_count
          daily_batch_count = [0, 0, 0, 0, 0, 0, 0]
      else:
        same_daily_batch = True

      daily_batch_count[batch_day] += 1

      # Initializing log output for this message
      message_log = ['Warnings logged for the following message:', message_str]

      # Stop event data validation before storing in database
      message_series = pd.Series(data=message_json)
      message_series = message_series.replace(r'^\s*$', np.NaN, regex=True)
      load_to_database = True

      # Every trip has a unique ID
      if not message_series['trip_id'] in trip_ids:
        # Each stop event must have the trip_id field populated with a non-negative numerical value
        if message_series.isna()['trip_id'] or (not message_series.str.isnumeric()['trip_id']) or pd.to_numeric(message_series['trip_id']) < 0:
          log_message = 'trip_id field is not populated with a non-negative numerical value'
          message_log.append(log_message)
          load_to_database = False
        else:
          trip_ids.append(message_series['trip_id'])
          print(F"{len(trip_ids)}")

        # Each stop event must have the vehicle_id field populated with a non-negative numerical value
        if message_series.isna()['vehicle_number'] or (not message_series.str.isnumeric()['vehicle_number']) or pd.to_numeric(message_series['vehicle_number']) < 0:
          log_message = 'vehicle_number field is not populated with a non-negative numerical value'
          message_log.append(log_message)

        # Each stop event must have the route_id field populated with a non-negative numerical value
        if message_series.isna()['route_number'] or (not message_series.str.isnumeric()['route_number']) or pd.to_numeric(message_series['route_number']) < 0:
          log_message = 'route_number field is not populated with a non-negative numerical value'
          message_log.append(log_message)

        # The direction of the vehicle captured in each stop event must always be in either 0 or 1
        directions = [0, 1]
        if message_series.isna()['direction'] or (not message_series.str.isnumeric()['direction']) or pd.to_numeric(message_series['direction']) not in directions:
          log_message = 'direction field is not populated with a 0 or 1'
          message_log.append(log_message)

        # When the event_date field is a weekday, Saturday, or Sunday, the service_key field should be set to “W”, “S”, and “U” respectively
        day_of_week = datetime.strptime(message_series['event_date'], '%Y-%m-%d').weekday()
        if day_of_week == 5 and message_series['service_key'] != 'S':
          log_message = 'event_date is a Saturday, but service_key field does not store an S'
          message_log.append(log_message)
        elif day_of_week == 6 and message_series['service_key'] != 'U':
          log_message = 'event_date is a Sunday, but service_key field does not store a U'
          message_log.append(log_message)
        elif day_of_week < 5 and message_series['service_key'] != 'W':
          log_message = 'event_date is a weekday, but service_key field does not store a W'
          message_log.append(log_message)

      else:
        load_to_database = False

        # Each stop event with the same trip_id should also have the same vehicle_number
        if message_series['vehicle_number'] != last_message_series['vehicle_number']:
          if not (message_series.isna()['vehicle_number'] and last_message_series.isna()['vehicle_number']):
            log_message = 'vehicle_numbers do not match for stop events with the same trip_id'
            message_log.append(log_message)

        # Each stop event with the same trip_id should also have the same route_number
        if message_series['route_number'] != last_message_series['route_number']:
          if not (message_series.isna()['route_number'] and last_message_series.isna()['route_number']):
            log_message = 'route_numbers do not match for stop events with the same trip_id'
            message_log.append(log_message)

        # Each stop event with the same trip_id should also have the same direction
        if message_series['direction'] != last_message_series['direction']:
          if not (message_series.isna()['direction'] and last_message_series.isna()['direction']):
            log_message = 'directions do not match for stop events with the same trip_id'
            message_log.append(log_message)

        # Each stop event with the same trip_id should also have the same service_key
        if message_series['service_key'] != last_message_series['service_key']:
          if not (message_series.isna()['service_key'] and last_message_series.isna()['service_key']):
            log_message = 'service_keys do not match for stop events with the same trip_id'
            message_log.append(log_message)

      # All stop events collected in the same daily batch should have the same event_date
      if last_message_series is not None and same_daily_batch:
        if message_series['event_date'] != last_message_series['event_date']:
          log_message = 'event_date field does not match the event_date field of other stop events from this daily batch'
          message_log.append(log_message)

      # For each week, the number of stop events collected on each weekday should be greater than the amount collected on each weekend day
      if verify_batch_count:
        verify_batch_count = False
        count_error = False
        dates = 5

        for day in range(dates):
          daily_count = last_week_batch_count[day]
          if daily_count != 0 and (daily_count < last_week_batch_count[5] or daily_count < last_week_batch_count[6]):
            count_error = True
        if count_error:
          log_message = F"More weekend readings counted than weekday readings last week:\n"
          "Mon: {last_week_batch_count[0]}, Tue: {last_week_batch_count[1]}, Wed: {last_week_batch_count[2]}, "
          "Thu: {last_week_batch_count[3]}, Fri: {last_week_batch_count[4]}, Sat: {last_week_batch_count[5]}, "
          "Sun: {last_week_batch_count[6]}"
          message_log.append(log_message)

      # Store state of last retrieved message
      last_message_series = message_series

      # If any validation warnings were logged, they are output to the daily log file
      if len(message_log) > 2:
        with open(log_file_path, 'a') as outfile:
          outfile.write('\n'.join(message_log))
          outfile.write('\n')
          outfile.write('\n')

      # If stop event data passes validations, it can be transformed and stored in database
      if load_to_database:
        database_entries_waiting = True

        # Transform and load appropriate data from message_series into loadtrip table
        with db_connection.cursor() as cursor:
          serv_key = '\\N'

          if message_series['service_key'] == 'W':
            serv_key = 'Weekday'
          elif message_series['service_key'] == 'S':
            serv_key = 'Saturday'
          elif message_series['service_key'] == 'U':
            serv_key = 'Sunday'

          dir_key = '\\N'

          if pd.to_numeric(message_series['direction']) == 0:
            dir_key = 'Out'
          elif pd.to_numeric(message_series['direction']) == 1:
            dir_key = 'Back'

          message_series = message_series.fillna('\\N')

          trip_id = message_series['trip_id']
          route_id = message_series['route_number']
          vehicle_id = message_series['vehicle_number']
          service_key = serv_key
          direction = dir_key
          db_row = io.StringIO(F"{trip_id}\t{route_id}\t{vehicle_id}\t{service_key}\t{direction}\n")
          cursor.copy_from(db_row, 'loadtrip')

    # When a daily batch of messages has been received and loaded to loadtrip table,
    #   then entries in the loadtrip table are merged with entries in the trip table
    # After merging tables, clear today's batch of entries from the loadtrip table
    if merge_trip_tables:
      merge_trip_tables = False
      database_entries_waiting = False

      print('Merging new entries from loadtrip table into trip table...')

      with db_connection.cursor() as cursor:
        cursor.execute("""
          UPDATE trip
          SET route_id = lt.route_id, service_key = lt.service_key, direction = lt.direction
          FROM loadtrip AS lt
          WHERE trip.trip_id = lt.trip_id;
          DELETE FROM loadtrip;
        """)

      print('Finished merging tables')

except KeyboardInterrupt:
  pass
finally:
  consumer.close()

