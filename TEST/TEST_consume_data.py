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
today = date.today()
trip_ids = []
#         TODO: will need to change log file path in linux to /home/werner/log_files
log_file_path = F"/home/werner/TEST/log_files/consumer_logs-{today.strftime('%Y-%m-%d')}.txt"
last_message_series = None
same_daily_batch = True
daily_batch_count = [0, 0, 0, 0, 0, 0, 0]
last_week_batch_count = [0, 0, 0, 0, 0, 0, 0]
batch_day = today.weekday()
verify_batch_count = False


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
      message_str = json.dumps(message_json)


      file_name = datetime.today().strftime('%Y-%m-%d')
      with open(F"/home/werner/TEST/test_data/sensor_readings_{file_name}.txt", 'a') as outfile:
        outfile.write(F"{message_str}\n")


      """

      # Initializing new daily values when messages are received on a new day
      current_day = date.today()
      if today != current_day:
        today = date.today()
        trip_ids = []
#         TODO: will need to change log file path in linux to home/werner/log_files
        log_file_path = F"/home/werner/TEST/log_files/consumer_logs-{today.strftime('%Y-%m-%d')}.txt"
        same_daily_batch = False

        batch_day = today.weekday()
        if batch_day == 0:
            last_week_batch_count = daily_batch_count
            daily_batch_count = [0, 0, 0, 0, 0, 0, 0]
            verify_batch_count = True
      else:
        same_daily_batch = True

      # Initializing log output for this message
      message_log = ['Warnings logged for the following message:', message_str]

      # Breadcrumb data validation before storing in database
      message_series = pd.Series(data=message_json)
      message_series = message_series.replace(r'^\s*$', np.NaN, regex=True)
      ready_for_database = True

      # Each sensor reading must have the EVENT_NO_TRIP, EVENT_NO_STOP, and VEHICLE_ID fields populated with a numerical value
      if message_series.isna()['EVENT_NO_TRIP'] or not message_series.str.isnumeric()['EVENT_NO_TRIP']:
        log_message = 'EVENT_NO_TRIP field is not populated with a numerical value'
        message_log.append(log_message)
        ready_for_database = False

      if message_series.isna()['EVENT_NO_STOP'] or not message_series.str.isnumeric()['EVENT_NO_STOP']:
        log_message = 'EVENT_NO_STOP field is not populated with a numerical value'
        message_log.append(log_message)

      if message_series.isna()['VEHICLE_ID'] or not message_series.str.isnumeric()['VEHICLE_ID']:
        log_message = 'VEHICLE_ID field is not populated with a numerical value'
        message_log.append(log_message)
        ready_for_database = False

      # The direction of the vehicle captured in each sensor reading must always be in the range 0 to 359, inclusive
      direction_int = pd.to_numeric(message_series['DIRECTION'])
      if message_series.isna()['DIRECTION'] or direction_int < 0 or direction_int > 359:
        log_message = 'DIRECTION field value is outside of allowed bounds between 0 and 359'
        message_log.append(log_message)
        message_series['DIRECTION'] = np.NaN

      # The GPS horizontal dilution of precision should reflect an accuracy level that makes the data suitable for use (ideally GPS_HDOP should be between 0 and 5)
      hdop_num = pd.to_numeric(message_series['GPS_HDOP'])
      if message_series.isna()['GPS_HDOP'] or hdop_num < 0 or hdop_num > 5:
        log_message = 'GPS_HDOP field value is outside of suitable bounds between 0 and 5'
        message_log.append(log_message)

      # The vehicle velocity captured in each sensor reading should never be a negative integer
      velocity_int = pd.to_numeric(message_series['VELOCITY'])
      if message_series.isna()['VELOCITY'] or velocity_int < 0:
        log_message = 'VELOCITY field is not a positive integer or 0'
        message_log.append(log_message)
        message_series['VELOCITY'] = np.NaN

      # If a longitude value is recorded for a sensor reading, then a latitude value must be recorded as well
      # Conversely, if a latitude value is recorded for a sensor reading, then a longitude value must be recorded as well
      if not message_series.isna()['GPS_LONGITUDE'] and message_series.isna()['GPS_LATITUDE']:
        log_message = 'GPS_LONGITUDE field is populated, but GPS_LATITUDE field is not populated'
        message_log.append(log_message)

      if not message_series.isna()['GPS_LATITUDE'] and message_series.isna()['GPS_LONGITUDE']:
        log_message = 'GPS_LATITUDE field is populated, but GPS_LONGITUDE field is not populated'
        message_log.append(log_message)

      # All sensor readings should have a correctly formatted operation date (ex: 07-SEP-20)
      if re.fullmatch('\d{2}-\w{3}-\d{2}', message_series['OPD_DATE']) is None:
        log_message = 'OPD_DATE field is not correctly formatted'
        message_log.append(log_message)
        ready_for_database = False

      # Consecutive sensor readings collected for the same trip should have an ACT_TIME that is larger than the preceding reading
      if last_message_series is not None and message_series['EVENT_NO_TRIP'] == last_message_series['EVENT_NO_TRIP']:
        if pd.to_numeric(message_series['ACT_TIME']) <= pd.to_numeric(last_message_series['ACT_TIME']):
          log_message = 'ACT_TIME field does not have a value greater than the last ACT_TIME reading for this trip'
          message_log.append(log_message)

        # Each consecutive sensor reading recorded for the same trip must have a METERS odometer value greater than or equal to the preceding sensor reading
        if pd.to_numeric(message_series['METERS']) < pd.to_numeric(last_message_series['METERS']):
          log_message = 'METERS field does not have a value greater than or equal to the last METERS reading for this trip'
          message_log.append(log_message)

      # All sensor readings recorded in the same daily batch should have the same operation date
      if last_message_series is not None and same_daily_batch:
        if message_series['OPD_DATE'] != last_message_series['OPD_DATE']:
          log_message = 'OPD_DATE field does not match the OPD_DATE field of other readings from this daily batch'
          message_log.append(log_message)

      # For each week, the number of sensor readings collected on each weekday should be greater than the amount collected on each weekend day
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

      # If breadcrumb data passes validations, it can be transformed and stored in database
      if ready_for_database:
        # Every trip has a unique ID
        if not message_series['EVENT_NO_TRIP'] in trip_ids:
          trip_ids.append(message_series['EVENT_NO_TRIP'])

          print(F"{len(trip_ids)} ")

          # Transform and load appropriate data from message_series into Trip table in database
          with db_connection.cursor() as cursor:
            trip_id = message_series['EVENT_NO_TRIP']
            route_id = '\\N'
            vehicle_id = message_series['VEHICLE_ID']
            service_key = '\\N'
            direction = '\\N'
            db_row = io.StringIO(F"{trip_id}\t{route_id}\t{vehicle_id}\t{service_key}\t{direction}\n")
            cursor.copy_from(db_row, 'trip')

        # Transform and load appropriate data from message_series into Breadcrumb table in database
        with db_connection.cursor() as cursor:
          if not message_series.isna()['VELOCITY']:
            message_series['VELOCITY'] = pd.to_numeric(message_series['VELOCITY']) * 2.2369

          time_stamp = datetime.strptime(message_series['OPD_DATE'], '%d-%b-%y')
          reading_time = int(message_series['ACT_TIME'])
          time_stamp += timedelta(seconds=reading_time)

          message_series = message_series.fillna('\\N')

          tstamp = time_stamp
          latitude = message_series['GPS_LATITUDE']
          longitude = message_series['GPS_LONGITUDE']
          direction = message_series['DIRECTION']
          speed = message_series['VELOCITY']
          trip_id = message_series['EVENT_NO_TRIP']
          db_row = io.StringIO(F"{tstamp}\t{latitude}\t{longitude}\t{direction}\t{speed}\t{trip_id}\n")
          cursor.copy_from(db_row, 'breadcrumb')
    """
  # Append each received message to the sensor_readings.txt file



except KeyboardInterrupt:
  pass
finally:
  consumer.close()

