#!/usr/bin/python3
import csv, json
from geojson import Feature, FeatureCollection, Point


# Simple Python script for converting a TSV file with longitude, latitude, and speed data to GeoJSON format
#   This script is copied with few modifications from an example


features = []

with open('map_data.tsv', newline='') as csvfile:
  reader = csv.reader(csvfile, delimiter='\t')
  data = csvfile.readlines()

  for line in data[1:len(data)-1]:
    line = line.strip()
    row = line.split('\t')

    # Skip rows where speed is missing
    print(row)
    if len(row) < 3:
      continue

    raw_longitude = row[0]
    raw_latitude = row[1]
    speed = row[2]

    try:
      latitude, longitude = map(float, (raw_latitude, raw_longitude))
      features.append(
        Feature(
          geometry = Point((longitude,latitude)),
          properties = { 'speed': (float(speed)) }
        )
      )
    except ValueError:
      print('ValueError detected')
      continue


collection = FeatureCollection(features)

with open('map_data.geojson', 'w') as outfile:
  outfile.write('%s' % collection)

