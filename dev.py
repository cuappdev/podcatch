#!/usr/bin/python

from podcasts.storers.couchbase_storer import CouchbaseStorer
import json
import os
import time

def write_to_cb():
  """
  Loads information in the file `data`
  into a local Couchbase instance
  """

  # URL for Couchbase (assuming no password
  # for local dev env)
  couchbase_url = 'couchbase://localhost:8091/podcasts'
  storer = CouchbaseStorer(couchbase_url)

  # Load in jsons' file names
  json_files = []
  for _, _, filenames in os.walk('./data'):
    json_files.extend(filenames)

  # Load in JSONS
  JSONS = []
  for j_file in json_files:
    with open('./data/' + j_file) as data_file:
      JSONS.append(json.load(data_file))

  # Load each into Couchbase
  for j in JSONS:
    print 'Storing ' + str(j['series']['id']) + \
      ' in Couchbase'
    storer.store(j)
    time.sleep(0.5)


# Run the task
write_to_cb()
