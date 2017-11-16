#!/usr/bin/python
from utils.couchbase_storer import CouchbaseStorer
import json
import os
import time

def write_to_cb():
  """
  Loads information in the file `data`
  into a local Couchbase instance
  """

  CB_URL = os.environ['COUCHBASE_BASE_URL']
  PODCASTS_BUCKET = os.environ['PODCASTS_BUCKET']

  # URL for Couchbase (assuming no password
  # for local dev env)
  couchbase_url = 'couchbase://{}/{}'.format(CB_URL, PODCASTS_BUCKET)
  storer = CouchbaseStorer(couchbase_url)

  # Load in jsons' file names
  json_files = []
  for _, _, filenames in os.walk('./data'):
    json_files.extend(filenames)
  json_files = [f for f in json_files if '.json' in f]

  # Load in JSONS
  JSONS = []
  for j_file in json_files:
    with open('./data/' + j_file) as data_file:
      JSONS.append(json.load(data_file))

  # Load each into Couchbase
  for j in JSONS:
    print 'Storing {} in Couchbase'.format(str(j['series']['id']))
    storer.store(j)
    time.sleep(0.5)

if __name__ == '__main__':
  write_to_cb()
