# OVERALL IMPORTS
from flask import Flask, request
from threading import Thread
import schedule
import time
import os

# PODCAST IMPORTS
from podcasts.series_driver import SeriesDriver
from podcasts.episodes_driver import EpisodesDriver
from podcasts.site_crawler import SiteCrawler
from utils.couchbase_storer import CouchbaseStorer

# Constants
LOG_FILE           = 'log'
BASE_DIR           = 'csv'
COUCHBASE_URL      = os.environ['COUCHBASE_URL']
COUCHBASE_PASSWORD = os.environ['COUCHBASE_PASSWORD']
ONE_DAY            = 86400

# Flask App
app = Flask(__name__)

def get_log_num():
  """Get and update log number"""
  # Get num
  f = open(LOG_FILE)
  num = int(f.readline().rstrip())
  f.close()

  # Update num
  new_num = num + 1
  f = open(LOG_FILE, 'w')
  f.write(str(new_num))
  f.close()

  # Return originally read number
  return num

def digest_podcasts():
  """
  Digest most popular podcasts from
  iTunes on a daily-basis
  """
  # Number
  num = get_log_num()

  # Grab all series first
  # SeriesDriver(BASE_DIR + str(num)).get_popular(SiteCrawler().get_genres())

  # Grab all episodes once we have data stored
  storer = \
    (CouchbaseStorer(COUCHBASE_URL)
    if COUCHBASE_PASSWORD == ''
    else CouchbaseStorer(COUCHBASE_URL, COUCHBASE_PASSWORD))

  # EpisodesDriver(DIRECTORY, storer).eps_from_series()

def run_schedule():
  """Check schedule and run pending"""
  while 1:
    schedule.run_pending()
    time.sleep(1)


# Run the app
if __name__ == '__main__':
  # schedule.every(ONE_DAY).seconds.do(digest_podcasts)
  # t = Thread(target=run_schedule)
  # t.start()
  app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
