# OVERALL IMPORTS
from flask import Flask, request
from threading import Thread
import schedule
import time
import os
import pdb

# PODCAST IMPORTS
from podcasts.series_driver import SeriesDriver
from podcasts.episodes_driver import EpisodesDriver
from podcasts.site_crawler import SiteCrawler
from utils.couchbase_storer import CouchbaseStorer
from utils.constants import *

# Flask App
app = Flask(__name__)


def digest_podcasts():
  """
  Digest most popular podcasts from
  iTunes on a daily-basis
  """

  # Grab all series first
  SeriesDriver(BASE_DIR).get_series_from_urls(SiteCrawler().all_urls())

  storer = \
    (CouchbaseStorer(PODCASTS_BUCKET_URL)
    if PODCASTS_BUCKET_PASSWORD == ''
    else CouchbaseStorer(PODCASTS_BUCKET_URL, PODCASTS_BUCKET_PASSWORD))

  # Grab all episodes once we have data stored
  EpisodesDriver(DIRECTORY, storer).eps_from_series()


def run_schedule():
  """ 
  Check schedule and run pending
  """
  while 1:
    schedule.run_pending()
    time.sleep(1)




# Run the app
if __name__ == '__main__':
  # schedule.every(ONE_DAY).seconds.do(digest_podcasts)
  # t = Thread(target=run_schedule)
  # t.start()

  app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
