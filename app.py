# OVERALL IMPORTS
from flask import Flask, request
from threading import Thread
import schedule
import time
import os
import pdb
import json

# PODCAST IMPORTS
from podcasts.series_driver import SeriesDriver
from podcasts.episodes_driver import EpisodesDriver
from podcasts.site_crawler import SiteCrawler
from utils.couchbase_storer import CouchbaseStorer
from utils.series_patcher import SeriesPatcher
from utils.constants import *
from utils.thread_pool import *
import utils.log


# Flask App
app = Flask(__name__)
logger = utils.log.logger
patcher = SeriesPatcher("lol")


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

def start_rss_polling():
  """
  Create a thread pool and a job queue to check the rss feeds of every series
  in the podcasts bucket.
  """
  logger.info("Starting RSS polling with {} threads and job queue of size {}".format(NUM_RSS_THREADS, JOB_QUEUE_SIZE))
  thread_pool = ThreadPool(NUM_RSS_THREADS, JOB_QUEUE_SIZE)

  limit = 100
  offset = 0
  for i in range(NUM_RSS_THREADS):
    series_list = patcher.get_series_with_limit(limit, offset)
    rss_feed_tups = patcher.create_rss_feed_tups(series_list)
    args = (rss_feed_tups, CHECK_TIMESTAMP)
    thread_pool.add_task(patcher.patch_multiple, args)
    offset += limit

  thread_pool.wait_for_all()


def run_schedule():
  """ 
  Check schedule and run pending
  """
  while 1:
    schedule.run_pending()
    time.sleep(1)


@app.route('/refresh/<series_id>')
def refresh(series_id):
  """
  Params:
    series_id [int] - id for the series as designated by apple

  Returns:
    JSON object with keys "success" (either 0 or 1) and "episode" 
    that contains the entire episode object.

  Given a series_id, checks to see if we should request apple
  to get new episodes. If there are new episodes, returns the new episode object.
  """ 
  episode = {"series_id": series_id, "episode-id": 420, "description":"testing"}
  response = {"success": 0, "episode": episode}
  return json.dumps(response)


if __name__ == '__main__':
  # schedule.every(ONE_DAY).seconds.do(digest_podcasts)
  # schedule.every(15*MINUTES).seconds.do(start_rss_polling)
  # t = Thread(target=run_schedule)
  # t.start()
  # start_rss_polling()

  app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
