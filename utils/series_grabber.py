# Local
from constants import *
from logpod import LogPod
from couchbase_storer import CouchbaseStorer

# Py-Podcast
from podcasts.series_crawler import SeriesCrawler
from podcasts.episode_worker import EpisodeWorker
from podcasts.site_crawler import SiteCrawler
from podcasts.models.series import Series
from podcasts.constants import *
from podcasts.api import API

# Couchbase
from couchbase.exceptions import NotFoundError
from couchbase.bucket import Bucket

# Native Python
from Queue import Queue

# Imports
import threading
import urllib
import log
import pprint

class SeriesGrabberDriver(object):
  """
  Grabs new series and stores them and their series in the podcast
  bucket defined in Couchbase. Overall driver for this procedure
  """

  def __init__(self, num_threads=10):
    """
    Constructor:
      num_threads [int] - Number of threads driving the process of
        grabbing new series
    """
    # Used for setting fields
    podcast_url      = 'couchbase://{}/{}'.format(COUCHBASE_BASE_URL, PODCASTS_BUCKET)
    podcast_pass     = PODCASTS_BUCKET_PASSWORD
    # Fields
    self.num_threads = num_threads
    self.bucket      = (Bucket(podcast_url) if podcast_pass == ''
                        else Bucket(podcast_url, podcast_pass))
    self.storer      = (CouchbaseStorer(podcast_url) if podcast_pass == ''
                        else CouchbaseStorer(podcast_url, podcast_pass))
    self.logger      = log.logger

  def get_ids(self, urls):
    """
    Get ids from iTunes Preview Podcast site
    Params:
      urls [list of string URLs] - Indicates the pages to retrieve IDs from
    Returns:
      A list of ids as strings retrieved from the pages provided via the
      urls param (no duplicates)
    """
    # Establish queue
    genre_url_queue = Queue()

    # Queue, Id Set, + Set Lock
    id_set = set()
    set_lock = threading.Lock()

    # Spawn threads
    for i in xrange(self.num_threads):
      t = SeriesIdWorker(genre_url_queue, id_set, set_lock)
      t.daemon = True
      t.start()

    # Load up the queue with "jobs"
    for u in urls:
      genre_url_queue.put(u)

    # Wait for everything to finish
    genre_url_queue.join()

    # Return the resulting set
    return list(id_set)

  def in_db(self, series_id):
    """
    Checks to see if a particular series in the database
    Params:
      series_id [string] - iTunes series ID in string form
    Returns:
      True if the database contains the series, false otherwise
    """
    try:
      self.bucket.get(CouchbaseStorer.series_key_from_id(series_id))
      return True
    except NotFoundError as e:
      return False

  def new_series_ids(self, ids):
    """
    Determines which series are new by checking the database in a multi-threaded
    manner
    Params:
      ids [list of strings] - List of series ids retrieved from iTunes site
    Returns:
      A list of ids in string form, reflecting the series that are new and
      not in the database currently
    """
    new_ids = set()
    for s_id in ids:
      if not self.in_db(s_id):
        self.logger.info('Found a new series of ID: {}'.format(s_id))
        new_ids.add(s_id)
    return list(new_ids)

  def new_series(self, new_ids):
    """
    Grabs new series
    Params:
      new_ids [list of strings] - List of series' ids, reflecting series that
        are not currently in the database
    Returns:
      A list of Series objects
    """
    series_jsons = []
    i = 0; j = 100
    while(i < len(new_ids)):
      curr_ids = new_ids[i:j]
      ids_with_coms = ','.join(curr_ids)
      id_param = { 'id': ids_with_coms }
      results = API().req_itunes(ITUNES_LOOKUP_URL + urllib.urlencode(id_param)).json()['results']
      series_jsons.extend(results)
      i += 100; j+= 100;
    return [Series.from_itunes_json(j) for j in series_jsons]

  def update_db(self, new_series):
    """
    Grabs episodes corresponding to new series -> drops them in the db in a
    multi-threaded manner
    Params:
      new_series [list of Series] - List of new series
    """
    # Threads
    threads = []

    # Spawn threads
    for i in xrange(20):
      t = EpisodeWorker(self.storer, new_series, i)
      threads.append(t)
      t.start()

    # Wait for threads to finish
    for t in threads:
      t.join()

  def go(self):
    """
    Get the world
    """
    urls = SiteCrawler().all_urls() # -> all URLs on iTunes preview website
    ids = self.get_ids(urls)
    new_ids = self.new_series_ids(ids)
    new_series = self.new_series(new_ids)
    self.update_db(new_series)


class SeriesIdWorker(threading.Thread):
  """
  Thread that works on contributing to the bag of series_ids
  retrieved from iTunes, indicating what podcasts are currently
  available on iTunes
  """

  def __init__(self, genre_url_queue, id_set, set_lock):
    """
    Constructor:
      genre_url_queue [string Queue] - Concurrently-safe queue filled with genre
        paginated URLs that we're grabbing series_ids froms
      id_set [set of ints] - Set of series_ids seen
      set_lock [Lock] - Lock needed to add things to the global id_set
    """
    super(SeriesIdWorker, self).__init__()
    self.q        = genre_url_queue
    self.set_lock = set_lock
    self.id_set   = id_set
    self.crawler  = SeriesCrawler()
    self.logger   = log.logger

  def add_to_set(self, s_ids):
    """
    Contribute to the id_set in a concurrently-safe way
    """
    self.set_lock.acquire()
    for s_id in s_ids:
      self.id_set.add(s_id)
    self.set_lock.release()

  def run(self):
    """
    Run task, popping / processing URLs from genre_url_queue
    """
    while True:
      new_url = self.q.get()
      self.logger.info('Getting IDs from {}'.format(new_url))
      self.crawler.set_url(new_url)
      s_ids = self.crawler.get_ids()
      self.add_to_set(s_ids)
      self.q.task_done()
      self.logger.info('Got IDs from {}'.format(new_url))


if __name__ == '__main__':
  # Pretty printer
  pp = pprint.PrettyPrinter()
  grabber = SeriesGrabberDriver()
  ids =  grabber.get_ids(['https://itunes.apple.com/us/genre/podcasts-science-medicine/id1315?mt=2&letter=D'])
  new_ids = grabber.new_series_ids(ids)
  new_series = grabber.new_series(new_ids)
  grabber.update_db(new_series)
