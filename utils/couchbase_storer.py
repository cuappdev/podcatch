from couchbase.bucket import Bucket
from podcasts.storers.storer import Storer
import threading
import datetime
import sys
import os

class CouchbaseStorer(Storer):
  """
  Storer of podcasts in Couchbase
  """

  # MARK - Static details

  @staticmethod
  def series_key_from_id(series_id):
    """
    Static method defining the key generation logic for
    storing a series in Couchbase
    """
    return '{}:{}'.format(str(series_id), str(sys.maxsize))

  @staticmethod
  def episode_key_from_info(series_id, pub_date):
    return '{}:{}'.format(str(series_id), str(pub_date))

  # MARK - Instance details

  def __init__(self, url, password=None):
    """
    Constructor
    """
    self.url        = url # Bucket URL
    self.password   = password # Bucket password
    self.lock       = threading.Lock() # Thread-safe utilization of this driver
    self.db         = self._connect_db()
    self.db.timeout = 10

  def _connect_db(self):
    """
    Connect the DB using the url for this storer object
    """
    return Bucket(self.url) if self.password is None else Bucket(self.url, password=self.password)

  def _make_series_key(self, series_dict):
    """
    Series key for Couchbase
    """
    return CouchbaseStorer.series_key_from_id(series_dict['id'])

  def _make_episode_key(self, series_id, episode_dict):
    """
    Episode key for Couchbase
    """
    return CouchbaseStorer.episode_key_from_info(series_id, episode_dict['pubDate'])

  def store(self, result_dict):
    """
    See Storer#store(result_json)
    """
    # Build properly formatted bulk insert
    # plus batching
    bulk_upsert = dict()
    series_id = result_dict['series']['id']
    bulk_upsert[self._make_series_key(result_dict['series'])] = result_dict['series']
    for e in result_dict['episodes']:
      bulk_upsert[self._make_episode_key(series_id, e)] = e

    # Bulk insert (thread-safe)
    self.lock.acquire()
    # Ensure success
    while True:
      try:
        self.db.upsert_multi(bulk_upsert)
        bulk_upsert.clear()
        break
      except Exception:
        print 'Exception Occurred: Trying Again'
        self.db = self._connect_db()
    self.lock.release()
