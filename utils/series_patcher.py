from couchbase.bucket import Bucket
from constants import *
from logpod import LogPod
from Queue import Queue
import threading
import pdb
import feedparser

from podcasts.models.episode import Episode
from podcasts.models.series import Series

class SeriesPatcher(object):
  """
  Handle coordination of the patching of various series
  """

  def __init__(self, s_dir):
    """
    Constructor: sets up file logging + all bucket connections
      s_dir: directory for logging
    """
    # Logging to files
    self.podlog = LogPod(s_dir, UPDATE_TIME)

    # Bucket connections
    self.p_bucket     = Bucket('couchbase://{}/{}'.format(COUCHBASE_BASE_URL, PODCASTS_BUCKET))
    self.n_p_bucket = Bucket('couchbase://{}/{}'.format(COUCHBASE_BASE_URL, NEW_PODCASTS_BUCKET))

    # Attempt this b/c primary index is required
    try:
      self.n_p_bucket.n1ql_query(
        'CREATE PRIMARY INDEX ON `{}`'.format(NEW_PODCASTS_BUCKET)
      ).execute()
    except Exception as e:
      print 'Primary index already created'

  def num_series(self):
    """
    Get number of series currently present in database
    """
    result = self.p_bucket.n1ql_query(
      'SELECT COUNT(*) FROM `{}` WHERE type="series"'.format(PODCASTS_BUCKET)
    ).get_single_result()
    return result['$1']

  def patch_all(self):
    """
    Patch all series that currently exist in the database
    """
    pass

  def patch_multiple(self, rss_feed_tups, check_timestamp=True):
    """
    Params:
      rss_feed_tups [list] - list of (series_id, rss_feed_url, last_update) tuples

    Returns:
      a list of True/False indicating whether or not the series in the index
      had a new episode to be updated.
    """
    pass

  def patch_series(self, series_id, rss_feed_url, check_timestamp=True):
    """
    Params:
      series_id [int] - apple series id
      rss_feed_url [string] - rss url to query for the series
      check_timestamp [bool] - indicates whether we should only patch if the previous
        patch has turned stale (a.k.a we SHOULD look at the timestamp), or if
        we should patch regardless (a.k.a. IGNORE the previous timestamp)

    Returns:
      True/False depending on if series_id had a new episode to be updated.
      If True, the new episode will also get put in NEW_PODCASTS_BUCKET
    """
    episode_result = self.p_bucket.n1ql_query('SELECT * FROM `{}` WHERE type="episode" and seriesId={} ORDER BY pubDate DESC'.format(PODCASTS_BUCKET, series_id))

    # Note: we have to do two queries here because we need the series object from the database in order to build the full Episode object
    series_result = self.p_bucket.n1ql_query('SELECT * FROM `{}` WHERE type="series" and id={}'.format(PODCASTS_BUCKET, series_id)).get_single_result()

    # results are ordered from newest to oldest
    episode_results = []
    for row in episode_result:
      episode_results.append(row)

    rss = feedparser.parse(rss_feed_url)

    # Series()

    # query couchbase for series_id entries length
    # use EpisodeWorker.request_rss(url)
    # look into rss['entries'] and find length to check new episodes
    # rss['entries'][num]['title']

    pdb.set_trace()


  def check_diff(self, old_rss_entries, new_rss_entries):
    """
    Params:
      old_entries [list] - list of en 
      new_entries [list] - 
    Returns:

    """


    pass

if __name__=="__main__":
  patcher = SeriesPatcher("lol")
  patcher.patch_series(1001228357, "http://feed.theplatform.com/f/kYEXFC/zVfDzVbQRltO");


