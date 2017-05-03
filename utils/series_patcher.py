from couchbase.bucket import Bucket
from constants import *
from logpod import LogPod
from Queue import Queue
import threading

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
    self.podlog = LogPod(s_dir)

    # Bucket connections
    self.p_bucket     = Bucket('couchbase://{}/{}'.format(COUCHBASE_URL, PODCASTS_BUCKET))
    self.n_p_bucket = Bucket('couchbase://{}/{}'.format(COUCHBASE_URL, NEW_PODCASTS_BUCKET))

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

  def patch(self, rss_feed_tups, check_timestamp=True):
    """
    Given a list of (series_id, rss_feed_url, last_update) tuples, patch each
      rss_feed_tups: list of (series_id, rss_feed_url, last_update) tuples
      check_timestamp: indicates whether we should only patch if the previous
        patch has turned stale (a.k.a we SHOULD look at the timestamp), or if
        we should patch regardless (a.k.a. IGNORE the previous timestamp)
    """
    pass



print SeriesPatcher('lol').num_series()
