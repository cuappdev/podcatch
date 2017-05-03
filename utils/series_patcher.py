from couchbase.bucket import Bucket
from constants import *
from logpod import LogPod

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

# print SeriesPatcher('lol').num_series()
