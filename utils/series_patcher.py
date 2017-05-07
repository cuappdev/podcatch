from couchbase.bucket import Bucket
from couchbase_storer import CouchbaseStorer
from constants import *
from logpod import LogPod
from Queue import Queue
import threading
import pdb
import feedparser
import log

from podcasts.models.episode import Episode
from podcasts.models.series import Series

class SeriesPatcher(object):
  """
  Handle coordination of the patching of various series
  """

  def __init__(self, s_dir):
    """
    Constructor: sets up file logging + all bucket connections
      s_dir [string] - directory name for logging
    """
    # Logging to files
    self.podlog = LogPod(s_dir, UPDATE_TIME)

    # Bucket connections
    self.p_bucket     = Bucket('couchbase://{}/{}'.format(COUCHBASE_BASE_URL, PODCASTS_BUCKET))
    self.n_p_bucket   = Bucket('couchbase://{}/{}'.format(COUCHBASE_BASE_URL, NEW_PODCASTS_BUCKET))
    self.n_p_storer   = (CouchbaseStorer(NEW_PODCASTS_BUCKET_URL) if NEW_PODCASTS_BUCKET_PASSWORD == ''
                        else CouchbaseStorer(NEW_PODCASTS_BUCKET_URL, NEW_PODCASTS_BUCKET_PASSWORD))
    self.logger       = log.logger

    # Attempt this b/c primary index is required
    try:
      self.n_p_bucket.n1ql_query(
        'CREATE PRIMARY INDEX ON `{}`'.format(NEW_PODCASTS_BUCKET)
      ).execute()
    except Exception as e:
      print 'Primary index already created. Skipping this..'

  def num_series(self):
    """
    Get number of series currently present in database
    """
    result = self.p_bucket.n1ql_query(
      'SELECT COUNT(*) FROM `{}` WHERE type="series"'.format(PODCASTS_BUCKET)
    ).get_single_result()
    return result['$1']

  def patch_multiple(self, rss_feed_tups, check_timestamp=True):
    """
    Params:
      rss_feed_tups [list] - list of (series_id, rss_feed_url, last_update) tuples

    Returns:
      a list of True/False indicating whether or not the series in the index
      had a new episode to be updated.
    """
    results = []
    for rss_feed_tup in rss_feed_tups:
      series_id, rss_feed_url, last_update = rss_feed_tup
      result = self.patch_series(series_id, rss_feed_url, last_update)
      results.append(result)

    return results


  def patch_series(self, series_id, rss_feed_url, check_timestamp=True):
    """
    Checks the RSS feed of the series and updates the new podcasts bucket 
    if there are new episodes.

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
    if check_timestamp and not self.podlog.needs_update(series_id):
      return False # no need to update

    episode_result = self.p_bucket.n1ql_query(
      'SELECT * FROM `{}` WHERE type="episode" and seriesId={} ORDER BY pubDate DESC'.format(PODCASTS_BUCKET, series_id))

    # Note: we have to do a query here because we need the series object from the database in order to build the full Episode object
    series_json = self.p_bucket.get(CouchbaseStorer.series_key_from_id(series_id)).value
    s = Series.from_db_json(series_json)

    # results are ordered from newest to oldest
    episode_results = []
    for row in episode_result:
      episode_results.append(row)

    # we should already have at least 1
    assert len(episode_results) > 0 

    # results are ordered from newest to oldest
    rss = feedparser.parse(rss_feed_url)
    rss_len = len(rss['entries'])

    # sanity check
    assert rss_len >= len(episode_results)

    diff_len = rss_len - len(episode_results)
    if diff_len == 0:
      # sanity check
      assert rss['entries'][0]['title'] == episode_results[0]['podcasts']['title']
      self.logger.info('Nothing to update from series: {}, title: {}'.format(series_id, series_json['title']))
      return False

    self.logger.info('There are [{}] episodes to patch...'.format(diff_len))

    # make Episode objects out of only the new RSS entries
    ep_dicts = []
    for i in range(diff_len):
      rss_entry = rss['entries'][i]
      ep = Episode(s, rss_entry).__dict__
      ep_dicts.append(ep)
      self.logger.info('Storing new episode_title: {}, series_title: {}'.format(ep['title'], ep['seriesTitle']))

    self.n_p_storer.store_episodes(series_id, ep_dicts)
    self.podlog.update_series(series_id)
    self.logger.info('Updated timestamp for series {}'.format(series_id))
    return True

if __name__=="__main__":
  patcher = SeriesPatcher("lol")
  patcher.patch_series(1002937870, "http://feeds.soundcloud.com/users/soundcloud:users:156542883/sounds.rss");
