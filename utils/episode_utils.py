import os
import threading
import podcasts.itunes as itunes
from podcasts.models.series import Series
from podcasts.models.episode import Episode
from appdev.connectors import MySQLConnector

PODCAST_DB_USERNAME = os.getenv('PODCAST_DB_USERNAME')
PODCAST_DB_PASSWORD = os.getenv('PODCAST_DB_PASSWORD')
PODCAST_DB_HOST = os.getenv('PODCAST_DB_HOST')
PODCAST_DB_NAME = os.getenv('PODCAST_DB_NAME')

def create_connector():
  connector = MySQLConnector(PODCAST_DB_USERNAME, PODCAST_DB_PASSWORD, \
    PODCAST_DB_HOST, PODCAST_DB_NAME)
  return connector

def perform_query(connector, query):
  rows = connector.execute_batch([query])[0]
  return rows if rows else []

def get_series(connector):
  rows = \
    connector.read_batch('series', start=0, end=None, interval_size=1000)
  return rows

def get_episodes(connector):
  episode_rows = \
    connector.read_batch('episodes', start=0, end=None, interval_size=1000)
  return episode_rows

# All the data needed to perform an update
def get_data(connector):
  all_series = get_series(connector)
  all_episodes = get_episodes(connector)

  series_ids_to_episodes = {s.get('id') : [] for s in all_series}
  for e in all_episodes:
    series_id_to_episodes[e.get('series_id')].append(e)

  return all_series, series_ids_to_episodes

def diff_check_single_series(connector, single_series, current_episodes):
  # Assumption: title = uniqueness indicator for the episodes of a series
  current_ep_titles = set([e.get('title') for e in current_episodes])

  # Grab the feed for this series
  pcast_series = Series(s_id=single_series.get('id'), **single_series)
  episodes_from_feed = itunes.\
    get_rss_feed_data_from_series(pcast_series).\
    get('episodes')

  # Novel episodes found as a result of reading the RSS feed
  new_episodes = \
    [e for e in episodes_from_feed if e.get('title') not in current_ep_titles]
  return new_episodes

class DiffCheckThread(threading.Thread):
  def __init__(self, input_queue, output_queue, series_ids_to_episodes):
    super(DiffCheckThread, self).__init__()
    self.input_queue = input_queue
    self.output_queue = output_queue
    self.series_ids_to_episodes = series_ids_to_episodes
    self.daemon = True

  def run(self):
    # Check the queue for a series -> lookup the episodes ->
    # diff check -> newest episodes
    # -> add all to output queue
    pass

# Get data -> diff check -> storage
# TODO

if __name__ == '__main__':
  conn = create_connector()

  series = get_series(conn)

  for s in series:
    diff_check_single_series_epsiodes(conn, s)
