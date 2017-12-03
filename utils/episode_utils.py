import os
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

def diff_check_series_epsiodes(connector, single_series):
  series_id = single_series.get('id')
  query = 'SELECT * FROM episodes WHERE series_id = {}'.format(series_id)

  # Grab the episodes we currently have in the database
  current_episodes = perform_query(connector, query)
  # Assumption: title = uniqueness indicator for the episodes of a series
  current_ep_titles = set([e.get('title') for e in current_episodes])

  # Grab the feed for this series
  episodes_from_feed = itunes.\
    get_rss_feed_data_from_series(Series(**single_series)).\
    get('episodes')

  new_episodes = \
    [e for e in episodes_from_feed if e.get('title') not in current_ep_titles]
  #
  # for e in new_episodes:
  #   print e

  print len(current_episodes)
  print len(episodes_from_feed)
  print

if __name__ == '__main__':
  conn = create_connector()

  series = get_series(conn)

  for s in series:
    diff_check_series_epsiodes(conn, s)
