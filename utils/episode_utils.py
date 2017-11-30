import os
from appdev.connectors import MySQLConnector

PODCAST_DB_USERNAME = os.getenv('PODCAST_DB_USERNAME')
PODCAST_DB_PASSWORD = os.getenv('PODCAST_DB_PASSWORD')
PODCAST_DB_HOST = os.getenv('PODCAST_DB_HOST')
PODCAST_DB_NAME = os.getenv('PODCAST_DB_NAME')

def create_connector():
  connector = MySQLConnector(PODCAST_DB_USERNAME, PODCAST_DB_PASSWORD, \
    PODCAST_DB_HOST, PODCAST_DB_NAME)
  return connector

def get_series(connector):
  rows = \
    connector.read_batch('series', start=0, end=None, interval_size=1000)
  return rows

def get_episodes_by_series_ids(connector, series_ids):
  sql_queries = [
      'SELECT * FROM episodes WHERE series_id = {}'.format(series_id)
      for series_id in series_ids]
  return connector.execute_batch(sql_queries)

if __name__ == '__main__':
  conn = create_connector()
  series_ids = [series[2] for series in get_series(conn)]
  print len(series_ids)
  episodes_per_series = get_episodes_by_series_ids(conn, series_ids)
  print len(episodes_per_series)
