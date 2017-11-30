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

if __name__ == '__main__':
  conn = create_connector()
  print conn.config
  print conn.get_count('episodes')
