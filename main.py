import sys
import os
import threading
import datetime
import logging
from Queue import Queue, Empty
import podcasts.itunes as itunes
from podcasts.models.series import Series
from podcasts.models.episode import Episode
from appdev.connectors import MySQLConnector

PODCAST_DB_USERNAME = sys.argv[1]
PODCAST_DB_PASSWORD = sys.argv[2]
PODCAST_DB_HOST = sys.argv[3]
PODCAST_DB_NAME = sys.argv[4]

# Make sure we don't see logs we don't want
logging.getLogger('podfetch').disabled = True

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
    series_ids_to_episodes[e.get('series_id')].append(e)

  return all_series, series_ids_to_episodes

def diff_check_single_series(connector, single_series, current_episodes):
  # Assumption: title = uniqueness indicator for the episodes of a series
  current_ep_titles = set([e['title'] for e in current_episodes])

  # Grab the feed for this series
  pcast_series = Series(s_id=single_series.get('id'), **single_series)
  episodes_from_feed = itunes.\
    get_rss_feed_data_from_series(pcast_series).\
    get('episodes')

  # Novel episodes found as a result of reading the RSS feed
  new_episodes = [e for e in episodes_from_feed \
    if e.get('title').decode('utf-8') not in current_ep_titles]

  print u'{}: {} new episodes'.format(single_series.get('title'),
                                      0 if not new_episodes
                                      else len(new_episodes)).encode('utf-8')

  return new_episodes

class DiffCheckThread(threading.Thread):
  def __init__(self, input_queue, output_queue, \
    connector, series_ids_to_episodes):
    super(DiffCheckThread, self).__init__()
    self.input_queue = input_queue
    self.output_queue = output_queue
    self.connector = connector
    self.series_ids_to_episodes = series_ids_to_episodes
    self.daemon = True

  def run(self):
    empty = False
    while not empty:
      try:
        # Grab from queue + lookup corresponding episodes
        gotten_series = self.input_queue.get()
        current_eps = self.series_ids_to_episodes[gotten_series.get('id')]
        # Grab the newest episodes
        new_episodes = \
          diff_check_single_series(self.connector, gotten_series, current_eps)
        # Throw them into the output queue
        for e in new_episodes:
          self.output_queue.put(e)
      except Empty as e:
        print e
      finally:
        self.input_queue.task_done()
        empty = self.input_queue.empty()

# Get data -> diff check -> storage
def main():
  # Create our connection to the database
  print 'Creating connector'
  connector = create_connector()

  # PART 1: Grab the data
  print 'Fetching series and episode data'
  all_series, series_ids_to_episodes = get_data(connector)

  # PART 2: Multithread patching the data
  input_queue = Queue()
  for s in all_series:
    input_queue.put(s)
  output_queue = Queue()

  print 'Starting diff checker'
  for _ in xrange(0, 25):
    t = DiffCheckThread(input_queue, output_queue, \
      connector, series_ids_to_episodes)
    t.start()

  input_queue.join()
  new_episodes = list(output_queue.queue)

  print 'Formatting episodes'
  # PART 3: Store the new episodes
  for e in new_episodes:
    # Things that don't belong in the SQL row
    del e['image_url_sm']
    del e['image_url_lg']
    del e['type']
    del e['series_title']
    # Necessary fields for a successful write
    e['recommendations_count'] = 0
    e['tags'] = ';'.join(e['tags'])
    e['created_at'] = datetime.datetime.now()
    e['updated_at'] = datetime.datetime.now()
    e['real_duration_written'] = 0
    e['pub_date'] = None if e.get('pub_date') is None else \
      datetime.datetime.fromtimestamp(e.get('pub_date'))

  print 'Starting write'
  connector.write_batch('episodes', new_episodes)
  print 'Done'

if __name__ == '__main__':
  main()
