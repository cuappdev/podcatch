from datetime import datetime as dt
from podcasts.top_series_for_topic import fetch_series_all_genres
import podcasts.itunes

def generate_series_for_topic_models(current_entries):
  topics_to_series = dict(fetch_series_all_genres())

  # Overall top series needs a valid id
  topics_to_series[1] = topics_to_series[None]
  del topics_to_series[None]

  existing_topics = set([e.get('topic_id') for e in current_entries])
  nonexisting_topics = \
    set(topics_to_series.keys()) - existing_topics
  inserts = [
      {
          'topic_id': tid,
          'series_list': ','.join(map(str, topics_to_series[tid])),
          'created_at': dt.now(),
          'updated_at': dt.now()
      }
      for tid in nonexisting_topics
  ]
  updates = [
      {
          'topic_id': tid,
          'series_list': ','.join(map(str, topics_to_series[tid])),
          'updated_at': dt.now()
      }
      for tid in existing_topics
  ]
  return inserts, updates

def extract_series_and_episodes_from_feed(feed):
    series_dict = feed.get('series')
    series = {
        'id': series_dict.get('id'),
        'title': series_dict.get('title'),
        'country': series_dict.get('country'),
        'author': series_dict.get('author'),
        'image_url_lg': series_dict.get('image_url_lg'),
        'image_url_sm': series_dict.get('image_url_sm'),
        'feed_url': series_dict.get('feed_url'),
        'genres': ';'.join(series_dict.get('genres')),
        'subscribers_count': 0,
        'created_at': dt.now(),
        'updated_at': dt.now()
    }
    episodes = []
    for episode_dict in feed.get('episodes'):
      try:
        pub_date = None if episode_dict.get('pub_date') is None else \
          dt.fromtimestamp(episode_dict.get('pub_date'))
      except ValueError:
        pub_date = None
      ep = {
          'title': episode_dict.get('title').decode('utf-8'),
          'author': episode_dict.get('author').decode('utf-8'),
          'summary': episode_dict.get('summary').decode('utf-8'),
          'pub_date': pub_date,
          'duration': episode_dict.get('duration'),
          'real_duration_written': False,
          'audio_url': episode_dict.get('audio_url'),
          'tags': ';'.join(episode_dict.get('tags')),
          'series_id': series['id'],
          'recommendations_count': 0,
          'created_at': dt.now(),
          'updated_at': dt.now()
      }
      episodes.append(ep)
    return series, episodes

def gather_unstored_series_for_topic_with_episodes(unstored_series_ids):
  # query url becomes too big if query all at once
  many_series = []
  chunks = [unstored_series_ids[x:x+100]
            for x in range(0, len(unstored_series_ids), 100)]
  for chunk in chunks:
    many_series.extend(podcasts.itunes.get_series_by_ids(chunk))
  print 'Fetching episodes from feeds of unstored series'
  feeds = podcasts.itunes.get_feeds_from_many_series(many_series)
  series_ids = set()
  series_list, episode_list = [], []
  dicts = [extract_series_and_episodes_from_feed(f) for f in feeds]
  for series, episodes in dicts:
    if series['id'] not in series_ids:
      series_list.append(series)
      episode_list.extend(episodes)
    series_ids.add(series['id'])
  return series_list, episode_list
