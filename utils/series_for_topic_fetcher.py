import datetime
from podcasts.top_series_for_topic import fetch_series_all_genres

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
          'created_at': datetime.datetime.now(),
          'updated_at': datetime.datetime.now()
      }
      for tid in nonexisting_topics
  ]
  updates = [
      {
          'topic_id': tid,
          'series_list': ','.join(map(str, topics_to_series[tid])),
          'updated_at': datetime.datetime.now()
      }
      for tid in existing_topics
  ]
  return inserts, updates
