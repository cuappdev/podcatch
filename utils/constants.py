import os

# Times
MINUTE = 60
HOUR   = 60 * MINUTE
DAY    = 24 * HOUR
WEEK   = 7 * DAY
YEAR   = 365 * DAY

# Couchbase
COUCHBASE_URL      = os.environ['COUCHBASE_URL']
COUCHBASE_PASSWORD = os.environ['COUCHBASE_PASSWORD']

# Buckets
PODCASTS_BUCKET     = os.environ['PODCASTS_BUCKET']
NEW_PODCASTS_BUCKET = os.environ['NEW_PODCASTS']
