from flask import Flask, request
from threading import Thread
import schedule
import time
import os


# Constants
LOG_FILE           = "log"
BASE_DIR           = "csv"
COUCHBASE_URL      = os.environ['COUCHBASE_URL']
COUCHBASE_PASSWORD = os.environ['COUCHBASE_PASSWORD']
ONE_DAY            = 86400

# Flask App
app = Flask(__name__)


def get_log_num():
  """Get and update log number"""
  # Get num
  f = open(LOG_FILE)
  num = int(f.readline().rstrip())
  f.close()

  # Update num
  new_num = num + 1
  f = open(LOG_FILE, 'w')
  f.write(str(new_num))
  f.close()

  # Return originally read number
  return num


def digest_podcasts():
  """
  Digest most popular podcasts from
  iTunes on a daily-basis
  """
  num = get_log_num()
  print(num)
  # TODO - actually load podcasts 


def run_schedule():
  """Check schedule and run pending"""
  while 1:
    schedule.run_pending()
    time.sleep(1)


# Run the app
if __name__ == '__main__':
  schedule.every(10).seconds.do(digest_podcasts)
  t = Thread(target=run_schedule)
  t.start()
  app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)
