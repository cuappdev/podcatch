import datetime
import constants
import time
import os

class LogPod(object):
  """
  Framework for logging information about podcasts
  """

  def __init__(self, s_dir, update_time=15 * constants.MINUTE):
    """
    Constructor:
      s_dir: location to store logs
      update_time: second-based time indicator that allows one
        to determine if a RSS feed request is necessary
    """
    self.dir = s_dir
    self.update_time = update_time
    # Establish this directory if it doesn't exist
    if not os.path.exists('./{}'.format(self.dir)):
      os.makedirs('./{}'.format(self.dir))

  def contains_series(self, series_id):
    """
    Boolean indicator determining whether information has been
    logged about a particular series, given the series's
    series_id
    """
    series_id = str(series_id) # So we can find it in a array of strs
    series_ids = []
    for _, _, filenames in os.walk('./{}'.format(self.dir)):
      series_ids.extend(filenames)
    return series_id in series_ids

  def last_updated(self, series_id):
    """
    Check to see when we last updated a series based on the series's
    integer id
    """
    def _read_date(f):
      """
      Given a file `f`, read the date contained within the file
      and return in
      """
      return datetime.datetime.fromtimestamp(int(f.readlines()[0]))

    file_name = './{}/{}'.format(self.dir, str(series_id))
    if os.path.exists(file_name):
      with open(file_name, 'rb') as f:
        return _read_date(f)
    else:
      return None

  def update_series(self, series_id):
    """
    Register a series update, given the series's series_id
    """
    def _write_date(t, f):
      """
      Overwrite file `f` with time `t`
      """
      f.write(int(time.mktime(t.timetuple())))

    file_name = './{}/{}'.format(self.dir, str(series_id))
    with open(file_name, 'w') as f:
      _write_date(datetime.datetime.now(), f)

  def needs_update(self, series_id):
    """
    Returns a boolean / the time of last_upate, indicating whether
    a series needs an update, given the series's series_id
    """
    last_update = self.last_updated(series_id)
    if last_update == None: # Meaning we don't have it
      return True, None
    diff = datetime.datetime.now() - last_update
    return (diff.total_seconds() > self.update_time), last_update
