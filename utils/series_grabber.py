from constants import *
from logpod import LogPod
from Queue import Queue


class SeriesGrabber(object):
  """
  Grabs new series and stores them and their series in the podcast
  bucket defined in Couchbase
  """

  def __init__(self):
    """
    Constructor: sets up a driver to grab new series from iTunes
    """


    
