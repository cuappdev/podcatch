from Queue import Queue
from threading import Thread
import pdb

class Worker(Thread):
  """
  Worker that continues to poll jobs from the job queue.
  Only shuts down if there are no more jobs or if the program shuts down.
  """
  def __init__(self, queue):
    """
      Params:
        queue [Queue] - concurrent safe job queue
        daemon [bool] - killed automatically on program shutdown
    """
    Thread.__init__(self)
    self.jobs = queue
    self.daemon = True
    self.start()

  def run(self):
    empty = False
    while not empty:
      func, args, kargs = self.jobs.get()
      try:
        func(*args, **kargs)
      except Exception, e: # pylint: disable=W0703
        print e
      finally:
        self.jobs.task_done()
        empty = self.jobs.empty()

class ThreadPool(object):
  """
  A general thread pool that keeps a job queue of (func, args) where
  the function will be executed with the given args by a thread.
  """
  def __init__(self, num_threads, queue_size):
    """
      Constructor:
        num_threads [int] - size of the thread pool
        jobs [Queue] - concurrent safe job queue
        queue_size [int] - size of the job queue
    """
    self.num_threads = num_threads
    self.jobs = Queue(queue_size)
    self.workers = [Worker(self.jobs) for _ in range(num_threads)]

  def add_task(self, func, args, **kargs):
    self.jobs.put((func, args, kargs))

  def wait_for_all(self):
    self.jobs.join()
