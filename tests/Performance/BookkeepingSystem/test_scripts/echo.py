"""

This is a very simple bkk performance test. It calls the service with a message. The service
return the message.

"""
import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
cl = BookkeepingClient()


class Transaction(object):

  def __init__(self):
    self.custom_timers = {}

  def run(self):
    start_time = time.time()
    retVal = cl.echo("simple test")
    if not retVal['OK']:
      print 'ERROR', retVal['Message']
    end_time = time.time()
    self.custom_timers['Bkk_ResponseTime'] = end_time - start_time
    self.custom_timers['Bkk_Echo'] = end_time - start_time


if __name__ == '__main__':
  trans = Transaction()
  trans.run()
  print trans.custom_timers
