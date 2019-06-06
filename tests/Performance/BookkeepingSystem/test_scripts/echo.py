###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
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
