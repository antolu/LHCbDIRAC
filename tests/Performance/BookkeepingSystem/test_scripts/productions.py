#!/usr/bin/env python
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

import sys
import random
import time

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
cl = BookkeepingClient()

res = cl.getAvailableProductions()
if not res['OK']:
  print res['Message']
  sys.exit(0)

allproductions = sorted([i[0] for i in res['Value']], reverse=True)
productions = allproductions[:10000]


class Transaction(object):

  def __init__(self):
    self.custom_timers = {}

  def run(self):
    # print len(datasets)
    i = random.randint(0, len(productions) - 1)
    production = productions[i]
    # print dataset
    self.custom_timers['Bkk_Error'] = 0
    start_time = time.time()
    r = cl.getProductionProducedEvents(production)
    # print r
    if not r['OK']:
      self.custom_timers['Bkk_Error'] = self.custom_timers['Bkk_Error'] + 1
      print r['Message']
    end_time = time.time()
    self.custom_timers['Bkk_ResponseTime'] = end_time - start_time


if __name__ == '__main__':
  trans = Transaction()
  trans.run()
  print trans.custom_timers
