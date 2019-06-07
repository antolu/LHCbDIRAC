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

import time
import random

from DIRAC.Core.Base import Script
Script.parseCommandLine(ignoreErrors=True)

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

lfns = []
with open('testfiles.txt') as f:
  for i in f:
    lfns += [i.strip()]


class Transaction(object):

  def __init__(self):
    self.custom_timers = {}

  def run(self):
    nb = random.randint(0, len(lfns) - 1)
    start_time = time.time()
    retVal = bk.getFileMetadata(lfns[:nb])
    if not retVal['OK']:
      print retVal['Message']
    end_time = time.time()

    self.custom_timers['Bkk_ResponseTime'] = end_time - start_time


if __name__ == '__main__':
  trans = Transaction()
  trans.run()
  print trans.custom_timers
