#!/usr/bin/env python

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
