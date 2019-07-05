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
import os
import shutil


def cleanTestDir():
  for fileIn in os.listdir('.'):
    if 'Local' in fileIn:
      shutil.rmtree(fileIn)
    for fileToRemove in ['std.out', 'std.err', 'aLogFileForTest.txt', 'exe-script.py.log', 'ls.log']:
      try:
        os.remove(fileToRemove)
      except OSError:
        continue
