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
########################################################################
# File :    dirac-bookkeeping-simulationconditions-list
# Author :  Zoltan Mathe
########################################################################
"""
  List simulation conditions from the Bookkeeping
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([ __doc__.split('\n')[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ]))
Script.parseCommandLine(ignoreErrors=True)

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
exitCode = 0

res = bk.getSimConditions()
if res['OK']:
  dbresult = res['Value']
  for record in dbresult:
    print 'SimId: ' + str(record[0]).ljust(10)
    print '  SimDescription: ' + str(record[1]).ljust(10)
    print '  BeamCond: ' + str(record[2]).ljust(10)
    print '  BeamEnergy: ' + str(record[3]).ljust(10)
    print '  Generator: ' + str(record[4]).ljust(10)
    print '  MagneticField: ' + str(record[5]).ljust(10)
    print '  DetectorCond: ' + str(record[6]).ljust(10)
    print '  Luminosity: ' + str(record[7]).ljust(10)
    print '  G4settings: ' + str(record[8]).ljust(10)

else:
  print 'ERROR:', res['Message']

DIRAC.exit(exitCode)

