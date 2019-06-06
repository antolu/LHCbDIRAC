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
# File :    dirac-bookkeeping-get-file-sisters
# Author :  Zoltan Mathe
########################################################################
"""
  Report sisters (i.e. descendant of ancestor) for the given LFNs
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery

Script.registerSwitch('', 'FileType=', 'FileType to search [ALLSTREAMS.DST]')

Script.setUsageMessage('\n'.join([__doc__.split('\n')[1],
                                  'Usage:',
                                  '  %s [option] eventType  ' % Script.scriptName]))
fileType = 'ALLSTREAMS.DST'
Script.parseCommandLine(ignoreErrors=True)
for switch in Script.getUnprocessedSwitches():
  if switch[0] == "FileType":
    fileType = str(switch[1])

eventTypes = Script.getPositionalArgs()[0]

bkQuery = BKQuery({'EventType': eventTypes, "ConfigName": "MC"}, fileTypes=fileType, visible=True)
print "bkQuery:", bkQuery
prods = bkQuery.getBKProductions()

bk = BookkeepingClient()
for prod in prods:
  res = bk.getProductionInformations(prod)
  if res['OK']:
    value = res['Value']
    print value['Path'].split("\n")[1],
    for nf in value['Number of files']:
      if nf[1] == fileType:
        print nf[0],
    for ne in value['Number of events']:
      if ne[0] == fileType:
        print ne[1],
    print ""
