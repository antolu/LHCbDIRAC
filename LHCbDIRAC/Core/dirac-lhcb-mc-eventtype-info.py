#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-file-sisters
# Author :  Zoltan Mathe
########################################################################
"""
  Report sisters (i.e. descendant of ancestor) for the given LFNs
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.BookkeepingSystem.Client.BKQuery import BKQuery

import pprint

Script.registerSwitch( '', 'FileType=', 'FileType to search [ALLSTREAMS.DST]' )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option] eventType  ' % Script.scriptName ] ) )
fileType = 'ALLSTREAMS.DST'
Script.parseCommandLine( ignoreErrors = True )
for switch in Script.getUnprocessedSwitches():
  if switch[0] == "FileType" :
    fileType = str(switch[1])

eventTypes = Script.getPositionalArgs()[0]

bkQuery = BKQuery( {'EventType': eventTypes, "ConfigName":"MC"}, fileTypes=fileType, visible = True)
print "bkQuery:",bkQuery
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
