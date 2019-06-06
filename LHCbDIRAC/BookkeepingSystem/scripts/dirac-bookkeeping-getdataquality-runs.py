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
# File :    dirac-bookkeeping-getdataquality-runs.py
# Author :  Zoltan Mathe
########################################################################
"""
  Get Data Quality Flag for the given run
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script


Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Run ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Run:      Run number' ] ) )
Script.parseCommandLine( ignoreErrors = True )
runSet = set( int( id ) for arg in Script.getPositionalArgs() for id in arg.split( ',' ) )

if not runSet:
  Script.showHelp()
  DIRAC.exit()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
cl = BookkeepingClient()
print "-----------------------------------"
print "Run Number".ljust( 15 ) + "Stream".ljust( 10 ) + "Flag".ljust( 10 )
print "-----------------------------------"
for runId in sorted( runSet ):
  retVal = cl.getRunFilesDataQuality( runId )
  if retVal['OK']:
    for run, stream, flag in sorted( ( run, stream, flag ) for run, flag, stream in retVal["Value"] ):
      print str( run ).ljust( 15 ) + str( stream ).ljust( 10 ) + str( flag ).ljust( 10 )
    print "-----------------------------------"
  else:
    print retVal["Message"]

DIRAC.exit()

