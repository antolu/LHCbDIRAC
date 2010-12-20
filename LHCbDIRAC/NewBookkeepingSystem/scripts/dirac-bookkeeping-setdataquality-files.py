#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-setdataquality-files
# Author :  Zoltan Mathe
########################################################################
"""
  Set Quality Flag for the given files  
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... LFN|File Flag' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name',
                                     '  File:     Name of the file with a list of LFNs',
                                     '  Flag:     Quality Flag' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

if len( args ) < 2:
  result = bk.getAvailableDataQuality()
  if not result['OK']:
    print 'ERROR: %s' % ( result['Message'] )
    DIRAC.exit( 2 )
  flags = result['Value']
  print "Available Data Quality Flags"
  for flag in flags:
    print flag
  Script.showHelp()

exitCode = 0
file = args[0]
flag = args[1]
lfns = []
try:
  files = open( file )
  for f in files:
    lfns += [f.strip()]
except Exception, ex:
  lfns = [file]

result = bk.setQuality( lfns, flag )

if not result['OK']:
  print 'ERROR: %s' % ( result['Message'] )
  exitCode = 2
else:
  succ = result['Value']['Successful']
  failed = result['Value']['Failed']
  print 'The data quality has been set for the following files:'
  for i in succ:
    print i

  if len( failed ) != 0:
    print 'The data quality has not been set for the following files:'
    for i in failed:
      print i

DIRAC.exit( exitCode )
