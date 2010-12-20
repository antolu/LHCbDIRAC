#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-get-file-descendants
# Author :  Zoltan Mathe
########################################################################
"""
  Report descendants for the given LFNs
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... LFN|File [Level]' % Script.scriptName,
                                     'Arguments:',
                                     '  LFN:      Logical File Name',
                                     '  File:     Name of the file with a list of LFNs',
                                     '  Level:    Number of levels search (default: 1)' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) == 1:
  level = 1
elif len( args ) == 2:
  try:
    level = int( args[1] )
  except:
    Script.showHelp()
else:
  Script.showHelp()

file = str( args[0] )

exitCode = 0

lfns = []
try:
  files = open( file )
  for f in files:
    lfns += [f.strip()]
except Exception, ex:
  lfns = [file]

bk = BookkeepingClient()
result = bk.getDescendents( lfns, level )

if not result['OK']:
  print 'ERROR %s' % ( result['Message'] )
  exitCode = 2
else:
  values = result['Value']
  print 'Successful:'
  files = values['Successful']
  for i in files.keys():
    print i + ':'
    for j in files[i]:
      print '                 ' + j
  print 'Failed:', values['Failed']

DIRAC.exit( exitCode )
