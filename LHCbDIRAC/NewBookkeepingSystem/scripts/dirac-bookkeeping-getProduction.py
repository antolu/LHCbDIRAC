#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-getProduction
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve productions with the given conditions
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... ProgramName=<name> ProgramVersion=<version> Evt=<evt>' % Script.scriptName,
                                     'Arguments:',
                                     '  ProgramName:     Name of the Program (ie, Boole, ALL)',
                                     '  ProgramVersion:  Version of the Program (ie, v13r3, ALL)',
                                     '  Evt:             Event Type (ie, 13463000, ALL)' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

if len( args ) < 3:
  Script.showHelp()

exitCode = 0

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

prgn = ''
prgv = ''
evt = ''
for i in range( 0, len( args ) ):
  arg = args[i]
  values = arg.split( '=' )
  if len( values ) == 1:
    Script.showHelp()
  if values[0] == 'ProgramName':
    prgn = values[1]
  elif values[0] == 'ProgramVersion':
    prgv = values[1]
  elif values[0] == 'Evt':
    evt = values[1]

res = bk.getProductionsWithPrgAndEvt( prgn, prgv, evt )
if not res['OK']:
  print 'ERROR', res['Message']
  exitCode = 2
else:
  values = res['Value']
  print  '%s %s %s' % ( 'EventTypeId'.ljust( 20 ), 'Description'.ljust( 50 ), 'Production'.ljust( 40 ) )
  for record in values:
    eid = record[0]
    desc = record[1]
    prod = record[2]
    print '%s %s %s' % ( str( eid ).ljust( 20 ), desc.ljust( 50 ), str( prod ).ljust( 40 ) )

DIRAC.exit( exitCode )
