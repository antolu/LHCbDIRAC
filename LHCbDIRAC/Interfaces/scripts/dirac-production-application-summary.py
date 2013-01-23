#!/usr/bin/env python

import DIRAC
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Production ID> <DIRAC Status>' % ( Script.scriptName )
  DIRAC.exit( 2 )

if len( args ) < 2:
  usage()

diracProd = DiracProduction()
prodID = args[0]
stat = args[1]
result = diracProd.getProductionApplicationSummary( prodID, status = stat, printOutput = True )
if result['OK']:
  DIRAC.exit( 0 )
elif result.has_key( 'Message' ):
  print 'Getting production application status summary failed with message:\n%s' % ( result['Message'] )
  DIRAC.exit( 2 )
else:
  print 'Null result for getProductionApplicationSummary() call'
  DIRAC.exit( 2 )
