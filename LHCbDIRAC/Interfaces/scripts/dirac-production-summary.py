#!/usr/bin/env python

__RCSID__ = '$Id$'

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

import DIRAC
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s [<Production ID>]' % Script.scriptName
  DIRAC.exit(2)

if len(args) < 1:
  usage()
  
diracProd = DiracProduction()

prodID = None
if len(args) > 0:
  prodID = args[0]

result = diracProd.getProductionSummary( prodID, printOutput = True )
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Listing production summary failed with message:\n%s' % result['Message']
  DIRAC.exit(2)
else:
  print 'Null result for getProductionSummary() call'
  DIRAC.exit(2)
