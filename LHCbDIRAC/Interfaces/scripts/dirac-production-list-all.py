#!/usr/bin/env python
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

import DIRAC
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

args = Script.getPositionalArgs()

diracProd = DiracProduction()

result = diracProd.getAllProductions( printOutput = True )
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Listing productions failed with message:\n%s' % result['Message']
  DIRAC.exit(2)
else:
  print 'Null result for getAllProductions() call'
  DIRAC.exit(2)
