#!/usr/bin/env python

__RCSID__ = '$Id: dirac-production-progress.py 69330 2013-08-07 14:55:22Z ubeda $'

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

import DIRAC
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

args = Script.getPositionalArgs()

diracProd = DiracProduction()

exitCode = 0
for prodID in args:
  result = diracProd.getProductionProgress( prodID, printOutput = True )
  if result.has_key('Message'):
    print 'Listing production summary failed with message:\n%s' % result[ 'Message' ]
    exitCode = 2
  elif not result:
    print 'Null result for getProduction() call', prodID
    exitCode = 2
  else:
    exitCode = 0

if not args:
  result = diracProd.getProductionProgress( printOutput = True )
  if result.has_key( 'Message' ):
    print 'Listing production summary failed with message:\n%s' % result[ 'Message' ]
    exitCode = 2
  elif not result:
    print 'Null result for getProduction() call'
    exitCode = 2
  else:
    exitCode = 0

DIRAC.exit(exitCode)
