#!/usr/bin/env python

__RCSID__ = "$Id$"

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
