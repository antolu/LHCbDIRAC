#!/usr/bin/env python

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

import DIRAC
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s [<Production ID>]' %(Script.scriptName)
  DIRAC.exit(2)

#if len(args) < 1:
#  usage()

diracProd = DiracProduction()

exitCode = 0
for prodID in args:
  result = diracProd.getProductionProgress(prodID,printOutput=True)
  if result.has_key('Message'):
    print 'Listing production summary failed with message:\n%s' %(result['Message'])
    exitCode = 2
  elif not result:
    print 'Null result for getProduction() call', prodID
    exitCode = 2
  else:
    exitCode = 0

if not args:
  result = diracProd.getProductionProgress(printOutput=True)
  if result.has_key('Message'):
    print 'Listing production summary failed with message:\n%s' %(result['Message'])
    exitCode = 2
  elif not result:
    print 'Null result for getProduction() call', prodID
    exitCode = 2
  else:
    exitCode = 0


DIRAC.exit(exitCode)
