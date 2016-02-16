#!/usr/bin/env python

__RCSID__ = '$Id: dirac-production-logging-info.py 69333 2013-08-07 15:20:55Z ubeda $'

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

import DIRAC
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

args = Script.getPositionalArgs()

def usage():
  """ usage
  
  Prints script usage 
  
  """
  
  print 'Usage: %s <Production ID> |<Production ID>' % Script.scriptName
  DIRAC.exit(2)

if len(args) < 1:
  usage()

diracProd = DiracProduction()
exitCode = 0
errorList = []

for prodID in args:
  result = diracProd.getProductionLoggingInfo( prodID, printOutput = True )
  if result.has_key('Message'):
    errorList.append( (prodID, result['Message']) )
    exitCode = 2
  elif not result:
    errorList.append( (prodID, 'Null result for getProduction() call' ) )
    exitCode = 2
  else:
    exitCode = 0

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)
