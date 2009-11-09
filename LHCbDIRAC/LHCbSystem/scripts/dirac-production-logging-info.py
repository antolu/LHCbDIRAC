#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-production-logging-info.py $
# File :   dirac-production-logging-info
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-production-logging-info.py 18064 2009-11-05 19:40:01Z acasajus $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracProduction                    import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Production ID> |<Production ID>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

diracProd = DiracProduction()
exitCode = 0
errorList = []

for prodID in args:
  result = diracProd.getProductionLoggingInfo(prodID,printOutput=True)
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