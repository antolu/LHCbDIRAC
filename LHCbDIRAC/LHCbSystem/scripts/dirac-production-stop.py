#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-production-stop.py $
# File :   dirac-production-stop
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-production-stop.py 18064 2009-11-05 19:40:01Z acasajus $"
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
command = args[0]

for prodID in args:

  result = diracProd.production(prodID,'stop',printOutput=True,disableCheck=False)
  if result.has_key('Message'):
    errorList.append( (prodID, result['Message']) )
    exitCode = 2
  elif not result:
    errorList.append( (prodID, 'Null result for production() call' ) )
    exitCode = 2
  else:
    exitCode = 0

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)