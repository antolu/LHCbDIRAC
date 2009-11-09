#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-production-application-summary.py $
# File :   dirac-production-application-summary
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-production-application-summary.py 18064 2009-11-05 19:40:01Z acasajus $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracProduction                    import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Production ID> <DIRAC Status>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 2:
  usage()

diracProd = DiracProduction()
prodID = args[0]
stat = args[1]
result = diracProd.getProductionApplicationSummary(prodID,status=stat,printOutput=True)
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Getting production application status summary failed with message:\n%s' %(result['Message'])
  DIRAC.exit(2)
else:
  print 'Null result for getProductionApplicationSummary() call'
  DIRAC.exit(2)