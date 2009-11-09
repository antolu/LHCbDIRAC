#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-production-summary.py $
# File :   dirac-production-summary
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-production-summary.py 18064 2009-11-05 19:40:01Z acasajus $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracProduction                    import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s [<Production ID>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()
  
diracProd = DiracProduction()

prodID = None
if len(args) > 0:
  prodID = args[0]

result = diracProd.getProductionSummary(prodID,printOutput=True)
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Listing production summary failed with message:\n%s' %(result['Message'])
  DIRAC.exit(2)
else:
  print 'Null result for getProductionSummary() call'
  DIRAC.exit(2)