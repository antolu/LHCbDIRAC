#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-production-list-all.py $
# File :   dirac-production-list-all
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-production-list-all.py 18064 2009-11-05 19:40:01Z acasajus $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracProduction                    import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

diracProd = DiracProduction()

result = diracProd.getAllProductions(printOutput=True)
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Listing productions failed with message:\n%s' %(result['Message'])
  DIRAC.exit(2)
else:
  print 'Null result for getAllProductions() call'
  DIRAC.exit(2)