#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-production-submit.py $
# File :   dirac-production-submit
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-production-submit.py 18064 2009-11-05 19:40:01Z acasajus $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracProduction                    import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Production ID> <NumberOfJobs> |[<DIRAC Site>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 2:
  usage()

diracProd = DiracProduction()
prodID = args[0]

try:
  jobs = int(args[1])
except Exception,x:
  print 'Expected integer for number of jobs', args[1]
  DIRAC.exit(2)

site=''
if len(args) > 2:
  site = args[2]

result = diracProd.submitProduction(prodID,jobs,site)
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Submission failed with message:\n%s' %(result['Message'])
  DIRAC.exit(2)
else:
  print 'Problem during submitProduction() call'
  print result
  DIRAC.exit(2)