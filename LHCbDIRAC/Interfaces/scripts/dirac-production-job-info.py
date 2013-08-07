#!/usr/bin/env python
from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

import DIRAC
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Production ID> <Production Job ID>' % Script.scriptName
  DIRAC.exit(2)

if len(args)!=2:
  usage()

diracProd = DiracProduction()
prodID = args[0]
jobID  = args[1]
try:
  prodID = int(prodID)
  jobID  = int(jobID)
except Exception,x:
  print 'ERROR ProdID and Production JobID must be integers'
  DIRAC.exit(2)

result = diracProd.getProdJobInfo( prodID, jobID, printOutput = True )
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Getting production job info failed with message:\n%s' %(result['Message'])
  DIRAC.exit(2)
else:
  print 'Null result for getProdJobInfo() call'
  DIRAC.exit(2)
