#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-production-job-info
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.LHCbSystem.Client.DiracProduction import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Production ID> <Production Job ID>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args)!=2:
  usage()

diracProd = DiracProduction()
prodID=args[0]
jobID=args[1]
try:
  prodID=int(prodID)
  jobID=int(jobID)
except Exception,x:
  print 'ERROR ProdID and Production JobID must be integers'
  DIRAC.exit(2)

result = diracProd.getProdJobInfo(prodID,jobID,printOutput=True)
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Getting production job info failed with message:\n%s' %(result['Message'])
  DIRAC.exit(2)
else:
  print 'Null result for getProdJobInfo() call'
  DIRAC.exit(2)