#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-production-job-get-id.py $
# File :   dirac-production-job-get-id
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-production-job-get-id.py 18064 2009-11-05 19:40:01Z acasajus $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.LHCbSystem.Client.DiracProduction import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <WMS Job ID> [<WMS Job ID>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args)<1:
  usage()

jobIDs = []
diracProd = DiracProduction()
try:
  jobIDs = [int(jobID) for jobID in args]
except Exception,x:
  print 'ERROR WMS JobID(s) must be integers'
  DIRAC.exit(2)

exitCode = 0
errorList = []

for job in jobIDs:
  result = diracProd.getWMSProdJobID(job,printOutput=True)
  if result.has_key('Message'):
    errorList.append( (job, result['Message']) )
    exitCode = 2
  elif not result:
    errorList.append( (job, 'Null result for getWMSProdJobID() call' ) )
    exitCode = 2
  else:
    exitCode = 0

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)