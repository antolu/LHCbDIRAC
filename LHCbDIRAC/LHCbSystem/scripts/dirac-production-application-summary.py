#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-production-application-summary
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