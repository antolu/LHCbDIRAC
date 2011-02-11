#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Interfaces/scripts/dirac-production-site-summary.py $
# File :   dirac-production-site-summary
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Production ID> [<DIRAC Site>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

diracProd = DiracProduction()
prodID = args[0]

site = None
if len(args) == 2:
  site = args[1]

result = diracProd.getProductionSiteSummary(prodID,site,printOutput=True)
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Getting production site summary failed with message:\n%s' %(result['Message'])
  DIRAC.exit(2)
else:
  print 'Null result for getProductionSiteSummary() call'
  DIRAC.exit(2)