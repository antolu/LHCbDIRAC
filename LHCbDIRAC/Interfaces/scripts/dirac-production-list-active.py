#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Interfaces/scripts/dirac-production-list-active.py $
# File :   dirac-production-list-active
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

diracProd = DiracProduction()

result = diracProd.getActiveProductions(printOutput=True)
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Listing active productions failed with message:\n%s' %(result['Message'])
  DIRAC.exit(2)
else:
  print 'Null result for getActiveProductions() call'
  DIRAC.exit(2)