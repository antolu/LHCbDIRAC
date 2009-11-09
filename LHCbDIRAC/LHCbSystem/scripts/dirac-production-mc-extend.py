#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-production-mc-extend.py $
# File :   dirac-production-mc-extend
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-production-mc-extend.py 18064 2009-11-05 19:40:01Z acasajus $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.LHCbSystem.Client.DiracProduction import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <Production ID> <Number Of Jobs>' %(Script.scriptName)
  DIRAC.exit(2)

if len(args)<2 or len(args)>2:
  usage()

diracProd = DiracProduction()
prodID = args[0]
number = args[1]

result = diracProd.extendProduction(prodID,number,printOutput=True)
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Extending production failed with message:\n%s' %(result['Message'])
  DIRAC.exit(2)
else:
  print 'Null result for extendProduction() call'
  DIRAC.exit(2)