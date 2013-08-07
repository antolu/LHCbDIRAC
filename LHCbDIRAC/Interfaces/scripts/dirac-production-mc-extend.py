#!/usr/bin/env python

__RCSID__ = '$Id$'

from DIRAC.Core.Base import Script
Script.parseCommandLine( ignoreErrors = True )

import DIRAC
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

args = Script.getPositionalArgs()

def usage():
  """ usage
  
  Prints script usage 
  
  """
    
  print 'Usage: %s <Production ID> <Number Of Jobs>' % Script.scriptName
  DIRAC.exit(2)

if len(args)<2 or len(args)>2:
  usage()

diracProd = DiracProduction()
prodID = args[0]
number = args[1]

result = diracProd.extendProduction( prodID, number, printOutput = True )
if result['OK']:
  DIRAC.exit(0)
elif result.has_key('Message'):
  print 'Extending production failed with message:\n%s' % result['Message']
  DIRAC.exit(2)
else:
  print 'Null result for extendProduction() call'
  DIRAC.exit(2)
