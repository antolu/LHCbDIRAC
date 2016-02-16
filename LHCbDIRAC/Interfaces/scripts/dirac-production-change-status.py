#!/usr/bin/env python

__RCSID__   = "$Id: dirac-production-change-status.py 76778 2014-07-24 08:58:06Z fstagni $"

import DIRAC
from DIRAC.Core.Base import Script

Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()

from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
diracProd = DiracProduction()

def usage():
  print 'Usage: %s <Command> <Production ID> |<Production ID>' % Script.scriptName
  commands = diracProd.getProductionCommands()['Value']
  print "\nCommands include: %s" % ( ', '.join( commands.keys() ) )
  print '\nDescription:\n'
  for n, v in commands.items():
    print '%s:' %n
    for i, j in v.items():
      print '     %s = %s' % ( i, j )

  DIRAC.exit(2)

if len(args) < 2:
  usage()

exitCode = 0
errorList = []
command = args[0]

for prodID in args[1:]:

  result = diracProd.production( prodID, command, disableCheck = False )
  if result.has_key('Message'):
    errorList.append( (prodID, result['Message']) )
    exitCode = 2
  elif not result:
    errorList.append( (prodID, 'Null result for getProduction() call' ) )
    exitCode = 2
  else:
    exitCode = 0

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit(exitCode)
