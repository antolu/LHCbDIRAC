#!/usr/bin/env python
########################################################################
# File :   dirac-production-set-agent-type
# Author : Mario Ubeda Garcia
########################################################################
"""
  Set the Agent Type for a(the) given transformation(s)
"""
__RCSID__   = "$Id$"
__VERSION__ = "$Revision$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s <Production ID> |<Production ID>' % Script.scriptName,
                                     'Arguments:',
                                     '  <Production ID>:      DIRAC Production Id' ] ) )

Script.registerSwitch( 'a', 'automatic', "Set automatic agent type" )
Script.registerSwitch( 'm', 'manual', "Set manual agent type" )

Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()
if len(args) < 1:
  Script.showHelp()
  DIRAC.exit(2)

from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

diracProd = DiracProduction()
exitCode  = 0
errorList = []
command   = args[0]

automatic = False
manual    = False
type      = ''

switches = Script.getUnprocessedSwitches()

for switch in switches:
  opt   = switch[0].lower()
  
  if opt in ( 'a', 'automatic' ):
    automatic = True        
  if opt in ( 'm', 'manual' ):
    manual    = True        

if automatic and manual:
  print "ERROR: decide if you want automatic or manual ( not both )."     
  DIRAC.exit( 2 )
elif not ( automatic or manual ):
  print "ERROR: decide if you want automatic or manual."     
  DIRAC.exit( 2 ) 
elif automatic:
  type = 'automatic'  
elif manual:
  type = 'manual'
      
for prodID in args:

  result = diracProd.production( prodID, type, disableCheck = False )
  if result.has_key( 'Message' ):
    errorList.append( ( prodID, result[ 'Message' ] ) )
    exitCode = 2
  elif not result:
    errorList.append( ( prodID, 'Null result for production() call' ) )
    exitCode = 2
  else:
    exitCode = 0

for error in errorList:
  print "ERROR %s: %s" % error

DIRAC.exit( exitCode )
