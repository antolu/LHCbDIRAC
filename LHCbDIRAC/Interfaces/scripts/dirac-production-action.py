#!/usr/bin/env python
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
########################################################################
# File :   dirac-production-action
# Author : Mario Ubeda Garcia
########################################################################
"""
  Start or stop the production(s)
"""

__RCSID__ = "$Id$"
__VERSION__ = "$Revision: 71758 $"

import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s <Production ID> |<Production ID>' % Script.scriptName,
                                     'Arguments:',
                                     '  <Production ID>:      DIRAC Production Id' ] ) )

Script.registerSwitch( 't', 'start', "Start the production" )
Script.registerSwitch( 'p', 'stop', "Stop the production" )

Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()
if len(args) < 1:
  Script.showHelp()
  DIRAC.exit(2)

from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

diracProd = DiracProduction()
exitCode  = 0
errorList = []
start     = False
stop      = False
action    = ''

switches = Script.getUnprocessedSwitches()

for switch in switches:
  opt   = switch[0].lower()
  
  if opt in ( 't', 'start' ):
    start = True        
  if opt in ( 'p', 'stop' ):
    stop  = True   

if start and stop:
  print "ERROR: decide if you want to start or stop ( not both )."     
  DIRAC.exit( 2 )
elif not ( start or stop ):
  print "ERROR: decide if you want to start or stop."     
  DIRAC.exit( 2 ) 
elif start:
  action = 'start'  
elif stop:
  action = 'stop'

for prodID in args:

  result = diracProd.production( prodID, action, disableCheck = False )
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
