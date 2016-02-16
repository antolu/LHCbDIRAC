#!/usr/bin/env python
__RCSID__ = "$Id: dirac-production-file-status.py 69333 2013-08-07 15:20:55Z ubeda $"
__VERSION__ = "$Revision: 69333 $"
import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "ProductionID=", "Restrict query to given production ID (default is to show status for all)" )
Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

prodID = ''
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "productionid":
    prodID = switch[1]

args = Script.getPositionalArgs()

def usage():
  """ usage
  
  Prints script usage 
  
  """
  
  print 'Usage: %s <LFN> [<LFN>] [--ProductionID=<ID>] [Try -h,--help for more information]' % Script.scriptName
  DIRAC.exit( 2 )

if len( args ) < 1:
  usage()

if prodID:
  try:
    prodID = int( prodID )
  except Exception, x:
    print 'ERROR ProductionID should be an integer'
    DIRAC.exit( 2 )

diracProd = DiracProduction()
exitCode = 0
result = diracProd.checkFilesStatus( args, prodID, printOutput = False )
if not result['OK']:
  print 'ERROR %s' % ( result['Message'] )
  exitCode = 2

DIRAC.exit( exitCode )
