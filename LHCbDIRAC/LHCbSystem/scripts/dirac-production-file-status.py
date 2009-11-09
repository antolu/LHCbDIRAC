#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/scripts/dirac-production-file-status.py $
# File :   dirac-production-file-status
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-production-file-status.py 18064 2009-11-05 19:40:01Z acasajus $"
__VERSION__ = "$Revision: 1.1 $"
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "ProductionID=", "Restrict query to given production ID (default is to show status for all)" )
Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.LHCbSystem.Client.DiracProduction import DiracProduction
from DIRAC import gConfig

import time

prodID = ''
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower()=="productionid":
    prodID = switch[1]

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <LFN> [<LFN>] [--ProductionID=<ID>] [Try -h,--help for more information]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()

if prodID:
  try:
    prodID=int(prodID)
  except Exception,x:
    print 'ERROR ProductionID should be an integer'
    DIRAC.exit(2)

diracProd = DiracProduction()
exitCode = 0
result = diracProd.checkFilesStatus(args,prodID,printOutput=True)
if not result['OK']:
  print 'ERROR %s' %(result['Message'])
  exitCode = 2

DIRAC.exit(exitCode)
