#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/scripts/dirac-lhcb-sam-submit-all.py,v 1.1 2008/10/16 09:51:49 paterson Exp $
# File :   dirac-lhcb-sam-submit-all
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-lhcb-sam-submit-all.py,v 1.1 2008/10/16 09:51:49 paterson Exp $"
__VERSION__ = "$Revision: 1.1 $"
import sys
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "script=", "Optional path to python script to execute in SAM jobs [Experts only]" )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

from DIRAC.LHCbSystem.Testing.SAM.Client.DiracSAM import DiracSAM

script = ''

def usage():
  print 'Usage: %s [--script=<SCRIPT NAME>]' %(Script.scriptName)
  print 'SAM test jobs will be submitted to all CEs defined in the DIRAC CS'
  DIRAC.exit(2)

if args:
  usage()

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower()=="script":
    script=switch[1]

exitCode = 0
diracSAM = DiracSAM()
result = diracSAM._promptUser('Are you sure you want to submit SAM jobs for all CEs known to DIRAC?')
if not result['OK']:
  print 'Action cancelled.'
  DIRAC.exit(2)

result = diracSAM.submitAllSAMJobs(softwareEnableFlag=True,scriptName=script)
if not result['OK']:
  print 'ERROR %s' %result['Message']
  exitCode = 2

DIRAC.exit(exitCode)