#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/scripts/dirac-lhcb-sam-submit-ce.py,v 1.2 2009/08/19 15:14:40 roma Exp $
# File :   dirac-lhcb-sam-submit-ce
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-lhcb-sam-submit-ce.py,v 1.2 2009/08/19 15:14:40 roma Exp $"
__VERSION__ = "$Revision: 1.2 $"
import sys,string
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "ce=", "Computing Element to submit to (must be in DIRAC CS)" )
Script.registerSwitch( "", "removeLock=", "Force lock removal at site (True/False)" )
Script.registerSwitch( "", "deleteSharedArea=", "Force deletion of the shared area at site (True/False)" )
Script.registerSwitch( "", "enable=", "Global enable flag, set to False for debuggging (True/False)" )
Script.registerSwitch( "", "softwareEnable=", "Software enable flag, set to False for 'safe' mode submission of SAM jobs, disables software module including removal of software (True/False)" )
Script.registerSwitch( "", "reportEnable=", "Report enable flag, set to False for to disables reportSoftware module (True/False)" )
Script.registerSwitch( "", "logUpload=", "Log file upload flag (True/False)" )
Script.registerSwitch( "", "publishResults=", "Publish results to SAM DB flag (True/False)" )
Script.registerSwitch( "", "mode=", "Job submission mode (set to local for debugging)" )
Script.registerSwitch( "", "install_project=", "Optional install_project URL [Experts only]" )
Script.registerSwitch( "", "script=", "Optional path to python script to execute in SAM jobs [Experts only]" )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.LHCbSystem.Testing.SAM.Client.DiracSAM import DiracSAM

args = Script.getPositionalArgs()

#Default values
ce = ''
removeLock=False
deleteSharedArea=False
enable=True
softwareEnable=True
reportEnable=False
logUpload=True
publishResults=True
mode=None
install_project=None
scriptName=''

def usage():
  print 'Usage: %s [Try -h,--help for more information]' %(Script.scriptName)
  DIRAC.exit(2)

if args:
  usage()

def getBoolean(value):
  if value.lower()=='true':
    return True
  elif value.lower()=='false':
    return False
  else:
    print 'ERROR: expected boolean'
    DIRAC.exit(2)

exitCode = 0

for switch in Script.getUnprocessedSwitches():
  if switch[0]=="ce":
    ce=switch[1]
  elif switch[0]=="removeLock":
    removeLock=getBoolean(switch[1])
  elif switch[0]=="deleteSharedArea":
    deleteSharedArea=getBoolean(switch[1])
  elif switch[0]=="enable":
    enable=getBoolean(switch[1])
  elif switch[0]=="logUpload":
    logUpload=getBoolean(switch[1])
  elif switch[0]=="publishResults":
    publishResults=getBoolean(switch[1])
  elif switch[0]=="mode":
    mode=switch[1]
  elif switch[0]=="softwareEnable":
    softwareEnable=getBoolean(switch[1])
  elif switch[0]=="reportEnable":
    reportEnable=getBoolean(switch[1])
  elif switch[0]=="install_project":
    install_project=switch[1]
  elif switch[0].lower()=="script":
    scriptName=switch[1]

diracSAM = DiracSAM()
result = diracSAM.submitSAMJob(ce,removeLock,deleteSharedArea,logUpload,publishResults,mode,enable,softwareEnable,reportEnable,install_project,scriptName)
if not result['OK']:
  print 'ERROR %s' %result['Message']
  exitCode = 2
else:
  print 'JobID: %s' %(result['Value'])

DIRAC.exit(exitCode)
