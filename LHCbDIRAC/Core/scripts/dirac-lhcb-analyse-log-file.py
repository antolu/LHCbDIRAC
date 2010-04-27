#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Core/scripts/dirac-lhcb-run-test-job.py $
# File :   dirac-lhcb-analyse-log-file.py
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id:  dirac-lhcb-analyse-log-file.py 23248 2010-03-18 07:57:40Z paterson $"

import sys,string,os,shutil

import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "f:", "LogFile=", "Path to log file you wish to analyse" )
Script.registerSwitch( "p:", "Project=", "Optional: project name (will be guessed if not specified)" )

Script.parseCommandLine( ignoreErrors = True )

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from LHCbDIRAC.Core.Utilities.ProductionLogAnalysis import analyseLogFile

args = Script.getPositionalArgs()

logFile = ''
projectName = ''

#Methods to help with the script execution
def usage():
  print 'Usage: %s -f <LOGFILE> -p <PROJECT NAME> [Try -h,--help for more information]' %(Script.scriptName)
  DIRAC.exit(2)
  
#Start the script and perform checks
if args or not Script.getUnprocessedSwitches():
  usage()

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ('p','project'):
    projectName=switch[1]
  elif switch[0].lower() in ('f','logfile'):
    logFile=switch[1]

exitCode = 0
try:
  result = analyseLogFile(logFile,projectName)
except Exception,x:
  gLogger.exception('Log file analysis failed with exception: "%s"' %x)
  exitCode=2
  DIRAC.exit(exitCode)    

if not result['OK']:
  gLogger.warn(result)
  gLogger.error('Problem found with log file %s: "%s"' %(logFile,result['Message']))
  exitCode=2
else:
  gLogger.verbose(result)
  gLogger.info('Log file %s, %s' %(logFile,result['Value']))
  
DIRAC.exit(exitCode)  