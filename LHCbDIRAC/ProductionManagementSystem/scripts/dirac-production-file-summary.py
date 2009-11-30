#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-production-file-summary
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.1 $"
import sys,string
import DIRAC
from DIRAC.Core.Base import Script

def getBoolean(value):
  if value.lower()=='true':
    return True
  elif value.lower()=='false':
    return False
  else:
    print 'ERROR: expected boolean'
    DIRAC.exit(2)

#Default values
status=None
outFile=None
summary=True
printVerbose=False

Script.registerSwitch( "", "Status=", "ProductionDB file status to select" )
Script.registerSwitch( "", "OutputFile=", "Output file to store file records" )
Script.registerSwitch( "", "Summary=", "Print a summary of the files (True/False) default is True" )
Script.registerSwitch( "", "PrintOutput=", "Print all file records (extremely verbose) default is False" )
Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.LHCbSystem.Client.DiracProduction import DiracProduction

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower()=="status":
    status = switch[1]
  elif switch[0].lower()=="outputfile":
    outFile=switch[1]
  elif switch[0].lower()=="summary":
    summary=getBoolean(switch[1])
  elif switch[0].lower()=="printoutput":
    printVerbose=getBoolean(switch[1])

args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s <ProductionID> <Options> [Try -h,--help for more information]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args)!=1:
  usage()

exitCode = 0

diracProd = DiracProduction()

productionID = args[0]
try:
  productionID = int(productionID)
except Exception,x:
  print 'Production ID must be an integer, not %s:\n%s' %(productionID,x)
  DIRAC.exit(2)

result = diracProd.productionFileSummary(productionID,selectStatus=status,outputFile=outFile,printSummary=summary,printOutput=printVerbose)
if not result['OK']:
  print 'ERROR %s' %result['Message']
  exitCode = 2

DIRAC.exit(exitCode)