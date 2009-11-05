#! /usr/bin/env python
########################################################################
# $Id$
# File :   dirac-lhcb-production-check-outputs.py
# Author : Paul Szczypka
########################################################################
import sys, time, commands, os
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

"""
dirac-lhcb-production-check-outputs: used to obtain all files produced by a prodiction which need to be cleaned.
It creates (and overwrites) the following files by default:
bookkeepingLFNs.txt     : A list of all bookkeeping LFN's for selected productions.
prods.txt               : A list of all selected productions
productionOutputData.txt: A list of all the production Output Data


"""


Script.registerSwitch( "q", "Sequential", "Get LFN's for all productions in the range [ProdIDLow,ProdIDHigh]." )
Script.registerSwitch( "n", "NoFiles", "Do not write or create files (requires verbose to be useful)." )
Script.registerSwitch( "f:","FromDate=", "Start date of query period, string format: 'YYYY-MM-DD'" )
Script.registerSwitch( "p:","OutputDir=", "Output Directory for files. Default: $PWD" )
Script.registerSwitch( "v", "Verbose", "Enable Verbose Output")
#Script.registerSwitch( "", "", "" )

Script.parseCommandLine ( ignoreErrors = True )

from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.DiracProduction                    import DiracProduction

def getBoolean(value):
  if value.lower()=='true':
    return True
  elif value.lower()=='false':
    return False
  else:
    print 'ERROR: expected boolean'
    DIRAC.exit(2)

def usage():
  print 'Usage: %s [<Production IDs>] -f <YYYY-MM-DD> -n <-p <OutputDir>> -v | -q <Production ID Low> <Production ID High>' %(Script.scriptName)
  DIRAC.exit(2)


args = Script.getPositionalArgs()

# Default Values
sequentialProds = False
recordData = True
fromDate = 'today'
outputDir = os.environ.get('PWD')
verbose = False

if len(args) < 1:
  usage()

#print Script.getUnprocessedSwitches()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ('q', 'sequential'):
    sequentialProds = True
  elif switch[0].lower() in ('n', 'nofiles'):
    recordData = False
  elif switch[0].lower() in ('f', 'fromdate'):
    fromDate = switch[1]
  elif switch[0].lower() in ('p', 'outputdir'):
    if not os.path.isdir(switch[1]):
      print "ERROR: %s is not a valid directory, using default value." %(switch[1])
    else:
      outputDir = switch[1]
  elif switch[0].lower() in ('v', 'verbose'):
    verbose = True

if verbose:
  print "Using outputDir: %s" %(outputDir)



      
dirac = Dirac()
d = DiracProduction()

if sequentialProds:
    print "length of args: %s" %(len(args))
    if len(args) <= 1:
      print "ERROR: Option 'Sequential' selected but only one argument (%s) supplied." %(args[0])
      DIRAC.exit(1)
    else:
      prodIDLow = int(args[0])
      prodIDHigh = int(args[1])
      print "prodHigh: %s      prodLow: %s" %(prodIDHigh, prodIDLow)
      if (prodIDHigh - prodIDLow) <= 0:
        print "Error: ProdIDHigh is less than ProdIDLow, using appropriate range."
        temp = prodIDLow
        prodIDLow = prodIDHigh
        prodIDHigh = temp

prodFileName = 'prods.txt'
bkFileName = 'bookkeepingLFNs.txt'
podFileName = 'productionOutputData.txt'

if recordData:
    if verbose:
        print "Opening files ..."
    listProds = open('%s/%s' %(outputDir, prodFileName),'w')
    listBook = open('%s/%s' %(outputDir, bkFileName),'w')
    listOutput = open('%s/%s' %(outputDir, podFileName),'w')

numProds = 0
numBKFiles = 0
numPODFiles = 0

def loopOverProds(prodID,fromDate):
    global numProds, numBKFiles, numPODFiles, recordData, d, verbose
    print "Processing Production: %s" %(prodID)
    jobs = d.selectProductionJobs(long(prodID),Status='Done', Date=fromDate)
    if jobs['OK']:
        numProds += 1
        result = d.getProductionSummary(prodID)
        jobIDS = jobs['Value']
        if recordData:
            listProds.write("%s %s%s" %(prodID, fromDate, '\n'))
        if jobIDS:
            for job in jobIDS:
                jobJDL =  dirac.getJobJDL(job)
                if jobJDL['Value'].has_key('BookkeeepingLFNs'):
                    if not isinstance(jobJDL['Value']['BookkeeepingLFNs'], list ):
                      myList = [jobJDL['Value']['BookkeeepingLFNs']]
                    else:
                      myList = jobJDL['Value']['BookkeeepingLFNs']
                    for poData in myList:
                      if verbose:
                        print "%s%s%s%s%s     %s" %('ProdID: ', prodID, ' JobID: ', job, ' BookkeeepingLFNs:', poData)
                      numBKFiles += 1
                      if recordData:
                        listOutput.write("%s%s" %(poData, '\n'))

#                    print "%s%s%s%s%s      %s" %('ProdID: ', prodID, ' JobID: ', job, ' BookkeepingLFNs:', jobJDL['Value']['BookkeeepingLFNs'])
#                    numFiles += 1
#                    if recordData:
#                        listBook.write("%s%s" %(jobJDL['Value']['BookkeeepingLFNs'], '\n'))
                else:
                    if verbose:
                        print "%s%s%s%s%s" %('ProdID: ', prodID, ' JobID: ', job, ' has no BookkeepingLFNs.')
                if jobJDL['Value'].has_key('ProductionOutputData'):
                    if not isinstance(jobJDL['Value']['ProductionOutputData'], list ):
                      myList = [jobJDL['Value']['ProductionOutputData']]
                    else:
                      myList = jobJDL['Value']['ProductionOutputData']
                    for poData in myList:
                      if verbose:
                        print "%s%s%s%s%s %s" %('ProdID: ', prodID, ' JobID: ', job, ' ProductionOutputData:', poData)
                      numPODFiles += 1
                      if recordData:
                        listOutput.write("%s%s" %(poData, '\n'))
                else:
                  if verbose:
                    print "%s%s%s%s%s" %('ProdID: ', prodID, ' JobID: ', job, ' has no ProductionOutputData.')
    return

if sequentialProds:
    for prodID in range(int(prodIDLow), int(prodIDHigh) + 1):
        loopOverProds(prodID,fromDate)
else:
    for prodID in args:
        loopOverProds(int(prodID),fromDate)
    

print "%s%s%s%s%s%s" %("Prods: ", numProds, "  BK Files: ", numBKFiles, "  ProductionOutputData Files: ", numPODFiles)
if recordData:
    listProds.close()
    listBook.close()
    listOutput.close()

if recordData:
  print "Data written to:\n%s\n%s\n%s" %(prodFileName, bkFileName, podFileName)


DIRAC.exit(1)
