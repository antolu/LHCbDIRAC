#!/usr/bin/env python
########################################################################
# $Id$
# File :   dirac-lhcb-production-check.py
# Author : Paul Szczypka
########################################################################

"""
dirac-lhcb-production-check.py
Provides general information about a production which should be useful when a shifter is preparing a production status report.

It lists the numbe rof jobs in each major state with an example jobID.
It also lists the base locations of the files produced, the number of events produced and then number of files in the bookkeeping.
"""

from DIRAC.Core.Base import Script
import DIRAC

from random import choice
import sys,time,commands,string,re

def getBoolean(value):
  if value.lower()=='true':
    return True
  elif value.lower()=='false':
    return False
  else:
    print 'ERROR: expected boolean'
    DIRAC.exit(2)

Script.registerSwitch( "q", "Sequential", "Get info for all productions in the range [ProdIDLow,ProdIDHigh]." )
Script.registerSwitch( "r", "RandomExample", "Return random example values rather then simply the first item")
Script.registerSwitch( "j:", "JobStatus=", "Only look at jobs with selected Status")
Script.registerSwitch( "f", "FileExample", "Print example bookkeeping file locations.")
Script.registerSwitch( "d:", "FromDate=", "Start date of query period, string format: 'YYYY-MM-DD'")
Script.parseCommandLine( ignoreErrors = True )


from DIRAC.Interfaces.API.Dirac                             import Dirac
from DIRAC.Interfaces.API.DiracProduction                   import DiracProduction
from DIRAC.BookkeepingSystem.Client.BookkeepingClient       import BookkeepingClient

args = Script.getPositionalArgs()

#Default Values
sequentialProds = False
randomExample=False
optionJobStatus='Done'
fileExample=False
fromDate='2000-01-01'

def usage():
  print 'Usage: %s [<Production ID>]' %(Script.scriptName)
  DIRAC.exit(2)

if len(args) < 1:
  usage()
  
#print Script.getUnprocessedSwitches()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ('q', 'sequential'):
    sequentialProds = True
  if switch[0].lower() in ('randomexample', 'r'):
    randomExample = True
  elif switch[0].lower() in ('jobstatus', 'j'):
    optionJobStatus = switch[1]
  elif switch[0].lower() in ('fileexample' or 'f'):
    fileExample = True
  elif switch[0].lower() in ('fromdate' or 'd'):
    fromDate = switch[1]
    
exitCode = 0
dirac = Dirac()
dp = DiracProduction()
bk = BookkeepingClient()


if sequentialProds:
    if len(args) <= 1:
      print "ERROR: Option 'Sequential' selected but only one argument (%s) supplied." %(args[0])
      DIRAC.exit(1)
    else:
      prodIDLow = int(args[0])
      prodIDHigh = int(args[1])
      print "Sequential Productions: prodLow: %s      prodHigh: %s" %(prodIDLow, prodIDHigh)
      if (prodIDHigh - prodIDLow) <= 0:
        print "Error: ProdIDHigh is less than ProdIDLow, using appropriate range."
        temp = prodIDLow
        prodIDLow = prodIDHigh
        prodIDHigh = temp

print "===========================================================\n"

jobStatus = ('Completed', 'Done', 'Failed', 'Killed', 'Matched', 'Received', 'Running', 'Staging', 'Stalled', 'Waiting')

if not optionJobStatus in jobStatus:
  print "ERROR: Job State: %s is not valid." %(optionJobStatus)
  DIRAC.exit(exitcode)


def loopOverProds(prodID, fromDate, status):
  global jobStatus, randomExample, dp, bk, dirac
  print "%s%s" %('Production ID: ', prodID)
  for status in jobStatus:
    jobs = dp.selectProductionJobs(long(prodID),Status=status, Date=fromDate)
    if jobs['OK']:
      jobIDS = jobs['Value']
      if randomExample:
        exampleJob = choice(jobIDS)
      else:
        exampleJob = jobIDS[0]
      status = status.ljust(10)
      print "Number of %.10s Jobs: %.7d    Example JobID: %s" %(status, len(jobIDS), exampleJob)

  fileTypes = {'LogFilePath':'Log Files', 'BookkeepingLFNs':'LFNs', 'ProductionOutputData':'Output Data'}



  print "Looking at jobs with status %s from date: %s" %(optionJobStatus, fromDate)
  jobs = dp.selectProductionJobs(long(prodID),Status=optionJobStatus, Date=fromDate)

  if jobs['OK']:
    jobIDS = jobs['Value']
    jobJDL = dirac.getJobJDL(jobIDS[0])
    regExp = re.compile(r'(.+/){,}(.*)')
    if randomExample:
      jobJDL = dirac.getJobJDL(choice(jobIDS))
    else:
      jobJDL = dirac.getJobJDL(jobIDS[0])
    for myType in fileTypes:
      print "type: %s" %(myType)

      if jobJDL['Value'].has_key(myType):
        if type(jobJDL['Value'][myType]) is list:
          s = regExp.search(jobJDL['Value'][myType][1])
        else:
          s = regExp.search(jobJDL['Value'][myType])
        if s:
          myFileType = fileTypes[myType].ljust(11)
          print "Loc. of %.11s: %s" %(myFileType, s.group(1))
          if fileExample:
            print "%s %s" %('     Example: ', s.group(0))
        else:
          print "\n%s: %s" %('ERROR: No jobs found with Status: ', optionJobStatus)
      else:
        print "jobJDL does not have key: %s" %(type)

  res = bk.getProductionInformations(prodID)
  if res['OK']:
    val = res['Value']
    print "\nProduction %s Info:" %(prodID)
    infs = val['Production Info']
    if infs != None:
      for info in infs:
        print "Type: %s, Date: %s, EventType: %s" %(info[0], info[1], info[2])
        j = 1
        for i in range(1,7):
          j += 2
          if info[j] and info[j+1]:
            print "Step %s: %s %s" %(i, info[j], info[j+1])
    events = val['Number of events']
    print ""
    for event in events:
      myType = event[0].ljust(4)
      print "Number of Events of type %.4s: %.7d" %(myType, event[1])

    files = val['Number of files']
    for file in files:
      myType = file[1].ljust(4)
      print " Number of Files of type %.4s: %.7d" %(myType, file[0])

    jobs = val['Number of jobs']
    if jobs[0]:
      print "               Number of jobs: %.7d" %(jobs[0])

  print "-----------------------------------------------------------\n"
  return

# Collect Job data:
if sequentialProds:
    for prodID in range(int(prodIDLow), int(prodIDHigh) + 1):
        loopOverProds(prodID, fromDate, optionJobStatus)
else:
    for prodID in args:
        loopOverProds(int(prodID), fromDate, optionJobStatus)


DIRAC.exit(exitCode)
