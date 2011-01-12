#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-lhcb-production-check.py
# Author : Paul Szczypka
########################################################################
"""
  Provides general information about a production which should be useful when a shifter is preparing a production status report.

It lists the numbe rof jobs in each major state with an example jobID.
It also lists the base locations of the files produced, the number of events produced and then number of files in the bookkeeping.
"""
__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
import DIRAC

from random import choice
import sys, time, commands, string, re

Script.registerSwitch( "q", "Sequential", "Get info for all productions in the range [ProdIDLow,ProdIDHigh]." )
Script.registerSwitch( "r", "RandomExample", "Return random example values rather then simply the first item" )
Script.registerSwitch( "j:", "JobStatus=", "Only look at jobs with selected Status" )
Script.registerSwitch( "f", "FileExample", "Print example bookkeeping file locations." )
Script.registerSwitch( "D:", "FromDate=", "Start date of query period, string format: 'YYYY-MM-DD'" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Prod ...|Prod1 Prod2' % Script.scriptName,
                                     'Arguments:',
                                     '  Prod:      DIRAC Production Id (or JobGroup)' ] ) )
Script.parseCommandLine( ignoreErrors = True )


from DIRAC.Interfaces.API.Dirac                             import Dirac
from LHCbDIRAC.Interfaces.API.DiracProduction            import DiracProduction
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient

args = Script.getPositionalArgs()

#Default Values
sequentialProds = False
randomExample = False
optionJobStatus = 'Done'
fileExample = False
fromDate = '2000-01-01'

if len( args ) == 0:
  Script.showHelp()

#print Script.getUnprocessedSwitches()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ( 'q', 'sequential' ):
    sequentialProds = True
  if switch[0].lower() in ( 'randomexample', 'r' ):
    randomExample = True
  elif switch[0].lower() in ( 'jobstatus', 'j' ):
    optionJobStatus = switch[1]
  elif switch[0].lower() in ( 'fileexample' or 'f' ):
    fileExample = True
  elif switch[0].lower() in ( 'fromdate' or 'd' ):
    fromDate = switch[1]

exitCode = 0
dirac = Dirac()
dp = DiracProduction()
bk = BookkeepingClient()


if sequentialProds:
  print "length of args: %s" % ( len( args ) )
  if len( args ) == 1:
    print "ERROR: Option 'Sequential' selected but only one argument (%s) supplied." % ( args[0] )
    DIRAC.exit( 2 )
  else:
    try:
      prodIDLow = int( args[0] )
      prodIDHigh = int( args[1] )
    except:
      print 'ERROR: Production IDs must be integers'
      DIRAC.exit( 2 )
    if ( prodIDHigh - prodIDLow ) < 0:
      temp = prodIDLow
      prodIDLow = prodIDHigh
      prodIDHigh = temp
    print "Sequential Productions: prodLow: %s      prodHigh: %s" % ( prodIDLow, prodIDHigh )

print "===========================================================\n"

jobStatus = ( 'Completed', 'Done', 'Failed', 'Killed', 'Matched', 'Received', 'Running', 'Staging', 'Stalled', 'Waiting' )

if not optionJobStatus in jobStatus:
  print "ERROR: Job State: %s is not valid." % ( optionJobStatus )
  DIRAC.exit( 2 )


def loopOverProds( prodID, fromDate, status ):
  global jobStatus, randomExample, dp, bk, dirac
  print "%s%s" % ( 'Production ID: ', prodID )
  for status in jobStatus:
    jobs = dp.selectProductionJobs( long( prodID ), Status = status, Date = fromDate )
    if jobs['OK']:
      jobIDS = jobs['Value']
      if randomExample:
        exampleJob = choice( jobIDS )
      else:
        exampleJob = jobIDS[0]
      status = status.ljust( 10 )
      print "Number of %.10s Jobs: %.7d    Example JobID: %s" % ( status, len( jobIDS ), exampleJob )

  fileTypes = {'LogFilePath':'Log Files', 'BookkeepingLFNs':'LFNs', 'ProductionOutputData':'Output Data'}



  print "Looking at jobs with status %s from date: %s" % ( optionJobStatus, fromDate )
  jobs = dp.selectProductionJobs( long( prodID ), Status = optionJobStatus, Date = fromDate )

  if jobs['OK']:
    jobIDS = jobs['Value']
    jobJDL = dirac.getJobJDL( jobIDS[0] )
    regExp = re.compile( r'(.+/){,}(.*)' )
    if randomExample:
      jobJDL = dirac.getJobJDL( choice( jobIDS ) )
    else:
      jobJDL = dirac.getJobJDL( jobIDS[0] )
    for myType in fileTypes:
      print "type: %s" % ( myType )

      if jobJDL['Value'].has_key( myType ):
        if type( jobJDL['Value'][myType] ) is list:
          s = regExp.search( jobJDL['Value'][myType][1] )
        else:
          s = regExp.search( jobJDL['Value'][myType] )
        if s:
          myFileType = fileTypes[myType].ljust( 11 )
          print "Loc. of %.11s: %s" % ( myFileType, s.group( 1 ) )
          if fileExample:
            print "%s %s" % ( '     Example: ', s.group( 0 ) )
        else:
          print "\n%s: %s" % ( 'ERROR: No jobs found with Status: ', optionJobStatus )
      else:
        print "jobJDL does not have key: %s" % ( type )

  res = bk.getProductionInformations( prodID )
  if res['OK']:
    val = res['Value']
    print "\nProduction %s Info:" % ( prodID )
    infs = val['Production Info']
    if infs != None:
      for info in infs:
        print "Type: %s, Date: %s, EventType: %s" % ( info[0], info[1], info[2] )
        j = 1
        for i in range( 1, 7 ):
          j += 2
          if info[j] and info[j + 1]:
            print "Step %s: %s %s" % ( i, info[j], info[j + 1] )
    events = val['Number of events']
    print ""
    for event in events:
      myType = event[0].ljust( 4 )
      print "Number of Events of type %.20s: %.7s" % ( myType, event[1] )

    files = val['Number of files']
    for file in files:
      myType = file[1].ljust( 4 )
      print " Number of Files of type %.20s: %.7s" % ( myType, file[0] )

    jobs = val['Number of jobs']
    if jobs[0]:
      print "               Number of Steps: %.7s" % ( jobs[0] )

  print "-----------------------------------------------------------\n"
  return

# Collect Job data:
if sequentialProds:
    for prodID in range( prodIDLow, prodIDHigh + 1 ):
        loopOverProds( prodID, fromDate, optionJobStatus )
else:
    for prodID in args:
        loopOverProds( int( prodID ), fromDate, optionJobStatus )


DIRAC.exit( exitCode )
