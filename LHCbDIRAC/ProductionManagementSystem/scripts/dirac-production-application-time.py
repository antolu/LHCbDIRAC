#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-production-application-time
# Author :  Paul Szczypka
########################################################################
"""
  Records the average time spent running all applications in given production, break down by Site. 
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base                                        import Script

from random import choice
import sys, time, commands, string, re
import datetime

Script.registerSwitch( "q", "Sequential", "Get info for all productions in the range [ProdIDLow,ProdIDHigh]." )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Prod ...|Prod1 Prod2' % Script.scriptName,
                                     'Arguments:',
                                     '  Prod:      DIRAC Production Id (or JobGroup)' ] ) )
Script.parseCommandLine( ignoreErrors = True )


from DIRAC.Interfaces.API.Dirac                             import Dirac
from LHCbDIRAC.Interfaces.API.DiracProduction               import DiracProduction
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient

args = Script.getPositionalArgs()

#Default Values
sequentialProds = False

if len( args ) == 0:
  Script.showHelp()

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ( 'q', 'sequential' ):
    sequentialProds = True

exitCode = 0
dirac = Dirac()
dp = DiracProduction()
bk = BookkeepingClient()

print 'This requires analysing all jobs in a "Done" or "Completed" state for each production so can take some time.'

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

def loopOverProds( prodID, siteTimes ):
    prodJobsSummary = dp.getProductionJobSummary( prodID )
    jobsDone = prodJobsSummary['Value']['Done']['Execution Complete']['JobList']
    jobsCompleted = prodJobsSummary['Value']['Completed']['Pending Requests']['JobList']
    jobsToAnalyse = jobsDone + jobsCompleted
    howManyJobs = len( jobsToAnalyse )

    print "Analysing %s jobs for Production %s" % ( howManyJobs, prodID )

    # RegEx for application start and end.
    startRegx = r"(?P<exe>Executing )(?P<app>.*)_(?P<stepNum>.*$)"
    endRegx = r"(?P<app>.*) Step OK"
    # Compile RegEx
    startCregx = re.compile( startRegx )
    endCregx = re.compile( endRegx )

    for job in jobsToAnalyse:
        jobLog = dp.getProdJobLoggingInfo( job )
        result = dirac.attributes( job )
        if not result['OK']:
          errorList.append( ( job, result['Message'] ) )
          exitCode = 2

        jobSite = result['Value']['Site']

        for logBit in jobLog['Value']:
          for bit in logBit:
            s = startCregx.search( bit )
            if s:
              startTime = datetime.datetime( *time.strptime( logBit[3], "%Y-%m-%d %H:%M:%S" )[0:6] )
              if siteTimes.has_key( jobSite ):
                # Add key if it's not already there
                if not siteTimes[jobSite].has_key( s.group( 'app' ) ):
                  print "\033[1;32mADDING KEY: %s for site: %s\033[1;m" % ( s.group( 'app' ), jobSite )
                  siteTimes[jobSite][s.group( 'app' )] = {'Time': 0, 'jobCount': 0}
            else:
              e = endCregx.search( bit )
              if e:
                endTime = datetime.datetime( *time.strptime( logBit[3], "%Y-%m-%d %H:%M:%S" )[0:6] )
                appTime = ( endTime - startTime ).seconds
                if siteTimes.has_key( jobSite ):
                  if siteTimes[jobSite].has_key( e.group( 'app' ) ):
                    siteTimes[jobSite][e.group( 'app' )]['Time'] += appTime
                    siteTimes[jobSite][e.group( 'app' )]['jobCount'] += 1
                  else:
                    print "\033[1;31mERROR in endTime for match: %s\033[1;m" % ( logBit[3] )


    print "Production Statistics for ProdID: %s" % ( prodID )
    for site in siteTimes.keys():
      print "Site: %s" % site
      apps = siteTimes[site]
      for app in apps.keys():
        # Print site metrics
        # Leading zero on the float doesn't seem to work. :(
        print "App: %s  MeanTime (s): %07.2f  Jobs: %d" % ( app.rjust( 13 ), float( apps[app]['Time'] ) / float( apps[app]['jobCount'] ), apps[app]['jobCount'] )

    print "------------------------------------"

    return

# Create Dictionary
def setVars():
  # Tier1 sites
  sites = [
    'LCG.CERN.ch',
    'LCG.CNAF.it',
    'LCG.IN2P3.fr',
    'LCG.NIKHEF.nl',
    'LCG.PIC.es',
    'LCG.RAL.uk',
    'LCG.GRIDKA.de'
    ]

  siteTime = {}
  for site in sites:
    siteTime["%s" % site] = {}
  return siteTime


appTime = setVars()
if sequentialProds:
    for prodID in range( prodIDLow, prodIDHigh + 1 ):
      loopOverProds( prodID, appTime )
else:
    for prodID in args:
      loopOverProds( int( prodID ), appTime )

DIRAC.exit( exitCode )
