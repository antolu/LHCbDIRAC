#! /usr/bin/env python
########################################################################
# File :    dirac-lhcb-job-logging-check
# Author :  Greig A Cowan
########################################################################
""" check the logging info of a job """
__RCSID__ = "$Id: dirac-lhcb-job-logging-check.py 76842 2014-07-25 08:24:53Z fstagni $"

import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "Status=", "Primary status" )
Script.registerSwitch( "", "MinorStatus=", "Secondary status" )
Script.registerSwitch( "", "ApplicationStatus=", "Application status" )
Script.registerSwitch( "", "Site=", "Execution site" )
Script.registerSwitch( "", "Owner=", "Owner (DIRAC nickname)" )
Script.registerSwitch( "", "Date=", "Date in YYYY-MM-DD format, if not specified default is today" )
Script.registerSwitch( "", "JobGroup=", "Select jobs for specified job group" )
Script.registerSwitch( "", "Verbose=", "For more detailed information about file and job states. Default False." )
Script.addDefaultOptionValue( "LogLevel", "ALWAYS" )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.Dirac import Dirac
args = Script.getPositionalArgs()

wmsStatus = None
minorStatus = None
appStatus = None
site = None
owner = None
jobGroup = None
date = None
verbose = False

def usage():
  """ help function """
  print 'Usage: %s [Try -h,--help for more information]' % ( Script.scriptName )
  DIRAC.exit( 2 )

def getJobMetadata( localwmsStatus, localminorStatus, localappStatus, localsite, localowner, localjobGroup, intdate, intdirac ):
  """Gets information about jobs from the WMS"""
  # getJobStates
  result_wms = intdirac.selectJobs( Status = localwmsStatus,
                                 MinorStatus = localminorStatus,
                                 ApplicationStatus = localappStatus,
                                 Site = localsite,
                                 Owner = localowner,
                                 JobGroup = localjobGroup,
                                 Date = intdate
                                 )

  if not result_wms['OK']:
    print 'ERROR %s' % result_wms['Message']
    DIRAC.exit( 2 )
  else:
    # create list of jobIDs in this state belonging to this production
    localjobIDs = result_wms['Value']

  return localjobIDs

def getAttributes( localjobID ):
  """ get the attributes of the JOB """
  result = dirac.attributes( localjobID, printOutput = False )
  if result['OK']:
    return result['Value']['Owner']

def getLogging( localjobID ):
  """ get the logging info of the JOB """
  result = dirac.loggingInfo( localjobID, printOutput = False )
  if result['OK']:
    try:
      for status in result['Value']:
        if ( status[0] == 'Running' ) and ( 'error' in status[2].lower() or \
             'not found' in status[2].lower() or \
             'exited with status' in status[2].lower() ):
          failed_time = status[3]
          return failed_time, status[2]
        elif ( status[0] == 'Completed' ) and ( 'error' in status[1].lower() or \
             'not found' in status[1].lower() or \
             'exited with status' in status[1].lower() ):
          failed_time = status[3]
          return failed_time, status[1]
    except TypeError:
      return None, None


def getParameters( localjobID ):
  """ get the parameters of the JOB """
  result = dirac.parameters( localjobID, printOutput = False )
  if result['OK']:
    try:
      localnode = result['Value']['HostName']
      walltime = result['Value']['WallClockTime(s)']
      cputime = result['Value']['TotalCPUTime(s)']
      localmemory = result['Value']['Memory(kB)']
      localeff = float( cputime ) / float( walltime )
      return localnode, localeff, localmemory
    except TypeError:
      return None, None, None


if args:
  usage()

exitCode = 0

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "status":
    wmsStatus = switch[1]
  elif switch[0].lower() == "minorstatus":
    minorStatus = switch[1]
  elif switch[0].lower() == "applicationstatus":
    appStatus = switch[1]
  elif switch[0].lower() == "site":
    site = switch[1]
  elif switch[0].lower() == "owner":
    owner = switch[1]
  elif switch[0].lower() == "jobgroup":
    jobGroup = switch[1]
  elif switch[0].lower() == "date":
    date = switch[1]
  elif switch[0].lower() == "verbose":
    verbose = switch[1]

selDate = date
if not date:
  selDate = 'Today'

dirac = Dirac()

print '''* This script collates information about job error messages.
* The following job IDs meet the criteria specified on the command line.
* If the jobs failed at the pilot stage (i.e.,failure to download an
input sandbox or issues with LFN resolution) then there will be no job
logging information available to summarise.
'''

jobIDs = getJobMetadata( wmsStatus, minorStatus, appStatus, site, owner, jobGroup, date, dirac )
error2Nodes = {}
node2Errors = {}
error2Users = {}
user2Errors = {}
efficiencies = []

print 'Number of jobs that pass the above criteria: %d' % len( jobIDs )

for jobID in jobIDs:
  print jobID
  node, eff, memory = getParameters( int( jobID ) )
  logging = getLogging( int( jobID ) )
  user = getAttributes( int( jobID ) )

  if not logging:
    continue

  if eff:
    efficiencies.append( eff )
    eff = '%0.2f' % eff

  try:
    if error2Nodes.has_key( logging[1] ):
      error2Nodes[ logging[1] ].append( node )
    else:
      error2Nodes[ logging[1] ] = [ node ]

    if node2Errors.has_key( node ):
      node2Errors[ node ].append( logging[1] )
    else:
      node2Errors[ node ] = [ logging[1]]

    if error2Users.has_key( logging[1] ):
      error2Users[ logging[1] ].append( user )
    else:
      error2Users[ logging[1] ] = [ user ]

    if user2Errors.has_key( user ):
      user2Errors[ user ].append( logging[1] )
    else:
      user2Errors[ user ] = [ logging[1]]

    if verbose:
      print jobID, node, logging[0], logging[1], eff, memory
  except Exception, e:
    print e

print
print '##################################'
print '# Error-Node summary information #'
print '##################################'
print "%s %s %s" % ( "Error".ljust( 50 ), "Occurences".ljust( 12 ), "Unique nodes".ljust( 15 ) )
for error in sorted( error2Nodes.keys() ):
  nodes = error2Nodes[error]
  print "%s %s %s" % ( error.ljust( 50 ), str( len( nodes ) ).ljust( 12 ), str( len( set( nodes ) ) ).ljust( 15 ) )

print
print '##################################'
print '# Error-User summary information #'
print '##################################'
print "%s %s %s" % ( "Error".ljust( 50 ), "Occurences".ljust( 12 ), "Unique users".ljust( 15 ) )
for error in sorted( error2Users.keys() ):
  users = error2Users[error]
  print "%s %s %s" % ( error.ljust( 50 ), str( len( users ) ).ljust( 12 ), str( len( set( users ) ) ).ljust( 15 ) )

print
print '###################################'
print '# Node-Error summary information  #'
print '###################################'
print "%s %s %s" % ( "Node".ljust( 50 ), "Errors".ljust( 12 ), "Unique errors".ljust( 15 ) )
for node in sorted( node2Errors.keys() ):
  errors = node2Errors[node]
  if node:
    print "%s %s %s" % ( node.ljust( 50 ), str( len( errors ) ).ljust( 12 ), str( len( set( errors ) ) ).ljust( 15 ) )
  else:
    print "%s %s %s" % ( ''.ljust( 50 ), str( len( errors ) ).ljust( 12 ), str( len( set( errors ) ) ).ljust( 15 ) )

print
print '###################################'
print '# User-Error summary information  #'
print '###################################'
print "%s %s %s" % ( "User".ljust( 50 ), "Errors".ljust( 12 ), "Unique errors".ljust( 15 ) )
for user in sorted( user2Errors.keys() ):
  errors = user2Errors[user]
  print "%s %s %s" % ( user.ljust( 50 ), str( len( errors ) ).ljust( 12 ), str( len( set( errors ) ) ).ljust( 15 ) )

n = len( efficiencies )
if n != 0:
  average = sum( efficiencies ) / len( efficiencies )
  tmp = 0.0
  for eff in efficiencies:
    tmp += ( eff - average ) ** 2
  stddev = ( tmp / len( efficiencies ) ) ** 0.5

  print '\nAverage CPU efficiency is %0.2f' % average
  print 'Standard Deviation of CPU efficiency is %0.2f' % stddev

DIRAC.exit( 0 )
