#! /usr/bin/env python
import os, sys
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/scripts/dirac-lhcb-job-logging-check.py,v 1.5 2009/03/09 18:29:45 gcowan Exp $
# File :   dirac-lhcb-job-logging-check
# Author : Greig A Cowan
########################################################################
__RCSID__   = "$Id: dirac-lhcb-job-logging-check.py,v 1.5 2009/03/09 18:29:45 gcowan Exp $"
__VERSION__ = "$Revision: 1.5 $"
import sys,string, pprint
from DIRACEnvironment import DIRAC
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
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

args = Script.getPositionalArgs()

wmsStatus=None
minorStatus=None
appStatus=None
site=None
owner=None
jobGroup=None
date=None
verbose=False

def usage():
  print 'Usage: %s [Try -h,--help for more information]' %(Script.scriptName)
  DIRAC.exit(2)

def getJobMetadata( wmsStatus, minorStatus, appStatus, site, owner, jobGroup, date, dirac):
  '''Gets information about jobs from the WMS'''
  # getJobStates
  result_wms = dirac.selectJobs( Status = wmsStatus,
                                 MinorStatus = minorStatus,
                                 ApplicationStatus = appStatus,
                                 Site = site,
                                 Owner = owner,
                                 JobGroup = jobGroup,
                                 Date = date
                                 )

  if not result_wms['OK']:
    print 'ERROR %s' % result_wms['Message']
    DIRAC.exit( 2)
  else:
    # create list of jobIDs in this state belonging to this production
    jobIDs = result_wms['Value']
    
  return jobIDs

def getAttributes( jobID):
  result = dirac.attributes( jobID, printOutput=False)
  if result['OK']:
    return result['Value']['Owner']

def getLogging( jobID):
  result = dirac.loggingInfo(jobID,printOutput=False)
  if result['OK']:
    try:
      for status in result['Value']:
        if ( status[0] == 'Running' ) and ('error' in status[2].lower() or \
             'not found' in status[2].lower() or \
             'exited with status' in status[2].lower()):
          failed_time = status[3]
          return failed_time, status[2]
        elif ( status[0] == 'Completed' ) and ('error' in status[1].lower() or \
             'not found' in status[1].lower() or \
             'exited with status' in status[1].lower()):
          failed_time = status[3]
          return failed_time, status[1]
    except:
      return None, None


def getParameters( jobID):
  result = dirac.parameters(jobID,printOutput=False)  
  if result['OK']:
    try:
      node = result['Value']['HostName']
      walltime = result['Value']['WallClockTime(s)']
      cputime = result['Value']['TotalCPUTime(s)']
      memory = result['Value']['Memory(kB)'] 
      eff = float(cputime)/float(walltime)
      return node, eff, memory
    except:
      return None, None, None
    

if args:
  usage()

exitCode = 0

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower()=="status":
    wmsStatus=switch[1]
  elif switch[0].lower()=="minorstatus":
    minorStatus=switch[1]
  elif switch[0].lower()=="applicationstatus":
    appStatus=switch[1]
  elif switch[0].lower()=="site":
    site=switch[1]
  elif switch[0].lower()=="owner":
    owner=switch[1]
  elif switch[0].lower()=="jobgroup":
    jobGroup=switch[1]
  elif switch[0].lower()=="date":
    date=switch[1]
  elif switch[0].lower()=="verbose":
    verbose=switch[1]

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

jobIDs = getJobMetadata( wmsStatus, minorStatus, appStatus, site, owner, jobGroup, date, dirac)
error2Nodes = {}
node2Errors = {}
error2Users = {}
user2Errors = {}
efficiencies = []

print 'Number of jobs that pass the above criteria: %d' % len(jobIDs)

for jobID in jobIDs:
  print jobID
  node, eff, memory = getParameters( int(jobID))
  logging = getLogging( int(jobID))
  user = getAttributes( int(jobID))
  
  if not logging:
    continue

  if eff:
    efficiencies.append( eff)
    eff = '%0.2f' % eff
  
  try:
    if error2Nodes.has_key( logging[1] ):
      error2Nodes[ logging[1] ].append( node)
    else:
      error2Nodes[ logging[1] ] = [ node ] 

    if node2Errors.has_key( node):
      node2Errors[ node ].append( logging[1])
    else:
      node2Errors[ node ] = [ logging[1]]  

    if error2Users.has_key( logging[1] ):
      error2Users[ logging[1] ].append( user)
    else:
      error2Users[ logging[1] ] = [ user ] 

    if user2Errors.has_key( user):
      user2Errors[ user ].append( logging[1])
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
for error, nodes in error2Nodes.iteritems():
  print error, '\toccurred on', len(nodes), 'nodes, of which', len(set(nodes)), 'were unique'

print
print '##################################'
print '# Error-User summary information #'
print '##################################'
for error, users in error2Users.iteritems():
  print error, '\toccurred for', len(users), 'users, of which', len(set(users)), 'were unique'


print
print '###################################'
print '# Node-Error summary information  #'
print '###################################'
for node, errors in node2Errors.iteritems():
  print node, '\thad', len(errors), 'errors, of which', len(set(errors)), 'were unique'

print
print '###################################'
print '# User-Error summary information  #'
print '###################################'
for user, errors in user2Errors.iteritems():
  print user, '\t\thad', len(errors), 'errors, of which', len(set(errors)), 'were unique'

n = len(efficiencies)
if n != 0:
  average = sum( efficiencies)/len( efficiencies)
  tmp = 0.0
  for eff in efficiencies:
    tmp += (eff - average)**2
  stddev = ( tmp/ len(efficiencies))**0.5

  print '\nAverage CPU efficiency is %0.2f' % average
  print 'Standard Deviation of CPU efficiency is %0.2f' % stddev

DIRAC.exit( 0 )
