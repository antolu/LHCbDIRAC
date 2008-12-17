#! /usr/bin/env python
import os, sys
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/scripts/dirac-lhcb-job-logging-check.py,v 1.2 2008/12/17 15:21:40 gcowan Exp $
# File :   dirac-lhcb-job-logging-check
# Author : Greig A Cowan
########################################################################
__RCSID__   = "$Id: dirac-lhcb-job-logging-check.py,v 1.2 2008/12/17 15:21:40 gcowan Exp $"
__VERSION__ = "$Revision: 1.2 $"
import sys,string, pprint
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.Dirac import Dirac

Script.registerSwitch( "", "Status=", "Primary status" )
Script.registerSwitch( "", "MinorStatus=", "Secondary status" )
Script.registerSwitch( "", "ApplicationStatus=", "Application status" )
Script.registerSwitch( "", "Site=", "Execution site" )
Script.registerSwitch( "", "Owner=", "Owner (DIRAC nickname)" )
Script.registerSwitch( "", "Date=", "Date (YYYY-MM-DD)" )
Script.registerSwitch( "", "JobGroup=", "Select jobs for specified job group" )
Script.registerSwitch( "", "Verbose=", "For more detailed information about file and job states. Default False." )
Script.addDefaultOptionValue( "LogLevel", "ALWAYS" )
Script.parseCommandLine( ignoreErrors = True )

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

def getJobMetadata( wmsStatus, minorStatus, appStatus, site, owner, date, jobGroup, dirac):
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

def getLogging( jobID):
  result = dirac.loggingInfo(jobID,printOutput=False)
  
  if result['OK']:
    try:
      for status in result['Value']:
        if ( status[0] == 'Running' ) and ('error' in status[2].lower() or \
             'found' in status[2].lower() or \
             'exited' in status[2].lower()):
          failed_time = status[3]
          return failed_time, status[2]
        elif ( status[0] == 'Completed' ) and ('error' in status[1].lower() or \
             'found' in status[1].lower() or \
             'exited' in status[1].lower()):
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

jobIDs = getJobMetadata( wmsStatus, minorStatus, appStatus, site, owner, date, jobGroup, dirac)
error2Nodes = {}
node2Errors = {}
efficiencies = []
for jobID in jobIDs:
  node, eff, memory = getParameters( int(jobID))
  logging = getLogging( int(jobID))

  if eff:
    efficiencies.append( eff)
    eff = '%0.2f' % eff
  
  try:
    if error2Nodes.has_key( logging[1] ):
      error2Nodes[ logging[1] ].append( node)
    else:
      error2Nodes[ logging[1] ] = [ node ] 

    if node2Errors.has_key( node):
      node2Errors[ node ].append( node)
    else:
      node2Errors[ node ] = [ logging[1]]  

    if verbose:
      print jobID, node, logging[0], logging[1], eff, memory
  except Exception, e:
    print e

print '\nError summary information\n'
for error, nodes in error2Nodes.iteritems():
  print error, 'occurred on', len(nodes), 'nodes'

print '\nNode summary information\n'
for node, errors in node2Errors.iteritems():
  print node, 'had', len(errors), 'errors, of which', len(set(errors)), 'were unique'

average = sum( efficiencies)/len( efficiencies)
tmp = 0.0
for eff in efficiencies:
  tmp += (eff - average)**2
stddev = ( tmp/ len(efficiencies))**0.5

print '\nAverage CPU efficiency is %0.2f' % average
print 'Standard Deviation of CPU efficiency is %0.2f' % stddev

DIRAC.exit( 0 )
