#!/usr/bin/env python
########################################################################
# File :    dirac-wms-get-wn
# Author :  Philippe Charpentier
########################################################################
"""
  Get WNs for a selection of jobs
"""
__RCSID__ = "$Id: dirac-bookkeeping-file-metadata.py 65177 2013-04-22 15:24:07Z phicharp $"
import  DIRAC.Core.Base.Script as Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript


if __name__ == "__main__":
  site = 'DIRAC.BOINC.ch'
  status = ["Running"]
  workerNode = None
  since = None
  date = 'today'
  full = False
  Script.registerSwitch( '', 'Site=', '   Select site (default: %s)' % site )
  Script.registerSwitch( '', 'Status=', '   Select status (default: %s)' % status )
  Script.registerSwitch( '', 'WorkerNode=', '  Select WN' )
  Script.registerSwitch( '', 'Since=', '   Date since when to select jobs, or number of days (default: today)' )
  Script.registerSwitch( '', 'Full', '   Printout full list of job (default: False except if --WorkerNode)' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... LFN|File' % Script.scriptName] ) )
  Script.parseCommandLine()
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'Site':
      site = switch[1]
    elif switch[0] == 'Status':
      status = switch[1].split( ',' )
    elif switch[0] == 'WorkerNode':
      workerNode = switch[1]
    elif switch[0] == 'Full':
      full = True
    elif switch[0] == 'Since':
      import datetime
      date = switch[1].lower()
      if date == 'today':
        since = None
      elif date == 'yesterday':
        since = 1
      elif date == 'ever':
        since = 2 * 365
      elif date.isdigit():
        since = int( date )
        date += ' days'
      else:
        since = date
      if type( since ) == type( 0 ):
        since = str( datetime.datetime.now() - datetime.timedelta( days = since ) ).split()[0]


  if workerNode:
    status = [None]
    full = True

  import DIRAC
  from DIRAC import gLogger
  from DIRAC.Interfaces.API.Dirac import Dirac
  from DIRAC.Core.DISET.RPCClient                          import RPCClient
  monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
  dirac = Dirac()

  # Get jobs according to selection
  jobs = []
  for stat in status:
    res = dirac.selectJobs( site = site, date = since, status = stat )
    if not res['OK']:
      gLogger.error( 'Error selectin jobs', res['Message'] )
      DIRAC.exit( 1 )
    jobs += [int( job ) for job in res['Value']]
  if not jobs:
    gLogger.always( 'No jobs found...' )
    DIRAC.exit( 0 )
  # res = monitoring.getJobsSummary( jobs )
  # print eval( res['Value'] )[jobs[0]]

  allJobs = []
  result = {}
  wnJobs = {}
  # Get host name
  for job in jobs:
    res = monitoring.getJobParameter( job, 'HostName' )
    if res['OK']:
        node = res['Value'].get( 'HostName', 'Unknown' )
        if workerNode:
          if workerNode != node:
            continue
          allJobs.append( job )
        result.setdefault( job, {} )['Status'] = status
        result[job]['Node'] = node
        wnJobs[node] = wnJobs.setdefault( node, 0 ) + 1

  # If necessary get jobs' status
  if allJobs:
    res = monitoring.getJobsStatus( allJobs )
    if res['OK']:
      jobStatus = res['Value']
      res = monitoring.getJobsMinorStatus( allJobs )
      if res['OK']:
        jobMinorStatus = res['Value']
        res = monitoring.getJobsApplicationStatus( allJobs )
        if res['OK']:
          jobApplicationStatus = res['Value']
    if not res['OK']:
      gLogger.error( 'Error getting job parameter', res['Message'] )
    else:
      for job in allJobs:
        status = jobStatus.get( job, {} ).get( 'Status', 'Unknown' ) + '; ' + \
                 jobMinorStatus.get( job, {} ).get( 'MinorStatus', 'Unknown' ) + '; ' + \
                 jobApplicationStatus.get( job, {} ).get( 'ApplicationStatus', 'Unknown' )
        result[job]['Status'] = status
  else:
    allJobs = jobs

  # Print out result
  if workerNode:
    gLogger.always( 'Found %d jobs at %s, WN %s (since %s):' % ( len( allJobs ), site, workerNode, date ) )
    gLogger.always( 'List of jobs:', ','.join( [str( job ) for job in allJobs] ) )
  else:
    gLogger.always( 'Found %d jobs %s at %s (since %s):' % ( len( allJobs ), status, site, date ) )
    gLogger.always( 'List of WNs:', ','.join( ['%s (%d)' % ( node, wnJobs[node] )
                                               for node in sorted( wnJobs,
                                                                   cmp = ( lambda n1, n2: ( wnJobs[n2] - wnJobs[n1] ) ) )] ) )
  if full:
    for job in sorted( allJobs, reverse = True ):
      status = result[job]['Status']
      if workerNode:
        gLogger.always( '%s %s' % ( job, status ) )
      else:
        node = result[job]['Node']
        gLogger.always( '%s %s at %s' % ( job, status, node ) )
