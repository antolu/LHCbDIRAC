#! /usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-production-job-select-check.py
# Author :  Stuart Paterson
########################################################################
"""
  Select jobs with given conditions and check the status of: LFC, BK and prodDB
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "", "Status=", "Primary status" )
Script.registerSwitch( "", "MinorStatus=", "Secondary status" )
Script.registerSwitch( "", "ApplicationStatus=", "Application status" )
Script.registerSwitch( "", "Site=", "Execution site" )
Script.registerSwitch( "", "Owner=", "Owner (DIRAC nickname)" )
Script.registerSwitch( "", "JobGroup=", "Select jobs for specified job group" )
Script.registerSwitch( "", "Date=", "Date in YYYY-MM-DD format, if not specified default is today" )
Script.registerSwitch( "", "JobID=", "Select job with jobID" )
Script.registerSwitch( "", "Verbose", "Verbosity" )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )

from DIRAC.Interfaces.API.Dirac import Dirac

from LHCbDIRAC.Core.Utilities.JobInfoFromXML        import JobInfoFromXML
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

rm = ReplicaManager()
bk = BookkeepingClient()

from DIRAC.Core.DISET.RPCClient                     import RPCClient

prodClient = RPCClient( 'Transformation/TransformationManager' )

args = Script.getPositionalArgs()
if args:
  Script.showHelp()

#Default values
status = None
minorStatus = None
appStatus = None
site = None
owner = None
jobGroup = None
date = None
jobID = None
verbose = False

def printDict( dictionary ):
  """ Dictionary pretty printing
  """
  key_max = 0
  value_max = 0
  for key, value in dictionary.items():
    if len( key ) > key_max:
      key_max = len( key )
    if len( str( value ) ) > value_max:
      value_max = len( str( value ) )
  for key, value in dictionary.items():
    print key.rjust( key_max ), ' : ', str( value ).ljust( value_max )


exitCode = 0

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "status":
    status = switch[1]
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
  elif switch[0].lower() == "jobid":
    jobID = switch[1]
  elif switch[0].lower() == "verbose":
    verbose = True


if jobID:
  jobs = [jobID, ]
  conds = ["JobID = %s" % jobID, ]
else:
  selDate = date
  if not date:
    selDate = 'Today'
  conditions = {'Status':status, 'MinorStatus':minorStatus, 'ApplicationStatus':appStatus, 'Owner':owner, 'JobGroup':jobGroup, 'Date':selDate}
  dirac = Dirac()
  result = dirac.selectJobs( Status = status, MinorStatus = minorStatus, ApplicationStatus = appStatus, Site = site, Owner = owner, JobGroup = jobGroup, Date = date )

  if not result['OK']:
    print 'ERROR %s' % result['Message']
    exitCode = 2
    DIRAC.exit( exitCode )

  conds = []
  for n, v in conditions.items():
    if v:
      conds.append( '%s = %s' % ( n, v ) )
  jobs = result['Value']


print '==> Selected %s jobs with conditions: %s' % ( len( jobs ), ', '.join( conds ) )
print ', '.join( jobs )

jobsOK = []
jobsLFC = []
jobsBK = []
jobsPROD = []


for job in jobs:
  jobinfo = JobInfoFromXML( job )
  result = jobinfo.valid()
  if not result['OK']:
    print result['Message']
    continue

  result = jobinfo.getOutputLFN()
  lfns = result['Value']

  #Check LFC
  replicas = rm.getReplicas( lfns )
  okLFC = False
  if not replicas['OK']:
    print replicas['Message']
  else:
    value = replicas['Value']
    successful = value.get( 'Successful', [] )
    failed = value.get( 'Failed', [] )
    if verbose:
      print "LFC replicas:"
      if len( successful ):
        print "Successful:"
        printDict( successful )
      if len( failed ):
        print "Failed:"
        printDict( failed )
    if len( successful ) == len( lfns ):
      okLFC = True

  #Check Bookkeeping
  bkresponce = bk.exists( lfns )
  okBK = False
  if not bkresponce['OK']:
    print bkresponce['Message']
  else:
    bkvalue = bkresponce['Value']
    if verbose:
      print "Bookkeeping:"
      printDict( bkvalue )
    count = 0
    for value in bkvalue.itervalues():
      if value:
        count += 1
    if count == len( lfns ):
      okBK = True

  #Check ProductionDB

  result = jobinfo.getInputLFN()
  lfns = result['Value']
  okPROD = True
  if len( lfns ):
    prodid = int( jobinfo.prodid )
    if verbose:
      print 'ProductionDB for production %d' % prodid
    fs = prodClient.getFileSummary( lfns, prodid )
    if fs['OK']:
      for lfn in lfns:
        try:
          status = fs['Value']['Successful'][lfn][prodid]
	  if not status['FileStatus'].count( 'Processed' ):
	    okPROD = False
	  if verbose:
	    print status
	except:
	  okPROD = False
  else:
    print fs['Message']

  if okLFC and okBK and okPROD:
    jobsOK.append( job )
  if not okLFC:
    jobsLFC.append( job )
  if not okBK:
    jobsBK.append( job )
  if not okPROD:
    jobsPROD.append( job )


print "OK: %i job(s)" % len( jobsOK )
print jobsOK
print "Problem with LFC: %i job(s)" % len( jobsLFC )
print jobsLFC
print "Problem with Bookkeeping: %i job(s)" % len( jobsBK )
print jobsBK
print "Problem with ProductionDB: %i job(s)" % len( jobsPROD )
print jobsPROD

DIRAC.exit( exitCode )
