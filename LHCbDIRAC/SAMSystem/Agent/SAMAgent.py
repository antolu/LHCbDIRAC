########################################################################
# $HeadURL$
########################################################################

__RCSID__ = "$Id: SAMAgent.py 18857 2009-12-02 12:07:41Z atsareg $"

"""  SAM Agent submits SAM jobs
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from LHCbDIRAC.SAMSystem.Client.DiracSAM import DiracSAM

from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.Core.Utilities import shellCall
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Utilities import List

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC  import gMonitor

import os, time

AGENT_NAME = "SAM/SAMAgent"

class SAMPublisher:

  def __init__( self ):
    self.Script = None
    self.samPublishClient = os.getenv( 'DIRAC', '/opt/dirac/pro' ) + '/LHCbDIRAC/SAMSystem/Distribution/sam.tar.gz'

  def install( self, dest = None ):
    cmd = 'tar -zxvf %s' % ( self.samPublishClient )
    if dest:
      gLogger.info( "Publisher", "Try to install in %s " % ( dest ) )
      if not os.path.isdir( dest ):
        return S_ERROR( "Publisher", "No such directory: %s" % ( dest ) )
      cmd += " --directory %s" % ( dest )
    res = shellCall( 0, cmd )
    if not res['OK']:
      return res
    if res['Value'][0]:
      return S_ERROR( res['Value'][2] )
    if dest:
      dirname = dest
    else:
      dirname = os.getcwd()
    self.Script = os.path.join( dirname, "sam/bin/same-publish-tuples" )
    return S_OK()

  def publish( self, testName, ce, status, detailedData = None ):

    if not self.Script:
      return S_ERROR( "SAMPublisher is not installed" )

    if not detailedData:
      detailedData = 'EOT\n<br>\nEOT'

    timeStr = time.strftime( "%Y-%m-%d-%H-%M-%S", time.gmtime() )

    defFile = "testName: %(TestName)s\ntestAbbr: LHCb %(TestName)s\ntestTitle: LHCb SAM %(TestName)s\nEOT\n" % {'TestName':testName}
    envFile = "envName: CE-%s\nname: LHCbSAMTest\nvalue: OK\n" % ( timeStr )
    resultFile = "nodename: %s\ntestname: %s\nenvName: CE-%s\nvoname: lhcb\nstatus: %d\ndetaileddata: %s\n" % ( ce, testName, timeStr, status, detailedData )

    publishFlag = True

    for test, cont in ( ( 'TestDef', defFile ), ( 'TestEnvVars', envFile ), ( 'TestData', resultFile ) ):
      fname = os.tmpnam()
      fopen = open( fname, 'w' )
      fopen.write( cont )
      fopen.close()
      cmd = '%s %s %s' % ( self.Script, test, fname )
      result = shellCall( 0, cmd )
      if not result['OK']:
        gLogger.warn( "Publishing %s" % ( test ), result['Message'] )
        publishFlag = False
      elif result['Value'][0]:
        gLogger.warn( "Publishing %s" % ( test ), result['Value'][2] )
        publishFlag = False

      os.remove( fname )

    if publishFlag:
      return S_OK()
    else:
      return S_ERROR( "Publishing error" )

class SAMAgent( AgentModule ):

  def initialize( self ):
    self.pollingTime = self.am_getOption( 'PollingTime', 3600 * 6 )
    gLogger.info( "PollingTime %d hours" % ( int( self.pollingTime ) / 3600 ) )

    self.useProxies = self.am_getOption( 'UseProxies', 'True' ).lower() in ( "y", "yes", "true" )
    self.proxyLocation = self.am_getOption( 'ProxyLocation', '' )
    if not self.proxyLocation:
      self.proxyLocation = False

    gMonitor.registerActivity( "TotalSites", "Total Sites", "SAMAgent", "Sites", gMonitor.OP_SUM, 3600 * 2 )
    gMonitor.registerActivity( "ActiveSites", "Active Sites", "SAMAgent", "Sites", gMonitor.OP_SUM, 3600 * 2 )
    gMonitor.registerActivity( "BannedSites", "Banned Sites", "SAMAgent", "Sites", gMonitor.OP_SUM, 3600 * 2 )
    gMonitor.registerActivity( "DeletedJobs", "Deleted Jobs", "SAMAgent", "Jobs", gMonitor.OP_SUM, 3600 * 2 )

    self.samPub = SAMPublisher()

    result = self.samPub.install()
    if not result['OK']:
      gLogger.error( "SAM publisher installation", result['Message'] )

    if self.useProxies:
      self.am_setModuleParam( "shifterProxy", "SAMManager" )

    return result

  def execute( self ):

    gLogger.debug( "Executing %s" % ( AGENT_NAME ) )

    self._clearSAMjobs()
    self._siteAccount()

    diracSAM = DiracSAM()

    result = diracSAM.submitAllSAMJobs( False ) # Forbidden software installation
    if not result['OK']:
      gLogger.error( result['Message'] )

    return result

  def _siteAccount( self ):

    gLogger.debug( "Executing %s" % ( AGENT_NAME ) )

    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
    result = wmsAdmin.getSiteMask()
    if not result[ 'OK' ]:
      self.log.error( "Can't get SiteMask: %s" % result[ 'Message' ] )
      return result
    sitesmask = result['Value']
    numsitesmask = len( sitesmask )

    result = gConfig.getSections( '/Resources/Sites/LCG' )
    if not result[ 'OK' ]:
      self.log.error( "Can't get Sites: %s" % result[ 'Message' ] )
      return result
    sites = result['Value']
    numsites = len( sites )

    gMonitor.addMark( "TotalSites", numsites )
    gMonitor.addMark( "ActiveSites", numsitesmask )
    gMonitor.addMark( "BannedSites", numsites - numsitesmask )
    gMonitor.addMark( "DeletedJobs", self.deletedJobs )

    self.log.verbose( "Site Account finished" )

    return S_OK()

  def _clearSAMjobs( self, days = 2 ):

    gLogger.info( "Clear SAM jobs for last %d day(s)" % days )
    dirac = Dirac()

    samPub = self.samPub
    testName = 'CE-lhcb-availability'


    sites = gConfig.getSections( '/Resources/Sites/LCG' )['Value']

    allces = []
    jobIDs = []
    conditions = {}
    conditions['JobGroup'] = 'SAM'

    days_ago = time.gmtime( time.time() - 60 * 60 * 24 * days )
    Date = '%s-%s-%s' % ( days_ago[0], str( days_ago[1] ).zfill( 2 ), str( days_ago[2] ).zfill( 2 ) )

    for site in sites:

      opt = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )['Value']
      name = opt.get( 'Name', '' )
      ces = List.fromChar( opt.get( 'CE', '' ) )
      allces += ces

      conditions['Site'] = site
      monitoring = RPCClient( 'WorkloadManagement/JobMonitoring', timeout = 120 )

      result = monitoring.getJobs( conditions, Date )
      if not result['OK']:
        gLogger.warn( "Error get Jobs for Site %s" % ( site ), result['Message'] )
        continue
      gLogger.debug( "Jobs for site %s" % site, repr( result['Value'] ) )
      jobIDs += result['Value']

    ceOldWaitingJobs = {}
    ceNewStartedJobs = {}

    startedStatus = ( "Done", "Failed", "Completed", "Running" )

    for j in jobIDs:

      result = monitoring.getJobAttributes( int( j ) )
      if not result['OK']:
        gLogger.warn( "getJobAttributes", result['Message'] )
        continue

      attr = result['Value']
      ce = attr["JobName"].replace( "SAM-", "" )
      submitTime = time.strptime( attr["SubmissionTime"] + "[UTC]", "%Y-%m-%d %H:%M:%S[%Z]" )
      deltat = time.time() - time.mktime( submitTime )
      dayAndNight = bool( int( deltat ) / ( 60 * 60 * 24 ) )
      status = attr['Status']
      gLogger.debug( "Job %s from CE %s %s" % ( j, ce, status ) )

      if dayAndNight and status == 'Waiting':
        ceOldWaitingJobs.setdefault( ce, [] ).append( j )

      if not dayAndNight and status in startedStatus:
        ceNewStartedJobs.setdefault( ce, [] ).append( j )

    oldWaitingJobs = []
    for jobs in ceOldWaitingJobs.values():
      oldWaitingJobs.extend( jobs )
    for job in oldWaitingJobs:
      gLogger.debug( "Kill Old SAM Job", repr( job ) )
      dirac.delete( int( job ) )
    gLogger.info( "%s:" % ( AGENT_NAME ), " %d SAM jobs were deleted" % ( len( oldWaitingJobs ) ) )
    self.deletedJobs = len( oldWaitingJobs )

    ceOldWaitingJobs = ceOldWaitingJobs.keys()
    ceNewStartedJobs = ceNewStartedJobs.keys()
    gLogger.info( "ceOldWaitingJobs", repr( ceOldWaitingJobs ) )
    gLogger.info( "ceNewStartedJobs", repr( ceNewStartedJobs ) )

    for ce in allces:
      status = 0
      if ( ce in ceNewStartedJobs ) and ( ce in ceOldWaitingJobs ):
        status = 40
      if ( ce in ceNewStartedJobs ) and not ( ce in ceOldWaitingJobs ):
        status = 10
      if not ( ce in ceNewStartedJobs ) and ( ce in ceOldWaitingJobs ):
        status = 50

      gLogger.verbose( "CE status:", "%s %d" % ( ce, status ) )

      res = samPub.publish( testName, ce, status )
      if not res['OK']:
        gLogger.warn( "SAM publisher error for CE %s" % ce, res['Message'] )



    return S_OK()
