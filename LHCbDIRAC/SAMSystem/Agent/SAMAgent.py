########################################################################
# $HeadURL$
########################################################################

__RCSID__ = "$Id: SAMAgent.py 18857 2009-12-02 12:07:41Z atsareg $"

"""  SAM Agent submits SAM jobs
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from LHCbDIRAC.SAMSystem.Client.DiracSAM import DiracSAM

from DIRAC.Core.Utilities import shellCall
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Core.Utilities import List

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC  import gMonitor

import os, time

AGENT_NAME = "SAM/SAMAgent"

class SAMAgent( AgentModule ):

  def initialize( self ):
    self.pollingTime = self.am_getOption( 'PollingTime', 3600 * 6 )
    gLogger.info( "PollingTime %d hours" % ( int( self.pollingTime ) / 3600 ) )

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/SAMManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'SAMManager' )

    gMonitor.registerActivity( "TotalSites", "Total Sites", "SAMAgent", "Sites", gMonitor.OP_SUM, 3600 * 2 )
    gMonitor.registerActivity( "ActiveSites", "Active Sites", "SAMAgent", "Sites", gMonitor.OP_SUM, 3600 * 2 )
    gMonitor.registerActivity( "BannedSites", "Banned Sites", "SAMAgent", "Sites", gMonitor.OP_SUM, 3600 * 2 )
    gMonitor.registerActivity( "DeletedJobs", "Deleted Jobs", "SAMAgent", "Jobs", gMonitor.OP_SUM, 3600 * 2 )

    return S_OK()

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

    return S_OK()
