""" SAM Agent submits SAM jobs
"""

import time

from datetime import date, timedelta

from DIRAC                       import gConfig, S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient  import RPCClient
from DIRAC.Core.Utilities        import List
from DIRAC.Interfaces.API.Dirac  import Dirac

from LHCbDIRAC.ResourceStatusSystem.Client.DiracSAM import DiracSAM

__RCSID__ = "$Id$"
AGENT_NAME = 'SAM/SAMAgent'

class SAMAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):

    AgentModule.__init__( self, *args, **kwargs )

    self.days = 2

  def initialize( self ):

    self.days = self.am_getOption( 'days', self.days )

    self.am_setOption( 'shifterProxy', 'TestManager' )

#    self.monitor.registerActivity( 'TotalSites',  'Total Sites',  'SAMAgent', 'Sites', _operation, _bucket )
#    self.monitor.registerActivity( 'ActiveSites', 'Active Sites', 'SAMAgent', 'Sites', _operation, _bucket )
#    self.monitor.registerActivity( 'BannedSites', 'Banned Sites', 'SAMAgent', 'Sites', _operation, _bucket )
#    self.monitor.registerActivity( 'DeletedJobs', 'Deleted Jobs', 'SAMAgent', 'Jobs',  _operation, _bucket )

    return S_OK()

  def execute( self ):

    res = self._clearSAMjobs()
    if not res[ 'OK' ]:
      self.log.error( res[ 'Message' ] )
      return res

    diracSAM = DiracSAM()

    ceSites = diracSAM.getSuitableCEs()
    if not ceSites[ 'OK' ]:
      self.log.error( ceSites[ 'Message' ] )
      return ceSites
    ceSites = ceSites[ 'Value' ]

    for ce, site in ceSites:
      result = diracSAM.submitNewSAMJob( ce = ce, site = site )
      if not result[ 'OK' ]:
        self.log.error( 'Submission to CE %s failed with message %s' % ( ce, result[ 'Message' ] ) )

    return result

  def _clearSAMjobs( self ):
    """ Clear SAM jobs older than <self.days> days
    """

    self.log.info( "Clear SAM jobs for last %d day(s)" % self.days )
    dirac = Dirac()

    allces     = []
    jobIDs     = []
    conditions = { 'JobGroup' : 'Test' }

    sites = gConfig.getSections( '/Resources/Sites/LCG' )
    if not sites[ 'OK' ]:
      return sites
    sites = sites[ 'Value' ]

    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    cutDate = ( date.today() - timedelta( days = self.days ) ).strftime( '%Y-%m-%d' )

    for site in sites:

      opt = gConfig.getOptionsDict( '/Resources/Sites/LCG/%s' % site )['Value']
      ces = List.fromChar( opt.get( 'CE', '' ) )
      allces += ces

      conditions[ 'Site' ] = site

      result = monitoring.getJobs( conditions, cutDate )

      if not result[ 'OK' ]:
        self.log.warn( "Error getJobs for site %s. %s" % ( site, result[ 'Message' ] ) )
        continue

      self.log.debug( "Jobs for site %s" % site, repr( result[ 'Value' ] ) )
      jobIDs += result[ 'Value' ]

    ceOldWaitingJobs = {}
    ceNewStartedJobs = {}

    startedStatus = ( "Done", "Failed", "Completed", "Running" )

    for j in jobIDs:

      result = monitoring.getJobAttributes( int( j ) )
      if not result[ 'OK' ]:
        self.log.warn( 'getJobAttributes %s' % result[ 'Message' ] )
        continue

      attr        = result[ 'Value' ]
      ce          = attr[ 'JobName' ].replace( "SAM-", "" )
      submitTime  = time.strptime( attr[ "SubmissionTime" ] + "[UTC]", "%Y-%m-%d %H:%M:%S[%Z]" )
      deltat      = time.time() - time.mktime( submitTime )
      dayAndNight = bool( int( deltat ) / ( 60 * 60 * 24 ) )
      status      = attr[ 'Status' ]
      self.log.debug( "Job %s from CE %s %s" % ( j, ce, status ) )

      if dayAndNight and status == 'Waiting':
        ceOldWaitingJobs.setdefault( ce, [] ).append( j )

      if not dayAndNight and status in startedStatus:
        ceNewStartedJobs.setdefault( ce, [] ).append( j )

    oldWaitingJobs = []
    for jobs in ceOldWaitingJobs.itervalues():
      oldWaitingJobs.extend( jobs )
    for job in oldWaitingJobs:
      self.log.debug( "Kill Old SAM Job", repr( job ) )
      dirac.delete( int( job ) )
    self.log.info( "%s:" % ( AGENT_NAME ), " %d SAM jobs were deleted" % ( len( oldWaitingJobs ) ) )

    ceOldWaitingJobs = ceOldWaitingJobs.keys()
    ceNewStartedJobs = ceNewStartedJobs.keys()

    self.log.info( "ceOldWaitingJobs", repr( ceOldWaitingJobs ) )
    self.log.info( "ceNewStartedJobs", repr( ceNewStartedJobs ) )

    for ce in allces:
      status = 0
      if ( ce in ceNewStartedJobs ) and ( ce in ceOldWaitingJobs ):
        status = 40
      if ( ce in ceNewStartedJobs ) and not ( ce in ceOldWaitingJobs ):
        status = 10
      if not ( ce in ceNewStartedJobs ) and ( ce in ceOldWaitingJobs ):
        status = 50

      self.log.verbose( "CE status:", "%s %d" % ( ce, status ) )

    return S_OK()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
