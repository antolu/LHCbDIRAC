''' SAM Agent submits SAM jobs
'''

import time

from datetime import date, timedelta

from DIRAC                       import gConfig, S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient  import RPCClient
from DIRAC.Core.Utilities        import List
from DIRAC.Interfaces.API.Dirac  import Dirac

from LHCbDIRAC.SAMSystem.Client.DiracSAM import DiracSAM

__RCSID__  = "$Id$"
AGENT_NAME = 'SAM/SAMAgent'

class SAMAgent( AgentModule ):  
  
  # Max lifetime of jobs before being cleaned
  __days = 2
  
  def __init__( self, *args, **kwargs ):
    
    AgentModule.__init__( self, *args, **kwargs )
    
    self.days = self.__days
    
  def initialize( self ):
    
    self.days = self.am_getOption( 'days', self.days )

    self.am_setOption( 'shifterProxy', 'TestManager' )

#    _operation = self.monitor.OP_SUM
#    _bucket    = 3600 * 2
#
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

#    deletedJobs = res[ 'Value' ]     
#    res = self._siteAccount( deletedJobs )
#    if not res[ 'OK' ]:
#      self.log.error( res[ 'Message' ] )

    diracSAM = DiracSAM()
   
    ces = diracSAM.getSuitableCEs()
    if not ces[ 'OK' ]:
      self.log.error( ces[ 'Message' ] )
      return ces
    ces = ces[ 'Value' ]
  
    for ce in ces:
      #result = diracSAM.submitSAMJob( ce, softwareEnable = False )
      result = diracSAM.submitNewSAMJob( ce )
      if not result[ 'OK' ]:
        self.log.error( 'Submission to CE %s failed with message %s' % ( ce, result[ 'Message' ] ) )

    return result

#  IXME: is this really needed ? Is it used ?
#  def _siteAccount( self, deletedJobs ):
#    '''
#      Adds marks to agent monitor regarding the number of sites
#    '''
#
#    wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )
#    result = wmsAdmin.getSiteMask()
#    
#    if not result[ 'OK' ]:
#      result[ 'Message' ] = "Can't get SiteMask: %s" % result[ 'Message' ]      
#      return result
#    sitesmask = result[ 'Value' ]
#
#    result = gConfig.getSections( '/Resources/Sites/LCG' )
#    
#    if not result[ 'OK' ]:
#      result[ 'Message' ] = "Can't get Sites: %s" % result[ 'Message' ] 
#      return result
#    sites = result[ 'Value' ]
#
#    numsites     = len( sites )
#    numsitesmask = len( sitesmask )
#    self.monitor.addMark( 'TotalSites', numsites )
#    self.monitor.addMark( 'ActiveSites', numsitesmask )
#    self.monitor.addMark( 'BannedSites', numsites - numsitesmask )
#    self.monitor.addMark( 'DeletedJobs', deletedJobs )
#    self.log.verbose( 'Site Account finished' )
#    return S_OK()

  def _clearSAMjobs( self ):
    '''
       Clear SAM jobs older than <self.days> days
    '''

    self.log.info( "Clear SAM jobs for last %d day(s)" % self.days )
    dirac = Dirac()

    #testName = 'CE-lhcb-availability'

    allces     = []
    jobIDs     = []
    conditions = { 'JobGroup' : 'Test' }
    
    sites = gConfig.getSections( '/Resources/Sites/LCG' )
    if not sites[ 'OK' ]:
      return sites
    sites = sites[ 'Value' ]   
    
    monitoring = RPCClient( 'WorkloadManagement/JobMonitoring' )
    cutDate    = ( date.today() - timedelta( days = 2 ) ).strftime( '%Y-%m-%d' )

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
    
#    deletedJobs = len( oldWaitingJobs )

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

    return S_OK()#deletedJobs )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
