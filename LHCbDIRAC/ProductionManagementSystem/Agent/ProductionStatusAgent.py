"""  The ProductionStatusAgent monitors productions for active requests
     and takes care to update their status. Initially this is just to handle
     simulation requests.

     Allowed production status transitions performed by this agent include:

     Idle -> ValidatingInput
     Idle -> ValidatingOutput

     ValidatedOutput -> Completed

     ValidatingInput -> RemovingFiles

     RemovedFiles -> Completed

     Active -> Idle

     Testing -> Idle

     In addition this also updates request status from Active to Done.

     To do: review usage of production API(s) and re-factor into Production Client

     AZ 10.14: merged with a part from RequestTrackingAgent to avoid race conditions
"""

__RCSID__ = "$Id$"

import time
from DIRAC                                                      import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.Interfaces.API.Dirac                                 import Dirac
from DIRAC.FrameworkSystem.Client.NotificationClient            import NotificationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations        import Operations

from LHCbDIRAC.TransformationSystem.Client.TransformationClient  import TransformationClient
from LHCbDIRAC.Interfaces.API.DiracProduction                    import DiracProduction
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient        import BookkeepingClient

#############################################################################
# The following is used for StandAlone debugging only (outside Agent)
from DIRAC import gLogger
gStandAlone = False                # work in command line without Agent
#gSimulate = gStandAlone and True  # real clients are replaced with simulation
gSimulate = False
gDoRealUpdate = True         # call status updates
gDoRealTracking = True       # update requests progress

class ProductionRequestSIM():
  """ Simulate PrductionRequest Service
  """
  def __init__( self, *args, **kwargs ):
    """ Define some test Production Requests:
       Active Simulation Request with 3 transformations. (pr 1, t 11 MCSimulation, 12 MCStripping, 13 Merge)
       Active Simulation Request with 2 subrequests, 2 transformations in each (pr 2,(3,4) t (14 MCSimulation, 15 Merge), (16,17) )
       Active Stripping Request with 2 transformations. (pr 5, t (18 DataStripping, 19 Merge))
    """
    self.pr = {
      1 : { 'state': 'Active', 'type': 'Simulation', 'master': 0, 'rqTotal': 10000,  'prods': { 11 : { 'Used': 0, 'Events': 0 }, 
                                                                                                12 : { 'Used': 0, 'Events': 0 },
                                                                                                13 : { 'Used': 1, 'Events': 0 } } },
      2 : { 'state': 'Active', 'type': 'Simulation', 'master': 0, 'rqTotal': 50000, 'prods': {} },
      3 : { 'state': '', 'type': '', 'master': 2, 'rqTotal': 20000, 'prods': { 14 : { 'Used': 0, 'Events': 0 }, 15 : { 'Used': 1, 'Events': 0 } } },
      4 : { 'state': '', 'type': '', 'master': 2, 'rqTotal': 30000, 'prods': { 16 : { 'Used': 0, 'Events': 0 }, 17 : { 'Used': 1, 'Events': 0 } } },
      5 : { 'state': 'Active', 'type': 'Stripping', 'master': 0, 'rqTotal': 0, 'prods': { 18 : { 'Used': 0, 'Events': 0 }, 19 : { 'Used': 1, 'Events': 0 } } },
      }

  def getAllProductionProgress( self ):
    """ Returns all known productions
    """
    answer = {}
    for prID, summary in self.pr.iteritems():
      answer[prID] = {}
      for tID, tInfo in summary['prods'].iteritems():
        answer[prID][tID] = { 'Used': tInfo['Used'], 'Events': tInfo['Events'] }
    return S_OK( answer )

  def getProductionRequestList( self, master, u1, u2, u3, u4, rfilter ):
    """ Only works for the calls used in this agent
    """
    answer = []
    for prID, summary in self.pr.iteritems():
      toInclude = False
      if not master and summary['state'] == 'Active': # Return Active requests
        toInclude = True
      elif master and summary['master'] == master:  # Subrequests
        toInclude = True
      if toInclude:
        hasSubrequest = 2 if len( summary['prods'] ) == 0 else 0
        bkTotal = 0
        for _tID, tInfo in summary['prods'].iteritems():
          if tInfo['Used']:
            bkTotal += tInfo['Events']
        answer.append( { 'RequestID': prID, 'HasSubrequest': hasSubrequest, 'RequestType': summary['type'], 'master': summary['master'],
                         'bkTotal': bkTotal, 'rqTotal': summary['rqTotal'] } )
    return S_OK( { 'Rows': answer } )

  def updateProductionRequest( self, prID, updDict ):
    """ Update the state of the request
    """
    if prID in self.pr and 'RequestState' in updDict:
      self.pr[prID]['state'] = updDict['RequestState']
      return S_OK()
    else:
      gLogger.error( 'Unsupported parameters for updateProductionRequest' )
      return S_ERROR( ' Unsupported ' )
    
  def updateTrackedProductions( self, toUpdate ):
    """ Update production progress
    """
    for it in toUpdate:
      for _prID, summary in self.pr.iteritems():
        if it['ProductionID'] in summary['prods']:
          summary['prods'][it['ProductionID']]['Events'] = it['BkEvents']
          break
    return S_OK()

  def __getPrForT( self, tID):
    """ For simulation only
    """
    for _prID, summary in self.pr.iteritems():
      if tID in summary['prods']:
        return summary
    return {}

  def getBkTotalForT( self, tID ):
    """ For simulation only
    """
    bkTotal = 0
    summary = self.__getPrForT( tID )
    for tID, tInfo in summary['prods'].iteritems():
      if tInfo['Used']:
        bkTotal += tInfo['Events']
    return bkTotal

  def getPrTotalForT( self, tID ):
    """ For simulation only
    """
    summary = self.__getPrForT( tID )
    return summary['rqTotal']


class TransformationAndBookkeepingSIM():
  """ Simulate TransformationClient and Bookkeeping client
  """
  def __init__( self, *args, **kwargs ):
    """ Define some test Transformations:
       11-18 from simulated requests
       100 is not request related transformation
    """
    self.t_types = { 11: 'MCSimulation', 12: 'MCStripping', 13: 'Merge', 14: 'MCSimulation', 15: 'MCReconstruction',
                     16: 'MCSimulation', 17: 'MCReconstruction', 18: 'DataStripping', 19: 'Merge',
                     100: 'Replication' }
    self.t  = { }
    for tID, tType in self.t_types.iteritems():
      self.t[tID] = { 'status': 'Active', 'processedEvents': 0, 'Type': tType, 
                      'filesStat': { 'Processed': 0, 'Unused': 0, 'Assigned': 0 }, 
                      'tasksStat': { 'TotalCreated': 0, 'Running': 0, 'Done': 0, 'Failed': 0 } 
                      }
    self.log = gLogger

    self.evPerFile = 100 # number of event in each MC generated file
    self.fPerJob  = 10   # number of files to merge in one MC merge job

  def __animateJobs ( self, tID, failing ):
    """ All running jobs go to either failed ot done state
    """
    ts = self.t[tID]['tasksStat']
    nJobsRun = ts['Running']
    if nJobsRun == 0:
      return ( 0, 0 )
    if nJobsRun < 10:
      failing = 0
    nJobsFail = int ( nJobsRun * failing / 100 )
    nJobsDone = nJobsRun - nJobsFail
    ts['Failed'] += nJobsFail
    ts['Done'] += nJobsDone
    ts['Running'] = 0
    return ( nJobsDone, nJobsFail )

  def __createProcessingJobs( self, tID, fPerJob ):
    """ Create processing jobs using filesStat
    """
    fs = self.t[tID]['filesStat']
    nJobs = int( fs['Unused'] / fPerJob )
    if nJobs == 0:
      return False
    fs['Unused'] -= nJobs * fPerJob
    fs['Assigned'] += nJobs * fPerJob
    ts = self.t[tID]['tasksStat']
    ts['Running'] += nJobs
    ts['TotalCreated'] += nJobs
    return True

  def __animateMerging ( self, tID, failing ):
    """ Advance merging transformation
    """
    
    ( nJobsDone, nJobsFail ) = self.__animateJobs( tID, failing )
    fs = self.t[tID]['filesStat']
    fs['Processed'] += nJobsDone * self.fPerJob
    fs['Assigned'] -= ( nJobsDone + nJobsFail ) * self.fPerJob
    fs['Unused'] += nJobsFail * self.fPerJob
    self.t[tID]['processedEvents'] += nJobsDone * self.fPerJob * self.evPerFile
    
    isModified = self.__createProcessingJobs( tID, self.fPerJob )

    if self.t[tID]['status'] == 'ValidatingOutput':
      # Emitate validating Agent
      self.t[tID]['status'] = 'ValidatedOutput'
      isModified = True

    if ( nJobsDone > 0 ) or isModified:
      self.log.verbose( 'Merging %s: %s' % ( tID, str( self.t[tID] ) ) )

  def __animateStripping ( self, tID, tNextID, failing ):
    """ Advance stripping transformation
    """
    ( nJobsDone, nJobsFail ) = self.__animateJobs( tID, failing )
    fs = self.t[tID]['filesStat']
    fs['Processed'] += nJobsDone
    fs['Assigned'] -= ( nJobsDone + nJobsFail )
    fs['Unused'] += nJobsFail
    self.t[tID]['processedEvents'] += nJobsDone * self.evPerFile
    self.t[tNextID]['filesStat']['Unused'] += nJobsDone
    
    isModified = self.__createProcessingJobs( tID, 1 )

    if self.t[tID]['status'] == 'RemovingFiles':
      # Emitate cleaning Agent
      self.t[tID]['status'] = 'RemovedFiles'
      isModified = True

    if ( nJobsDone > 0 ) or isModified:
      self.log.verbose( 'Stripping %s: %s' % ( tID, str( self.t[tID] ) ) )

  def __extendSimulation ( self, tID, prClient ):
    """ Imitate simulation extention
    """
    bkTotal = prClient.getBkTotalForT( tID )
    prTotal = prClient.getPrTotalForT( tID )
    if bkTotal >= prTotal:
      return False
    nJobs = int( ( (prTotal - bkTotal) + self.evPerFile - 1 ) / self.evPerFile )
    ts = self.t[tID]['tasksStat']
    ts['TotalCreated'] += nJobs
    ts['Running'] += nJobs
    return True

  def __animateSimulation ( self, tID, tNextID, failing, prClient ):
    """ Advance simulation transformation
    """
    
    ( nJobsDone, _nJobsFail ) = self.__animateJobs( tID, failing )
    self.t[tID]['processedEvents'] += nJobsDone * self.evPerFile
    self.t[tNextID]['filesStat']['Unused'] += nJobsDone

    isModified = False
    if self.t[tID]['status'] == 'Idle':
      isModified = self.__extendSimulation( tID, prClient )
    elif self.t[tID]['status'] == 'RemovingFiles':
      # Emitate cleaning Agent
      self.t[tNextID]['filesStat']['Unused'] = 0;
      self.t[tID]['status'] = 'RemovedFiles'
      isModified = True

    if ( nJobsDone > 0 ) or isModified:
      self.log.verbose( 'MC Simulation %s: %s' % ( tID, str( self.t[tID] ) ) )


  def _animate3TSimulation ( self, prClient ):
    """ animate Simulation->Stripping->Merge production request
    """
    if self.t[11]['tasksStat']['TotalCreated'] == 0:
      self.__extendSimulation( 11, prClient )
      self.log.verbose( 'MC Simulation %s: %s' % ( 11, str( self.t[11] ) ) )
      return # Initial condition (is the System really does that ???)

    self.__animateMerging( 13, 20 )
    self.__animateStripping( 12, 13, 20 )
    self.__animateSimulation( 11, 12, 20, prClient )

  def _animate2x2TSimulation ( self, prClient ):
    """ animate 2x Simulation->Reconstruction production request
    """
    if self.t[14]['tasksStat']['TotalCreated'] == 0:
      self.__extendSimulation( 14, prClient )
      self.__extendSimulation( 16, prClient )
      self.log.verbose( 'MC Simulation %s: %s' % ( 14, str( self.t[14] ) ) )
      self.log.verbose( 'MC Simulation %s: %s' % ( 16, str( self.t[16] ) ) )
      return # Initial condition (is the System really does that ???)

    self.__animateMerging( 15, 20 )
    self.__animateMerging( 17, 20 )
    self.__animateSimulation( 14, 15, 20, prClient )
    self.__animateSimulation( 16, 17, 20, prClient )


  def _animateReplication ( self ):
    """ animate replication transformation
    """
    tInfo = self.t[100]
    ts = tInfo['tasksStat']
    fs = tInfo['filesStat']
    if ts['TotalCreated'] == 0:
      # Create "new tasks" (once)
      ts['TotalCreated'] = 2
      ts['Running'] = 2
      fs['Assigned'] = 20
    elif ts['Done'] == 0:
      # On intermediate state - one job failed, one is done, one new is active
      ts['TotalCreated'] = 3
      ts['Running'] = 1
      ts['Failed'] = 1
      ts['Done'] = 1
      fs['Assigned'] = 10
      fs['Processed'] = 10
      tInfo['processedEvents'] = 1000
    elif ts['Done'] == 1:
      # Everything is done, but not yet "completed"
      ts['Running'] = 0
      ts['Done'] = 2
      fs['Assigned'] = 0
      fs['Processed'] = 20
      tInfo['processedEvents'] = 2000
    elif ts['Done'] == 2 and tInfo['status'] != 'Completed' :
      tInfo['status'] = 'Completed'
    else:
      return
    self.log.verbose( 'Replication %s : %s' % ( 100, str(tInfo) ) )
      


  def animate( self, prClient ):
    """ Calculate next "step" of simulation
    """
    # self._animateReplication()
    # self._animate3TSimulation( prClient )
    self._animate2x2TSimulation( prClient )

  # bkClient
  def getProductionProcessedEvents( self, tID ):
    return S_OK( self.t.get( tID, { 'processedEvents': 0 } )['processedEvents'] )

  # the rest is for TransformationClient
  def getTransformationWithStatus( self, status ):
    return S_OK( [ tID for tID, tInfo in self.t.iteritems() if tInfo['status'] == status ] )


  def getTransformation( self, tID ):
    if not tID in self.t:
      return S_ERROR( 'Transformation %s soes not exists' % tID )
    tInfo = self.t[tID]
    return S_OK( { 'Type' : tInfo['Type'] } )

  def getTransformationStats( self, tID ):
    if not tID in self.t:
      return S_ERROR( 'Transformation %s does not exists' % tID )
    return S_OK( self.t[tID]['filesStat'] )

  def getTransformationTaskStats( self, tID ):
    if not tID in self.t:
      return S_ERROR( 'Transformation %s does not exists' % tID )
    return S_OK( self.t[tID]['tasksStat'] )

  def setTransformationParameter( self, tID, par, value ):
    """ Only able to set "Status"
    """
    if not tID in self.t:
      return S_ERROR( 'Transformation %s does not exists' % tID )
    if par != 'Status':
      return S_ERROR( 'Unsupported Transformation parameter %s' % par )
    self.t[tID]['status'] = value
    return S_OK()

##########################################################

class ProductionStatusAgent( AgentModule ):
  """ Usual DIRAC agent
  """

  def __init__( self, *args, **kwargs ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param str loadName: load name of agent
    :param bool baseAgentName: whatever
    :param dict properties: whatever else
    """
    if not gStandAlone:
      AgentModule.__init__( self, *args, **kwargs )
    else:
      self.log = gLogger

    self.dProd = None
    self.dirac = None
    self.prClient = None
    self.tClient = None
    self.bkClient = None

    self.simulationTypes = Operations().getValue( 'Transformations/ExtendableTransfTypes', ['MCSimulation',
                                                                                            'Simulation'] )

    self.allKnownStates = ( 'RemovedFiles', 'RemovingFiles', 'ValidatedOutput', 'ValidatingInput', 'Testing', 'Active', 'Idle' )

    self.notify = True

    # For processing transformations, it can happened that there are some Unused files
    # with which to tasks can be created. The number of such files can be different depending
    # from the module and distrubution between centres.
    # So we declire such transfomrations isIdle() once there is no jobs, no files in other
    # pending states and the number of Unused files was not changed last cyclesTillIdle times
    self.cyclesTillIdle = 1
    self.filesUnused = {} # <tID: { 'Number': x, 'NotChanged': n }

  #############################################################################
  def initialize( self ):
    """ Sets default values.
    """
    # shifter
    if not gStandAlone:
      self.am_setOption( 'shifterProxy', 'ProductionManager' )
      self.notify = eval( self.am_getOption( 'NotifyProdManager', 'True' ) )

    # Set the clients
    self.dProd = DiracProduction()
    self.dirac = Dirac()
    if gSimulate:
      self.prClient = ProductionRequestSIM()
      self.tClient = TransformationAndBookkeepingSIM()
      self.bkClient = self.tClient
    else:
      self.prClient = RPCClient( 'ProductionManagement/ProductionRequest' )
      self.tClient = TransformationClient()
      self.bkClient = BookkeepingClient()

    return S_OK()

  #############################################################################
  def execute( self ):
    """ The execution method, track requests progress and implement a part of Production SM
    """
    updatedT = {}   # updated transformations
    updatedPr = []  # updated production requests (excluding traking updates)

    # Distinguish between leafs and master requests
    # Masters should not appear in the prodReqSummary and they should have no
    # associated productions.
    self.prMasters = {} # [ prID: [<subrequests> ...] ]
    self.prSummary = {}
    # { <reqID> :
    #     'type', 'master', 'bkTotal', 'prTotal',  - from _getActiveProductionRequests()
    #     'isDone', 'prods': [ <prodIf> : { 'Used', 'Events' } ] - from __getProductionRequestsProgress
    #     'state' for each production - from _getTransformationsState()
    #     'isIdle', 'isProcIdle' for each 'Active' or 'Idle' production, 'isSimulation' - from _getIdleProductionRequestProductions()
    #     'isFinished' - from _applyProductionRequestsLogic()
    # }
    self.prProds = {} # <prID>, map produciton to known request, from _getProductionRequestsProgress

    self.notPrTrans = {} # transformation without PR, from _getTransformationsState

    self.log.info( "******************************" )
    self.log.info( "Collecting required information" )
    self.log.info( "******************************" )

    result = self._getActiveProductionRequests()
    if not result['OK']:
      self.log.error( "Aborting cycle", result["Message"] )
      return S_OK()

    self._getTransformationsState()
    self._getIdleProductionRequestProductions()

    # That is IMPORTANT to do that after we have the transformation status,
    # since Validation can (really???) update BK, rendering MC incomplete
    result = self._trackProductionRequests() # also updates PR DB
    if not result['OK']:
      self.log.error( "Aborting cycle", result["Message"] )
      return S_OK()

    self.log.info( "******************************" )
    self.log.info( "Updating Production Requests and related transformations" )
    self.log.info( "******************************" )

    self._applyProductionRequestsLogic( updatedT, updatedPr )

    self.log.info( "******************************" )
    self.log.info( "Updating Production Request unrelated transformations (replication, etc.)" )
    self.log.info( "******************************" )

    self._applyOtherTransformationsLogic( updatedT )

    self.log.info( "*********" )
    self.log.info( "Reporting" )
    self.log.info( "*********" )

    if updatedT:
      self.log.info( 'Transformations updated this cycle:' )
      for n, v in updatedT.items():
        self.log.info( 'Transformations %s: %s => %s' % ( n, v['from'], v['to'] ) )

    if updatedPr:
      self.log.info( 'Production Requests updated to Done status: %s' % ( ', '.join( [str( i ) for i in updatedPr] ) ) )

    if gDoRealUpdate and not gSimulate:
      self._mailProdManager( updatedT, updatedPr )

    self._cleanFilesUnused()

    if gSimulate:
      self.tClient.animate( self.prClient )

    return S_OK()

  #############################################################################

  def __getProductionRequestsProgress( self ):
    """ get known progress for Active requests related productions
        Failures there are critical and can inforce wrong logic
    """

    self.log.verbose( "Collecting old Production Request Progress..." )
    result = self.prClient.getAllProductionProgress()
    if not result['OK']:
      return S_ERROR( 'Could not retrieve production progress summary: %s' % result['Message'] )
    progressSummary = result['Value'] # { <prID> : [ <prodId> : { 'Used', 'Events' } ] }

    for prID, summary in self.prSummary.iteritems():
      # Setting it before updating will give grace period before SM ops
      summary['isDone'] = True if summary['bkTotal'] >= summary['prTotal'] else False
      summary['prods'] = progressSummary.get( prID, {} )
      for tID in summary['prods']:
        self.prProds[tID] = prID
    self.log.verbose( "Done with old Production Request Progress" )
    return S_OK()

  def _getActiveProductionRequests( self ):
    """ get 'Active' requests.
        Failures there are critical and can inforce wrong logic
        Note: this method can be moved to the service
    """
    self.log.verbose( "Collecting active production requests..." )
    result = self.prClient.getProductionRequestList( 0, '', 'ASC', 0, 0, { 'RequestState':'Active' } )
    if not result['OK']:
      return S_ERROR( 'Could not retrieve active production requests: %s' % result['Message'] )
    activeMasters = result['Value']['Rows']
    for pr in activeMasters:
      prID = pr['RequestID']
      if pr['HasSubrequest']:
        self.prMasters[prID] = [ ]
        result = self.prClient.getProductionRequestList( prID, '', 'ASC', 0, 0, {} )
        if not result['OK']:
          return S_ERROR( 'Could not get subrequests for production request %s: %s' % ( prID, result['Message'] ) )
        for subPr in result['Value']['Rows']:
          subPrID = subPr['RequestID']
          self.prSummary[subPrID] = \
              { 'type' : pr['RequestType'], 'master':prID, 'bkTotal' : subPr['bkTotal'], \
                'prTotal' : subPr['rqTotal'] }
          self.prMasters[prID].append( subPrID )
      else:
        self.prSummary[prID] = \
            { 'type' : pr['RequestType'], 'master':0, 'bkTotal' : pr['bkTotal'], 'prTotal' : pr['rqTotal'] }

    result = self.__getProductionRequestsProgress()
    if not result['OK']:
      return result

    self.log.info( 'Will work with %s productions from %s Active (sub)requests' % \
        ( len( self.prProds ), len( self.prSummary ) ) )
    self.log.verbose( "Done with collecting Active production requests" )
    return S_OK()

  def __getTransformations( self, status ):
    """ dev function. Get the transformations (print info in the meanwhile)
    """

    res = self.tClient.getTransformationWithStatus( status )
    if not res['OK']:
      self.log.error( "Failed to get transformations", "%s: %s" % ( status, res['Message'] ) )
      raise RuntimeError( "Failed to get %s transformations: %s" % ( status, res['Message'] ) )
    if not res['Value']:
      self.log.debug( 'No transformations in %s status' % status )
      return []
    else:
      if len(res['Value']) > 20:
        self.log.verbose( "The following number of transformations are in %s status: %u" % ( status, len(res['Value']) ) )
      else:
        valOutStr = ', '.join( [str( i ) for i in res['Value']] )
        self.log.verbose( "The following transformations are in %s status: %s" % ( status, valOutStr ) )
      return res['Value']

  def _getTransformationsState( self ):
    """ get Transformations state (set 'Other' for not interesting states)
        failures to get something are not critical since there is no reaction on 'Other' state
    """
    self.log.verbose( "Collecting transformations state..." )
    try:
      # We put 'Finished' for both
      tListCompleted = self.__getTransformations( 'Completed' )
      tListArchived = self.__getTransformations( 'Archived' )
      tListFinished = tListCompleted + tListArchived
      for tID in tListFinished:
        prID = self.prProds.get( tID, None )
        if prID:
          self.prSummary[prID]['prods'][tID]['state'] = 'Finished'

      for state in self.allKnownStates:
        tList = self.__getTransformations( state )
        for tID in tList:
          prID = self.prProds.get( tID, None )
          if prID:
            self.prSummary[prID]['prods'][tID]['state'] = state
          else:
            notPrList = self.notPrTrans.setdefault( state, [] )
            notPrList.append( tID )
    except RuntimeError, error:
      self.log.error( error )

    for tID, prID in self.prProds.iteritems():
      if 'state' not in self.prSummary[prID]['prods'][tID]:
        self.prSummary[prID]['prods'][tID]['state'] = 'Other'

    self.log.verbose( "Done with collecting transformations states" )

  def __getTransformationTaskStats( self, tID ):
    """ get the stats for a transformation tasks (number of tasks in each status)
    """

    result = self.tClient.getTransformationTaskStats( tID )
    if not result['OK']:
      self.log.error( 'Could not retrieve transformation tasks stats', result['Message'] )
      tTaskStats = {}
    else:
      tTaskStats = result['Value']

    return tTaskStats

  def __getTransformationFilesStats( self, tID ):
    """ get the stats for a transformation files (number of files in each status)
    """

    result = self.tClient.getTransformationStats( tID )
    if not result['OK']:
      self.log.error( 'Could not retrieve transformation files stats', result['Message'] )
      tFilesStats = {}
    else:
      tFilesStats = result['Value']

    return tFilesStats

  def __isIdle( self, tID ):
    """ Cheks if a transformation is idle, is procIdle and either the tranformation is simulation
    """
    self.log.debug( "Checking either transformation %d is idle" % tID )
    result = self.tClient.getTransformation( tID )
    if not result['OK']:
      raise RuntimeError( "Failed to get transformation %s: %s" % ( tID, result['Message'] ) )
    tInfo = result['Value']
    if tInfo.get( 'Type', None ) in self.simulationTypes:
      isSimulation = True
      # simulation : go to Idle if
      # only failed and done jobs
      # AND number of tasks created in total == number of tasks submitted
      tStats = self.__getTransformationTaskStats( tID )
      self.log.debug( "Tasks Stats: %s" % str( tStats ) )
      isIdle = ( ( tStats.get( 'TotalCreated', 0 ) > 0 ) \
                  and \
                  all( [tStats.get( status, 0 ) == 0 for status in ['Checking', 'Completed', 'Created', 'Matched',
                                                                    'Received', 'Reserved', 'Rescheduled', 'Running',
                                                                    'Submitted', 'Waiting' ]] ) )
      isProcIdle = isIdle
    else:
      isSimulation = False
      # other transformation type : go to Idle if
      # 0 assigned files, unused files number was not chaning during the last cyclesTillIdle time
      # AND > 0 processed files
      # AND only failed and done jobs
      filesStats = self.__getTransformationFilesStats( tID )
      self.log.debug( "Files stats: %s" % str( filesStats ) )
      unused = filesStats.get( 'Unused', 0 )
      oldUnused = self.filesUnused.setdefault( tID, { 'Number': -1, 'NotChanged': 0 } )
      if oldUnused['Number'] == unused:
        oldUnused['NotChanged'] += 1
      else:
        oldUnused['NotChanged'] = 0
        oldUnused['Number'] = unused
      assigned = filesStats.get( 'Assigned', 0 )
      processed = filesStats.get( 'Processed', 0 )
      isProcIdle = ( ( processed > 0 ) and ( assigned == 0 ) and ( ( unused == 0 ) or ( oldUnused['NotChanged'] >= self.cyclesTillIdle ) ) )
      if isProcIdle:
        tStats = self.__getTransformationTaskStats( tID )
        self.log.debug( "Tasks Stats: %s" % str( tStats ) )
        isProcIdle = all( [tStats.get( status, 0 ) == 0 for status in ['Checking', 'Completed', 'Created', 'Matched',
                                                                       'Received', 'Reserved', 'Rescheduled', 'Running',
                                                                       'Submitted', 'Waiting' ]] )
      isIdle = isProcIdle and (unused == 0)
    return (isIdle, isProcIdle, isSimulation)

  def _getIdleProductionRequestProductions( self ):
    """ evaluate isIdle and isProcIdle status for all productions we need.
        failures are rememberd and are taken into account later
    """
    self.log.verbose( "Checking idle productions..." )
    for tID, prID in self.prProds.iteritems():
      tInfo = self.prSummary[prID]['prods'][tID]
      if tInfo['state'] in ( 'Active', 'Idle' ):
        try:
          isIdle, isProcIdle, isSimulation = self.__isIdle( tID )
          tInfo['isIdle'] = 'Yes' if isIdle else 'No'
          tInfo['isProcIdle'] = 'Yes' if isProcIdle else 'No'
          tInfo['isSimulation'] = isSimulation
        except RuntimeError, error:
          self.log.error( error )
          tInfo['isIdle'] = 'Unknown'
          tInfo['isProcIdle'] = 'Unknown'
          tInfo['isSimulation'] = False
      else:
        tInfo['isIdle'] = 'Unknown'
        tInfo['isProcIdle'] = 'Unknown'
        tInfo['isSimulation'] = False
    self.log.verbose( "Checking idle done" )

  def _trackProductionRequests( self ):
    """ contact BK for the current number of processed events
        failures are critical
    """
    self.log.verbose( "Updating production requests progress..." )
    toUpdate = []
    for tID, prID in self.prProds.iteritems():
      tInfo = self.prSummary[prID]['prods'][tID]
      result = self.bkClient.getProductionProcessedEvents( tID )
      if result['OK']:
        nEvents = result['Value']
        if nEvents and nEvents != tInfo['Events'] :
          self.log.debug( "Updating production %d, with BkEvents %d" % ( int( tID ), \
                                                                         int( nEvents ) ) )
          toUpdate.append( { 'ProductionID': tID, 'BkEvents': nEvents } )
          tInfo['Events'] = nEvents
      else:
        self.log.error( 'Progress is not updated", %s : %s' % ( tID, result['Message'] ) )
        return S_ERROR( 'Too dangerous to continue' )

    if toUpdate:
      if gDoRealTracking:
        result = self.prClient.updateTrackedProductions( toUpdate )
      else:
        result = S_OK()
      if not result['OK']:
        self.log.error( 'Could not send update to the Production Request System', result['Message'] )  # that is not critical
      else:
        self.log.info( 'The progress of %s Production Requests is updated' % len( toUpdate ) )
    self.log.verbose( "Production requests progress update is finished" )
    return S_OK()

  def _cleanFilesUnused( self ):
    """ remove old transformations from filesUnused
    """
    oldIDs = []
    for tID in self.filesUnused:
      if tID in self.prProds:
        continue
      used = False
      for _status, IDs in self.notPrTrans.iteritems():
        if tID in IDs:
          used = True
          break
      if not used:
        oldIDs.append( tID )
    for tID in oldIDs:
      del( self.filesUnused[tID] )

  def __updateTransformationStatus( self, tID, origStatus, status, updatedT ):
    """ This method updates the transformation status and logs the changes for each
        iteration of the agent.  Most importantly this method only allows status
        transitions based on what the original status should be.
    """
    self.log.info( 'Changing status for transformation %s to %s' % ( tID, status ) )

    if not gDoRealUpdate:
      updatedT[tID] = {'to':status, 'from':origStatus}
      return
    
    try:
      result = self.tClient.setTransformationParameter( tID, 'Status', status )
      if not result['OK']:
        self.log.error( "Failed to update status of transformation", "%s from %s to %s" % ( tID, origStatus, status ) )
      else:
        updatedT[tID] = {'to':status, 'from':origStatus}
    except RuntimeError, error:
      self.log.error( error )

  def _mailProdManager( self, updatedT, updatedPr ):
    """ Notify the production manager of the changes as productions should be
        manually extended in some cases.
    """
    if not updatedT and not updatedPr:
      self.log.info( 'No changes this cycle, mail will not be sent' )
      return

    if self.notify:
      notify = NotificationClient()
      subject = 'Transofrmation Status Updates ( %s )' % ( time.asctime() )
      msg = ['Transformations updated this cycle:\n']
      for tID, val in updatedT.iteritems():
        msg.append( 'Production %s: %s => %s' % ( tID, val['from'], val['to'] ) )
      msg.append( '\nProduction Requests updated to Done status this cycle:\n' )
      msg.append( ', '.join( [str( i ) for i in updatedPr] ) )
      result = notify.sendMail( 'vladimir.romanovsky@cern.ch', subject, '\n'.join( msg ),
                                'vladimir.romanovsky@cern.ch', localAttempt = False )
      if not result['OK']:
        self.log.error( "Could not send mail", result['Message'] )
      else:
        self.log.info( 'Mail summary sent to production manager' )

  def __updateProductionRequestStatus( self, prID, status, updatedPr ):
    """ This method updates the production request status.
    """
    self.log.info( 'Marking Production Request %s as %s' % ( prID, status ) )

    if not gDoRealUpdate:
      updatedPr.append( prID )
      return

    reqClient = RPCClient( 'ProductionManagement/ProductionRequest', useCertificates = False, timeout = 120 )
    result = reqClient.updateProductionRequest( long( prID ), {'RequestState':status} )
    if not result['OK']:
      self.log.error( result )
    else:
      updatedPr.append( prID )

  def _applyOtherTransformationsLogic( self, updatedT ):
    """ animate not Production Requests related transformations
        failures are not clitical
    """
    self.log.verbose( "Updating requests unrelated transformations..." )

    if 'RemovedFiles' in self.notPrTrans:
      self.log.info( 'Processing %s requests unrelated transformations in "RemovedFiles" state' % \
                       len( self.notPrTrans['RemovedFiles'] ) )
      for tID in self.notPrTrans['RemovedFiles']:
        self.__updateTransformationStatus( tID, 'RemovedFiles', 'Completed', updatedT )

    if 'Active' in self.notPrTrans:
      self.log.info( 'Processing %s requests unrelated transformations in "Active" state' % \
                       len( self.notPrTrans['Active'] ) )
      for tID in self.notPrTrans['Active']:
        try:
          isIdle, _isProcIdle, _isSimulation = self.__isIdle( tID )
          if isIdle:
            self.__updateTransformationStatus( tID, 'Active', 'Idle', updatedT )
        except RuntimeError, error:
          self.log.error( error )

    if 'Idle' in self.notPrTrans:
      self.log.info( 'Processing %s requests unrelated transformations in "Idle" state' % \
                       len( self.notPrTrans['Idle'] ) )
      for tID in self.notPrTrans['Idle']:
        try:
          isIdle, _isProcIdle, _isSimulation = self.__isIdle( tID )
          if not isIdle:
            self.__updateTransformationStatus( tID, 'Idle', 'Active', updatedT )
        except RuntimeError, error:
          self.log.error( error )

    self.log.verbose( 'Requests unrelated transformations update is finished' )


  def _isReallyDone( self, summary ):
    """ Evaluate 'isDone' from current update cycle
    """
    bkTotal = 0
    for _tID, tInfo in summary['prods'].iteritems():
      if tInfo['Used']:
        bkTotal += tInfo['Events']
    return ( True if bkTotal >= summary['prTotal'] else False )

  def _producersAreIdle( self, summary ):
    """ Return True in case all producers (not 'Used') transformations are Idle, Finished or not exist
    """
    for _tID, tInfo in summary['prods'].iteritems():
      if tInfo['Used']:
        continue
      if tInfo['isIdle'] != 'Yes' and tInfo['state'] != 'Finished' :
        return False
    return True

  def _producersAreProcIdle( self, summary ):
    """ Return True in case all producers (not 'Used') transformations are procIdle or finished or not exist
    """
    for _tID, tInfo in summary['prods'].iteritems():
      if tInfo['Used']:
        continue
      if tInfo['isProcIdle'] != 'Yes' and tInfo['state'] != 'Finished':
        return False
    return True

  def _processorsAreProcIdle( self, summary ):
    """ Return True in case all processors ('Used' or not Sim) transformations are procIdle or finished or not exist
    """
    for _tID, tInfo in summary['prods'].iteritems():
      if not tInfo['Used'] and tInfo['isSimulation']:
        continue
      if tInfo['isProcIdle'] != 'Yes' and tInfo['state'] != 'Finished' :
        return False
    return True

  def _mergersAreDone( self, summary ):
    """ Return True in case all mergers ('Used') transformations are finished or not exist
    """
    for _tID, tInfo in summary['prods'].iteritems():
      if not tInfo['Used']:
        continue
      if tInfo['state'] != 'Finished':
        return False
    return True

  def _mergersAreProcIdle( self, summary ):
    """ Return True in case all mergers ('Used') transformations are procIdle or finished or not exist
    """
    for _tID, tInfo in summary['prods'].iteritems():
      if not tInfo['Used']:
        continue
      if tInfo['isProcIdle'] != 'Yes' and tInfo['state'] != 'Finished' :
        return False
    return True

  def _requestedMoreThenProduced( self, tID, summary):
    """ Check that this transformation has registered less events than it was requested
    """
    if summary['prods'][tID]['Events'] < summary['prTotal']:
      self.log.verbose( " Transformation %s has produced less events, asking for extention " % tID )
      return True
    return False

  def _applyProductionRequestsLogic( self, updatedT, updatedPr ):
    """ Apply known logic to transformations related to production requests
        NOTE: we make decision based on BK statistic collected in the PREVIOUS cycle (except isReallyDone)
              and transformation status in THIS cycle BEFORE the Logic is started
    """
    self.log.verbose( "Production Requests logic..." )

    for prID, summary in self.prSummary.iteritems():
      countFinished = 0

      for tID, tInfo in summary['prods'].iteritems():
        if tInfo['state'] == 'Finished':
          # Do nothing with finished transformations
          countFinished += 1
        elif tInfo['state'] == 'Idle':
          if tInfo['isIdle'] == 'No':
            # 'Idle' && !isIdle() --> 'Active'
            self.__updateTransformationStatus( tID, 'Idle', 'Active', updatedT )
          elif tInfo['isIdle'] == 'Yes' and self._isReallyDone( summary ):
            if summary['type'] == 'Simulation':
              # 'Idle' && isIdle() && isDone for MC logic
              if tInfo['Used']:
                if self._producersAreIdle( summary ):
                  self.__updateTransformationStatus( tID, 'Idle', 'ValidatingOutput', updatedT )
                # else
                #  it can happened that MC is !isIdle()
              else:
                if self._mergersAreProcIdle( summary ):
                  # Note: 'isSimulation' should not be there (it should stay in 'Active')
                  self.__updateTransformationStatus( tID, 'Idle', 'ValidatingInput', updatedT )
                # else:
                #   We wait till mergers finish the job
            # else
            #  we do not know what to do with that (yet)
          # else  
          # 'Idle' && isIdle() (or unknown) && !isDone is not interesting combination
        elif tInfo['state'] == 'RemovedFiles':
          self.__updateTransformationStatus( tID, 'RemovedFiles', 'Completed', updatedT )
        elif tInfo['state'] == 'Active':
          if tInfo['isIdle'] == 'Yes':
            if summary['type'] == 'Simulation':
              # 'Active' && isIdle() for MC logic
              if tInfo['Used'] or not tInfo['isSimulation']:
                # The merger will either wait for MC extention (if !isDone)
                # or will start validation once producers are isIdle()
                self.__updateTransformationStatus( tID, 'Active', 'Idle', updatedT )
              else:
                # 'Active' && isIdle() && !Used && isSimulation
                if self._isReallyDone( summary ):                  
                  if self._mergersAreProcIdle( summary ):
                    self.__updateTransformationStatus( tID, 'Active', 'ValidatingInput', updatedT )
                elif self._processorsAreProcIdle( summary ) or self._requestedMoreThenProduced( tID, summary):
                  # we are not done yet, extend production
                  self.__updateTransformationStatus( tID, 'Active', 'Idle', updatedT )
                # else:
                #  we wait till the situation with mergers is clear
            else:
              # for not MC, use reasonable default
              self.__updateTransformationStatus( tID, 'Active', 'Idle', updatedT )
          # elif tInfo['isProcIdle'] == 'Yes'
          #   Should we do something there? For Sim prod that is not possible conditions since (isProcIdle == isIdle)
          # else:
          #  'Active' && ! isIdle() (or unknown) is not interesting
        elif tInfo['state'] == 'ValidatedOutput':
          if ( summary['type'] == 'Simulation' ) and summary['isDone'] and tInfo['Used']:
            self.__updateTransformationStatus( tID, 'ValidatedOutput', 'Completed', updatedT )
          else:
            self.log.warn( 'Logical bug: transformation %s unexpectadly has "ValidatedOutput"' & tID )
        elif tInfo['state'] == 'ValidatingInput':
          if ( summary['type'] == 'Simulation' ) and summary['isDone'] and not tInfo['Used']:
            self.__updateTransformationStatus( tID, 'ValidatingInput', 'RemovingFiles', updatedT )
          else:
            self.log.warn( 'Logical bug: transformation %s is unexpectadly "ValidatingInput"' & tID )
        elif tInfo['state'] == 'Testing':
          isIdle, isProcIdle, isSimulation = self.__isIdle( tID )
          self.log.verbose( "TransID %d, %s, %s, %s" % ( tID, isIdle, isProcIdle, isSimulation ) )
          if isIdle:
            self.__updateTransformationStatus( tID, 'Testing', 'Idle', updatedT )


      summary['isFinished'] = True if countFinished == len(summary['prods']) else False
      if summary['isFinished'] and not summary['master'] and summary['type'] == 'Simulation':
        self.__updateProductionRequestStatus( prID, 'Done', updatedPr )

    for masterID, prList in self.prMasters.iteritems():
      countFinished = 0
      for prID in prList:
        if self.prSummary[prID]['isFinished']:
          countFinished += 1
      if countFinished == len(prList) and summary['type'] == 'Simulation':
        self.__updateProductionRequestStatus( masterID, 'Done', updatedPr )

    self.log.verbose( "Done with Production Requests logic" )
