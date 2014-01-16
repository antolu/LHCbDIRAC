"""  The ProductionStatusAgent monitors productions for active requests
     and takes care to update their status. Initially this is just to handle
     simulation requests.

     Allowed production status transitions performed by this agent include:

     Idle -> ValidatingInput
     Idle -> ValidatingOutput

     ValidatedOutput -> Completed
     ValidatedOutput -> Active

     ValidatingInput -> Active
     ValidatingInput -> RemovingFiles

     RemovedFiles -> Completed

     Active -> Idle

     In addition this also updates request status from Active to Done.

     To do: review usage of production API(s) and re-factor into Production Client
"""

__RCSID__ = "$Id$"

import time
from DIRAC                                                      import S_OK
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.Interfaces.API.Dirac                                 import Dirac
from DIRAC.FrameworkSystem.Client.NotificationClient            import NotificationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations        import Operations

from LHCbDIRAC.TransformationSystem.Client.TransformationClient  import TransformationClient
from LHCbDIRAC.Interfaces.API.DiracProduction                    import DiracProduction


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
    AgentModule.__init__( self, *args, **kwargs )

    self.dProd = DiracProduction()
    self.dirac = Dirac()
    self.reqClient = RPCClient( 'ProductionManagement/ProductionRequest' )
    self.transClient = TransformationClient()
    self.simulationTypes = Operations().getValue( 'Transformations/ExtendableTransfTypes', ['MCSimulation',
                                                                                            'Simulation'] )
    self.notify = True

  #############################################################################
  def initialize( self ):
    """Sets default values.
    """
    self.am_setOption( 'shifterProxy', 'ProductionManager' )
    self.notify = eval( self.am_getOption( 'NotifyProdManager', 'True' ) )

    return S_OK()

  #############################################################################
  def execute( self ):
    """ The execution method, periodically checks productions for requests in the
        Active status.
    """
    updatedProductions = {}
    updatedRequests = []

    prodReqSummary, progressSummary = self._getProgress()
    doneAndUsed, doneAndNotUsed, _notDoneAndUsed, _notDoneAndNotUsed = self._evaluateProgress( prodReqSummary,
                                                                                               progressSummary )

    self.log.info( "******************************" )
    self.log.info( "Checking for Requests to close" )
    self.log.info( "******************************" )

    try:
      prodsListCompleted = self._getTransformations( 'Completed' )
      prodsListArchived = self._getTransformations( 'Archived' )
      prodsList = prodsListCompleted + prodsListArchived
      if prodsList:
        reqsMap = self._getReqsMap( prodReqSummary, progressSummary )
        for masterReq, reqs in reqsMap.iteritems():
          allProds = []
          for _req, prods in reqs.iteritems():
            allProds = allProds + prods
          if allProds and ( set( allProds ) < set( prodsList ) ):
            self._updateRequestStatus( masterReq, 'Done', updatedRequests )
    except RuntimeError, error:
      self.log.error( error )


    self.log.info( "**************************************" )
    self.log.info( "Checking for RemovedFiles -> Completed" )
    self.log.info( "**************************************" )

    try:
      prods = self._getTransformations( 'RemovedFiles' )
      if prods:
        for prod in prods:
          self._updateProductionStatus( prod, 'RemovedFiles', 'Completed', updatedProductions )
    except RuntimeError, error:
      self.log.error( error )

    self.log.info( "***************************" )
    self.log.info( "Checking for Active -> Idle" )
    self.log.info( "***************************" )

    self._checkActiveToIdle( updatedProductions )

    self.log.info( "*********************************************************************************************" )
    self.log.info( "Checking for ValidatedOutput -> Completed/Active and ValidatingInputs -> RemovingFiles/Active" )
    self.log.info( "*********************************************************************************************" )

    try:
      prods = self._getTransformations( 'ValidatedOutput' )
      if prods:
        for prod in prods:
          if prod not in doneAndUsed:
            self.log.info( 'Production %d is returned to Active status' % prod )
            self._updateProductionStatus( prod, 'ValidatedOutput', 'Active', updatedProductions )
          else:
            self.log.info( 'Production %d is put in Completed status' % prod )
            self._updateProductionStatus( prod, 'ValidatedOutput', 'Completed', updatedProductions )
    except RuntimeError, error:
      self.log.error( error )

    try:
      prods = self._getTransformations( 'ValidatingInput' )
      if prods:
        for prod in prods:
          if prod not in doneAndNotUsed:
            self.log.info( 'Production %d is returned to Active status' % prod )
            self._updateProductionStatus( prod, 'ValidatingInput', 'Active', updatedProductions )
          else:
            self.log.info( 'Production %d is put in RemovingFiles status' % prod )
            self._updateProductionStatus( prod, 'ValidatingInput', 'RemovingFiles', updatedProductions )
    except RuntimeError, error:
      self.log.error( error )

    self.log.info( "*****************************************************************" )
    self.log.info( "Checking for Idle -> ValidatingInput and Idle -> ValidatingOutput" )
    self.log.info( "*****************************************************************" )

    self._checkIdleToValidatingInputAndValidatingOutput( doneAndUsed, doneAndNotUsed, updatedProductions )

    self.log.info( "***************************" )
    self.log.info( "Checking for Idle -> Active" )
    self.log.info( "***************************" )
    self._checkIdleToActive( updatedProductions )

    self.log.info( "*********" )
    self.log.info( "Reporting" )
    self.log.info( "*********" )

    self.log.info( 'Productions updated this cycle:' )
    for n, v in updatedProductions.items():
      self.log.info( 'Production %s: %s => %s' % ( n, v['from'], v['to'] ) )

    if updatedRequests:
      self.log.info( 'Requests updated to Done status: %s' % ( ', '.join( [str( i ) for i in updatedRequests] ) ) )
    self._mailProdManager( updatedProductions, updatedRequests )
    return S_OK()

  #############################################################################

  def _checkActiveToIdle( self, updatedProductions ):
    try:
      prods = self._getTransformations( 'Active' )
      if prods:
        for prod in prods:
          isIdle = self.__isIdle( prod )
          if isIdle:
            self.log.info( 'Production %d is put in Idle status' % prod )
            self._updateProductionStatus( prod, 'Active', 'Idle', updatedProductions )
    except RuntimeError, error:
      self.log.error( error )

  def _checkIdleToActive( self, updatedProductions ):
    """ Inverse of _checkActiveToIdle
    """
    try:
      prods = self._getTransformations( 'Idle' )
      if prods:
        for prod in prods:
          isIdle = self.__isIdle( prod )
          if not isIdle:
            self.log.info( 'Production %d is put in Active status' % prod )
            self._updateProductionStatus( prod, 'Idle', 'Active', updatedProductions )
    except RuntimeError, error:
      self.log.error( error )

  def __isIdle( self, prod ):
    """ Cheks if a production is idle
    """
    self.log.verbose( "Checking production %d" % prod )
    prodInfo = self.transClient.getTransformation( prod )['Value']
    if prodInfo.get( 'Type', None ) in self.simulationTypes:
      # simulation : go to Idle if
      # only failed and done jobs
      # AND number of tasks created in total == number of tasks submitted
      prodStats = self._getTransformationTaskStats( prod )
      self.log.debug( "Stats: %s" % str( prodStats ) )
      isIdle = ( prodStats.get( 'TotalCreated', 0 ) == prodStats.get( 'Submitted', 0 ) ) \
      and all( [prodStats.get( status, 0 ) == 0 for status in ['Checking', 'Completed', 'Created', 'Matched',
                                                               'Received', 'Reserved', 'Rescheduled', 'Running',
                                                               'Waiting' ]] )
    else:
      # other production type : go to Idle if
      # 0 unused, 0 assigned files
      # AND > 0 processed files
      prodStats = self._getTransformationFilesStats( prod )
      self.log.debug( "Stats: %s" % str( prodStats ) )
      isIdle = ( prodStats.get( 'Processed', 0 ) > 0 ) \
      and all( [prodStats.get( status, 0 ) == 0 for status in ['Unused', 'Assigned']] )

    return isIdle

  def _checkIdleToValidatingInputAndValidatingOutput( self, doneAndUsed, doneAndNotUsed, updatedProductions ):
    try:
      prods = self._getTransformations( 'Idle' )
      if prods:
        if not doneAndUsed:
          self.log.info( 'No productions have yet reached the necessary number of BK events' )
        else:
          # Now have to update productions to ValidatingOutput / Input after cleaning jobs
          for prod in prods:
            if prod in doneAndUsed:
              self._cleanActiveJobs( prod )
              self._updateProductionStatus( prod, 'Idle', 'ValidatingOutput', updatedProductions )
            elif prod in doneAndNotUsed:
              self._cleanActiveJobs( prod )
              self._updateProductionStatus( prod, 'Idle', 'ValidatingInput', updatedProductions )
    except RuntimeError, error:
      self.log.error( error )

  #############################################################################

  def _getTransformations( self, status ):
    """ dev function. Get the transformations (print info in the meanwhile)
    """

    res = self.transClient.getTransformationWithStatus( status )
    if not res['OK']:
      self.log.error( "Failed to get %s productions: %s" % ( status, res['Message'] ) )
      raise RuntimeError( "Failed to get %s productions: %s" % ( status, res['Message'] ) )
    if not res['Value']:
      self.log.info( 'No productions in %s status' % status )
      return []
    else:
      valOutStr = ', '.join( [str( i ) for i in res['Value']] )
      self.log.verbose( 'The following productions are in %s status: %s' % ( status, valOutStr ) )
      return res['Value']


  def _getProgress( self ):
    """ get production request summary and progress
    """
    result = self.reqClient.getProductionRequestSummary( 'Active', 'Simulation' )
    if not result['OK']:
      self.log.error( 'Could not retrieve production request summary: %s' % result['Message'] )
      prodReqSummary = {}
    else:
      prodReqSummary = result['Value']

    result = self.reqClient.getAllProductionProgress()
    if not result['OK']:
      self.log.error( 'Could not retrieve production progress summary: %s' % result['Message'] )
      progressSummary = {}
    else:
      progressSummary = result['Value']

    return prodReqSummary, progressSummary

  def _getTransformationTaskStats( self, transformationID ):
    """ get the stats for a transformation tasks (number of tasks in each status)
    """

    result = self.transClient.getTransformationTaskStats( transformationID )
    if not result['OK']:
      self.log.error( 'Could not retrieve transformation tasks stats: %s' % result['Message'] )
      transformationStats = {}
    else:
      transformationStats = result['Value']

    return transformationStats

  def _getTransformationFilesStats( self, transformationID ):
    """ get the stats for a transformation files (number of files in each status)
    """

    result = self.transClient.getTransformationStats( transformationID )
    if not result['OK']:
      self.log.error( 'Could not retrieve transformation files stats: %s' % result['Message'] )
      transformationStats = {}
    else:
      transformationStats = result['Value']

    return transformationStats

  def _evaluateProgress( self, prodReqSummary, progressSummary ):
    """ determines which prods have reached the number of events requested and which didn't
    """
    doneAndUsed = {}
    doneAndNotUsed = {}
    notDoneAndUsed = {}
    notDoneAndNotUsed = {}

    for reqID, mdata in prodReqSummary.iteritems():
      totalRequested = mdata['reqTotal']
      bkTotal = mdata['bkTotal']
      if not bkTotal:
        continue
      progress = int( bkTotal * 100 / totalRequested )
      self.log.debug( 'Request progress for ID %s is %s%%' % ( reqID, progress ) )

      try:
        for prod, used in progressSummary[reqID].iteritems():
          if bkTotal >= totalRequested:
            if used['Used']:
              doneAndUsed[prod] = reqID
            else:
              doneAndNotUsed[prod] = reqID
          else:
            if used['Used']:
              notDoneAndUsed[prod] = reqID
            else:
              notDoneAndNotUsed[prod] = reqID
      except KeyError:
        self.log.error( 'Could not get production progress list for request %s' % reqID )
        continue

    return doneAndUsed, doneAndNotUsed, notDoneAndUsed, notDoneAndNotUsed

  def _getReqsMap( self, prodReqS, progressS ):
    """ just create a dict with all the requests (master -> subRequets and the prods of each)
    """
    reqsMap = dict()
    for request, mData in dict( prodReqS ).iteritems():
      masterReq = mData['master']
      if not masterReq:
        masterReq = request
      reqs = dict()
      prods = progressS[request].keys()
      reqs.setdefault( request, prods )
      try:
        reqsMap[masterReq].update( reqs )
      except:
        reqsMap.setdefault( masterReq, reqs )

    return reqsMap

  def _mailProdManager( self, updatedProductions, updatedRequests ):
    """ Notify the production manager of the changes as productions should be
        manually extended in some cases.
    """
    if not updatedProductions and not updatedRequests:
      self.log.info( 'No changes this cycle, mail will not be sent' )

    if self.notify:
      notify = NotificationClient()
      subject = 'Production Status Updates ( %s )' % ( time.asctime() )
      msg = ['Productions updated this cycle:\n']
      for prod, val in updatedProductions.iteritems():
        msg.append( 'Production %s: %s => %s' % ( prod, val['from'], val['to'] ) )
      msg.append( '\nRequests updated to Done status this cycle:\n' )
      msg.append( ', '.join( [str( i ) for i in updatedRequests] ) )
      res = notify.sendMail( 'vladimir.romanovsky@cern.ch', subject, '\n'.join( msg ),
                             'vladimir.romanovsky@cern.ch', localAttempt = False )
      if not res['OK']:
        self.log.error( res )
      else:
        self.log.info( 'Mail summary sent to production manager' )

  def _updateRequestStatus( self, reqID, status, updatedRequests ):
    """ This method updates the request status.
    """
    updatedRequests.append( reqID )

    reqClient = RPCClient( 'ProductionManagement/ProductionRequest', useCertificates = False, timeout = 120 )
    result = reqClient.updateProductionRequest( long( reqID ), {'RequestState':status} )
    if not result['OK']:
      self.log.error( result )

    return result

  def _cleanActiveJobs( self, prodID ):
    """ This method checks if a production having enough BK events has any Waiting
        or Running jobs in the WMS and removes them to ensure the output data sample
        is static.  Then the production status is updated.
    """
    running = self.dProd.selectProductionJobs( int( prodID ), Status = 'Running' )
    if running['OK']:
      self.log.info( 'Killing %s running jobs for production %s' % ( len( running['Value'] ), prodID ) )
      result = self.dirac.kill( running['Value'] )
      if not result['OK']:
        self.log.error( result )

    waiting = self.dProd.selectProductionJobs( int( prodID ), Status = 'Waiting' )
    if waiting['OK']:
      self.log.info( 'Deleting %s waiting jobs for production %s' % ( len( waiting['Value'] ), prodID ) )
      result = self.dProd.delete( waiting['Value'] )
      if not result['OK']:
        self.log.error( result )

  def _updateProductionStatus( self, prodID, origStatus, status, updatedProductions ):
    """ This method updates the production status and logs the changes for each
        iteration of the agent.  Most importantly this method only allows status
        transitions based on what the original status should be.
    """
    self.log.verbose( 'Changing status for prod %s to %s' % ( prodID, status ) )
    res = self.transClient.setTransformationParameter( prodID, 'Status', status )
    if not res['OK']:
      self.log.error( "Failed to update status of production %s from %s to %s" % ( prodID, origStatus, status ) )
    else:
      updatedProductions[prodID] = {'to':status, 'from':origStatus}
