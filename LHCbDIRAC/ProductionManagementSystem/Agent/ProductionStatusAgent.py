'''  The ProductionStatusAgent monitors productions for active requests
     and takes care to update their status. Initially this is just to handle
     simulation requests.

     Allowed production status transitions performed by this agent include:

     Active -> ValidatingInput
     Active -> ValidatingOutput

     ValidatedOutput -> Completed
     ValidatedOutput -> Active

     ValidatingInput -> Active
     ValidatingInput -> RemovingFiles

     RemovedFiles -> Completed

     In addition this also updates request status from Active to Done.

     To do: review usage of production API(s) and re-factor into Production Client
'''

__RCSID__ = "$Id$"

import time
from DIRAC                                                      import S_OK
from DIRAC.Core.Base.AgentModule                                import AgentModule
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.Interfaces.API.Dirac                                 import Dirac
from DIRAC.FrameworkSystem.Client.NotificationClient            import NotificationClient

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.Interfaces.API.DiracProduction                   import DiracProduction


class ProductionStatusAgent( AgentModule ):
  ''' Usual DIRAC agent
  '''

  def __init__( self, agentName, loadName, baseAgentName = False, properties = dict() ):
    ''' c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param str loadName: load name of agent
    :param bool baseAgentName: whatever
    :param dict properties: whatever else
    '''
    AgentModule.__init__( self, agentName, loadName, baseAgentName, properties )

    self.dProd = DiracProduction()
    self.dirac = Dirac()
    self.reqClient = RPCClient( 'ProductionManagement/ProductionRequest' )
    self.transformationClient = TransformationClient()


  #############################################################################
  def initialize( self ):
    '''Sets default values.
    '''
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    return S_OK()

  #############################################################################
  def execute( self ):
    ''' The execution method, periodically checks productions for requests in the
        Active status.
    '''
    updatedProductions = {}
    updatedRequests = []

    prodReqSummary, progressSummary = self._getProgress()
    doneAndUsed, doneAndNotUsed, _notDoneAndUsed, _notDoneAndNotUsed = self._evaluateProgress( prodReqSummary,
                                                                                               progressSummary )

    self.log.info( "******************************" )
    self.log.info( "Checking for Requests to close" )
    self.log.info( "******************************" )

    try:
      prodsList = self.__getTransformations( 'Completed' )
      if prodsList:
        reqsMap = self._getReqsMap( prodReqSummary, progressSummary )
        for masterReq, reqs in reqsMap.iteritems():
          allProds = []
          for _req, prods in reqs.iteritems():
            allProds = allProds + prods
          if set( allProds ) < set( prodsList ):
            self._updateRequestStatus( masterReq, 'Done', updatedRequests )
    except RuntimeError, error:
      self.log.error( error )


    self.log.info( "**************************************" )
    self.log.info( "Checking for RemovedFiles -> Completed" )
    self.log.info( "**************************************" )

    try:
      prods = self.__getTransformations( 'RemovedFiles' )
      if prods:
        for prod in prods:
          self._updateProductionStatus( prod, 'RemovedFiles', 'Completed', updatedProductions )
    except RuntimeError, error:
      self.log.error( error )


    self.log.info( "*********************************************************************************************" )
    self.log.info( "Checking for ValidatedOutput -> Completed/Active and ValidatingInputs -> RemovingFiles/Active" )
    self.log.info( "*********************************************************************************************" )

    try:
      prods = self.__getTransformations( 'ValidatedOutput' )
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
      prods = self.__getTransformations( 'ValidatingInput' )
      if prods:
        for prod in prods:
          if prod not in doneAndNotUsed:
            self.log.info( 'Production %d is returned to Active status' % prod )
            self._updateProductionStatus( prod, 'ValidatingInput', 'Active', updatedProductions )
          else:
            self.log.info( 'Production %d is put in Completed status' % prod )
            self._updateProductionStatus( prod, 'ValidatingInput', 'Completed', updatedProductions )
    except RuntimeError, error:
      self.log.error( error )

    self.log.info( "*********************************************************************" )
    self.log.info( "Checking for Active -> ValidatingInput and Active -> ValidatingOutput" )
    self.log.info( "*********************************************************************" )

    try:
      prods = self.__getTransformations( 'Active' )
      if prods:
        if not doneAndUsed:
          self.log.info( 'No productions have yet reached the necessary number of BK events' )
        else:
          #Now have to update productions to ValidatingOutput / Input after cleaning jobs
          for prod in prods:
            if prod in doneAndUsed:
              self._cleanActiveJobs( prod )
              self._updateProductionStatus( prod, 'Active', 'ValidatingOutput', updatedProductions )
            elif prod in doneAndNotUsed:
              self._cleanActiveJobs( prod )
              self._updateProductionStatus( prod, 'Active', 'ValidatingInput', updatedProductions )
    except RuntimeError, error:
      self.log.error( error )

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

  def __getTransformations( self, status ):
    ''' dev function. Get the transformations (print info in the meanwhile)
    '''

    res = self.transformationClient.getTransformationWithStatus( status )
    if not res['OK']:
      self.log.error( "Failed to get %s productions: %s" % ( status, res['Message'] ) )
      raise RuntimeError, "Failed to get %s productions: %s" % ( status, res['Message'] )
    if not res['Value']:
      self.log.info( 'No productions in %s status' % status )
      return []
    else:
      valOutStr = ', '.join( [str( i ) for i in res['Value']] )
      self.log.verbose( 'The following productions are in %s status: %s' % ( status, valOutStr ) )
      return res['Value']


  def _getProgress( self ):
    ''' get production request summary and progress
    '''
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

  def _evaluateProgress( self, prodReqSummary, progressSummary ):
    ''' determines which prods have reached the number of events requested and which didn't
    '''
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

  def _getReqsMap( self, prodReqSummary, progressSummary ):
    ''' just create a dict with all the requests (master -> subRequets and
    '''
    reqsMap = {}
    for request, mData in prodReqSummary.iteritems():
      masterReq = mData['master']
      if not masterReq:
        masterReq = request
      try:
        reqs = reqsMap[masterReq]
      except KeyError:
        reqs = {}
      reqs.update( {request:progressSummary[request].keys()} )
      reqsMap.update( {masterReq: reqs} )

    return reqsMap

  def _mailProdManager( self, updatedProductions, updatedRequests ):
    ''' Notify the production manager of the changes as productions should be
        manually extended in some cases.
    '''
    if not updatedProductions and not updatedRequests:
      self.log.info( 'No changes this cycle, mail will not be sent' )
      return S_OK()

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
    return S_OK()

  def _updateRequestStatus( self, reqID, status, updatedRequests ):
    ''' This method updates the request status.
    '''
    updatedRequests.append( reqID )

    reqClient = RPCClient( 'ProductionManagement/ProductionRequest', useCertificates = False, timeout = 120 )
    result = reqClient.updateProductionRequest( long( reqID ), {'RequestState':status} )
    if not result['OK']:
      self.log.error( result )

    return result

  def _cleanActiveJobs( self, prodID ):
    ''' This method checks if a production having enough BK events has any Waiting
        or Running jobs in the WMS and removes them to ensure the output data sample
        is static.  Then the production status is updated.
    '''
    running = self.dProd.selectProductionJobs( int( prodID ), Status = 'Running' )
    if running['OK']:
      self.log.info( 'Killing %s running jobs for production %s' % ( len( running['Value'] ), prodID ) )
      result = self.dirac.kill( running['Value'] )
      if not result['OK']:
        self.log.error( result )

    waiting = self.dProd.selectProductionJobs( int( prodID ), Status = 'Waiting' )
    if waiting['OK']:
      self.log.info( 'Deleting %s waiting jobs for production %s' % ( len( waiting['Value'] ), prodID ) )
      result = self.dProd.deleteProdJobs( waiting['Value'] )
      if not result['OK']:
        self.log.error( result )

  def _updateProductionStatus( self, prodID, origStatus, status, updatedProductions ):
    ''' This method updates the production status and logs the changes for each
        iteration of the agent.  Most importantly this method only allows status
        transitions based on what the original status should be.
    '''
    result = self.dProd.getProduction( long( prodID ) )
    if not result['OK']:
      self.log.error( 'Could not update production status for %s to %s:\n%s' % ( prodID, status, result ) )
    else:
      currentStatus = result['Value']['Status']
      if currentStatus.lower() == origStatus.lower():
        self.log.verbose( 'Changing status for prod %s from %s to %s' % ( prodID, currentStatus, origStatus ) )
        res = self.transformationClient.setTransformationParameter( prodID, 'Status', status )
        if not res['OK']:
          self.log.error( "Failed to update status of production %s from %s to %s" % ( prodID, origStatus, status ) )
        else:
          updatedProductions[prodID] = {'to':status, 'from':origStatus}
      elif currentStatus.lower() == status.lower():
        pass
      else:
        self.log.verbose( 'Production %s not updated to %s as it is currently in status %s' % ( prodID, status,
                                                                                                currentStatus ) )
