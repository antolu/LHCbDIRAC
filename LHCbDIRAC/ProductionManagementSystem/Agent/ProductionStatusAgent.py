"""  The ProductionStatusAgent monitors productions for active requests
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

     To do: review usage of production API(s) and refactor into Production Client
"""

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
  """ Usual DIRAC agent
  """

  def __init__( self, agentName, loadName, baseAgentName = False, properties = dict() ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param str loadName: load name of agent
    :param bool baseAgentName: whatever
    :param dict properties: whatever else
    """
    AgentModule.__init__( self, agentName, loadName, baseAgentName, properties )

    self.dProd = DiracProduction()
    self.dirac = Dirac()
    self.reqClient = RPCClient( 'ProductionManagement/ProductionRequest' )

    self.updatedProductions = {}
    self.updatedRequests = []

  #############################################################################
  def initialize( self ):
    """Sets default values.
    """
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    return S_OK()

  #############################################################################
  def execute( self ):
    """ The execution method, periodically checks productions for requests in the
        Active status.
    """
    self.updatedProductions = {}
    self.updatedRequests = []

    prodReqSummary, progressSummary = self._getProgress()

    prodValOutputs, prodValInputs, notDoneOutputs = self._evaluateProgress( prodReqSummary, progressSummary )

    #Check that there is something to do
    if not prodValOutputs.keys():
      self.log.info( 'No productions have yet reached the necessary number of BK events' )
      return S_OK()

    #Now have to update productions to ValidatingOutput / Input after cleaning jobs
    for prod, req in prodValOutputs.items():
      self.cleanActiveJobsUpdateStatus( prod, 'Active', 'ValidatingOutput' )
      #self.updateProductionStatus(prod,'Active','ValidatingOutput')

    for prod, req in prodValInputs.items():
      self.cleanActiveJobsUpdateStatus( prod, 'Active', 'ValidatingInput' )
      #self.updateProductionStatus(prod,'Active','ValidatingInput')

    #Select productions in ValidatedOutput and recheck #BK events
    #Either request is Done, productions Completed, MC prods go to RemoveInputs
    #or request is Active, productions Active, MC prods Active (and extended correctly)
    transformationClient = TransformationClient()
    res = transformationClient.getTransformationWithStatus( 'ValidatedOutput' )
    if not res['OK']:
      self.log.error( "Failed to get ValidatedOutput productions", res['Message'] )
      return res

    validatedOutput = res['Value']
    if not validatedOutput:
      self.log.info( 'No productions in ValidatedOutput status to process' )
      self.log.info( 'Productions updated this cycle:' )
      for n, v in self.updatedProductions.items():
        self.log.info( 'Production %s: %s => %s' % ( n, v['from'], v['to'] ) )
      self.mailProdManager()
      return S_OK()

    parentRequests = {}
    singleRequests = []
    for reqID, mdata in prodReqSummary.items():
      if not mdata['master']:
        singleRequests.append( reqID )
      else:
        parent = mdata['master']
        if parentRequests.has_key( parent ):
          reqs = parentRequests[parent]
          updated = reqs
          updated.append( reqID )
          parentRequests[parent] = updated
        else:
          parentRequests[parent] = [reqID]

    for req in parentRequests.keys():
      if req in singleRequests:
        singleRequests.remove( req )

    self.log.info( 'Single requests are: %s' % ( ', '.join( [str( i ) for i in singleRequests] ) ) )
    for n, v in parentRequests.items():
      self.log.info( 'Parent request %s has subrequests: %s' % ( n, ', '.join( [str( j ) for j in v] ) ) )

    self.log.info( 'The following productions are in ValidatedOutput status: %s' % ( ', '.join( [str( i ) for i in validatedOutput] ) ) )

    #All productions achieving #BK events will be in prodValOutputs dictionary
    returnToActive = []
    for prod in validatedOutput:
      if not prodValOutputs.has_key( prod ):
        if notDoneOutputs.has_key( prod ):
          unfinishedRequest = notDoneOutputs[prod]
          if unfinishedRequest in singleRequests:
            self.log.info( 'Prod %s for single request %s has not enough BK events' % ( prod, unfinishedRequest ) )
            returnToActive.append( prod )
        continue
      reqID = prodValOutputs[prod]
      if reqID in singleRequests:
        self.log.info( 'Production %s for single request ID %s has enough BK events after validation' % ( prod, reqID ) )
        #Note that this assumes again only 2 productions per request maximum
        mcProd = None
        for assocProd, req in prodValInputs.items():
          if req == reqID:
            mcProd = assocProd

        if mcProd:
          self.updateProductionStatus( mcProd, 'ValidatingInput', 'RemovingFiles' )

        self.updateProductionStatus( prod, 'ValidatedOutput', 'Completed' )
        self.updateAssociatedTransformation( prod )
        self.updateRequestStatus( reqID, 'Done' )

    completedParents = []
    for req, subReqs in parentRequests.items():
      finished = True
      toCheck = []
      notDoneProds = []
      for subreq in subReqs:
        for assocProd, valreq in prodValOutputs.items():
          if valreq == subreq:
            toCheck.append( assocProd )
        for notDoneProd, notDoneReq in notDoneOutputs.items():
          if notDoneReq == subreq:
            notDoneProds.append( notDoneProd )
      self.log.info( '==> Checking productions: %s for parent request %s' % ( ', '.join( [str( i ) for i in toCheck] ), req ) )
      for notDoneProd in notDoneProds:
        self.log.info( 'Production %s is part of parent request ID %s but does not have enough BK events' % ( notDoneProd, req ) )
        finished = False
      for prod in toCheck:
        if not prod in validatedOutput:
          self.log.info( 'Production %s is part of parent request ID %s but not yet in ValidatedOutput status' % ( prod, req ) )
          finished = False
        if not prodValOutputs.has_key( prod ):
          self.log.info( 'Production %s, request ID %s is in ValidatedOutput status but does not have enough BK events' % ( prod, req ) )
          finished = False
          returnToActive.append( prod )
      if finished and toCheck:
        completedParents.append( req )

    if completedParents:
      self.log.info( 'Completed parent requests to recheck BK events are: %s' % ( ', '.join( [str( i ) for i in completedParents] ) ) )

    for finishedReq in completedParents:
      if not parentRequests.has_key( finishedReq ):
        self.log.error( 'Req %s is not in list of completed parents' % ( finishedReq ) )
        continue
      subreqs = parentRequests[finishedReq]
      for subreq in subreqs:
        if not progressSummary.has_key( subreq ):
          self.log.error( 'Could not get production progress list for request %s:\n%s' % ( reqID, res ) )
          continue
        prodProgress = progressSummary[subreq]
        for prod, used in prodProgress.items():
          if used['Used']:
            self.updateProductionStatus( prod, 'ValidatedOutput', 'Completed' )
            self.updateAssociatedTransformation( prod )
          else:
            self.updateProductionStatus( prod, 'ValidatingInput', 'RemovingFiles' )

      self.updateRequestStatus( finishedReq, 'Done' )

    #Must return to active state the following Used productions (and associated MC prods)
    if returnToActive:
      self.log.info( 'Final productions to be returned to Active status are: %s' % ( ', '.join( [str( i ) for i in returnToActive] ) ) )

    mcReturnToActive = []
    for prod in returnToActive:
      for reqID, prodDict in progressSummary.items():
        if prod in prodDict.keys():
          for prodID, used in prodDict.items():
            if not prodDict['Used']:
              mcReturnToActive.append( prodID )

    if mcReturnToActive:
      self.log.info( 'Final MC productions to be returned to active are: %s' % ( ', '.join( [str( i ) for i in mcReturnToActive] ) ) )

    for prodID in returnToActive:
      self.updateProductionStatus( prodID, 'ValidatedOutput', 'Active' )
    for prodID in mcReturnToActive:
      self.updateProductionStatus( prodID, 'ValidatingInput', 'Active' )

    #Final action is to update MC input productions to completed that have been treated
    res = transformationClient.getTransformationWithStatus( 'RemovedFiles' )
    if not res['OK']:
      self.log.error( "Failed to get RemovedFiles productions", res['Message'] )
      return res
    if not res['Value']:
      self.log.info( 'No productions in RemovedFiles status' )
    else:
      for prod in res['Value']:
        self.updateProductionStatus( prod, 'RemovedFiles', 'Completed' )

    self.log.info( 'Productions updated this cycle:' )
    for n, v in self.updatedProductions.items():
      self.log.info( 'Production %s: %s => %s' % ( n, v['from'], v['to'] ) )

    if self.updatedRequests:
      self.log.info( 'Requests updated to Done status: %s' % ( ', '.join( [str( i ) for i in self.updatedRequests] ) ) )
    self.mailProdManager()
    return S_OK()

  #############################################################################

  def _getProgress( self ):
    ''' get production request summary and progress
    '''
    result = self.reqClient.getProductionRequestSummary( 'Active', 'Simulation' )
    if not result['OK']:
      self.log.error( 'Could not retrieve production request summary:\n%s\nwill be attempted on next execution cycle' % result )
      return S_OK()
    prodReqSummary = result['Value']

    result = self.reqClient.getAllProductionProgress()
    if not result['OK']:
      self.log.error( 'Could not retrieve production progress summary:\n%s\n will be attempted on next execution cycle' % result )
      return S_OK()
    progressSummary = result['Value']

    return prodReqSummary, progressSummary

  def _evaluateProgress( self, prodReqSummary, progressSummary ):
    ''' determines which prods should go in ValidatingInputs, ValidatingOutputs,
        and those for which nothing has to be done
    '''
    prodValOutputs = prodValInputs = notDoneOutputs = {}
    for reqID, mdata in prodReqSummary.iteritems():
      totalRequested = mdata['reqTotal']
      bkTotal = mdata['bkTotal']
      if not bkTotal:
        self.log.info( 'No events in BK for request ID %s' % ( reqID ) )
        continue
      progress = int( bkTotal * 100 / totalRequested )
      self.log.verbose( 'Request progress for ID %s is %s' % ( reqID, progress ) )
      if not progressSummary.has_key( reqID ):
        self.log.error( 'Could not get production progress list for request %s' % reqID )
        continue

      prodProgress = progressSummary[reqID]
      for prod, used in prodProgress.iteritems():
        if bkTotal >= totalRequested:
          if used['Used']:
            prodValOutputs[prod] = reqID
          else:
            prodValInputs[prod] = reqID
        else:
          if used['Used']:
            notDoneOutputs[prod] = reqID

    return prodValOutputs, prodValInputs, notDoneOutputs


  def updateAssociatedTransformation( self, prodID, status = 'Completed' ):
    """ This function checks for a production parameter "AssociatedTransformation"
        and if found will also update the transformation ID to the supplied status.
    """
    transformationClient = TransformationClient()
    result = transformationClient.getTransformationParameters( prodID, 'AssociatedTransformation' )
    if not result['OK']:
      self.log.info( 'No associated transformation found for productionID %s' % prodID )
      return S_OK()

    transID = int( result['Value'] )
    res = transformationClient.setTransformationParameter( transID, 'Status', status )
    if not res['OK']:
      self.log.error( "Failed to update status of transformation %s to %s" % ( transID, status ) )
    else:
      self.updatedProductions[transID] = {'to':status, 'from':'Active'}
    self.log.verbose( 'Changing status for transformation %s to %s' % ( transID, status ) )

    return S_OK()

  #############################################################################
  def mailProdManager( self ):
    """ Notify the production manager of the changes as productions should be
        manually extended in some cases.
    """
    if not self.updatedProductions and not self.updatedRequests:
      self.log.info( 'No changes this cycle, mail will not be sent' )
      return S_OK()

    notify = NotificationClient()
    subject = 'Production Status Updates ( %s )' % ( time.asctime() )
    msg = ['Productions updated this cycle:\n']
    for n, v in self.updatedProductions.items():
      msg.append( 'Production %s: %s => %s' % ( n, v['from'], v['to'] ) )
    msg.append( '\nRequests updated to Done status this cycle:\n' )
    msg.append( ', '.join( [str( i ) for i in self.updatedRequests] ) )
    res = notify.sendMail( 'vladimir.romanovsky@cern.ch', subject, '\n'.join( msg ), 'vladimir.romanovsky@cern.ch', localAttempt = False )
    if not res['OK']:
      self.log.error( res )
    else:
      self.log.info( 'Mail summary sent to production manager' )
    return S_OK()

  #############################################################################
  def updateRequestStatus( self, reqID, status ):
    """ This method updates the request status.
    """
    self.updatedRequests.append( reqID )
    #return S_OK()
    reqClient = RPCClient( 'ProductionManagement/ProductionRequest', useCertificates = False, timeout = 120 )
    result = reqClient.updateProductionRequest( long( reqID ), {'RequestState':status} )
    if not result['OK']:
      self.log.error( result )

    return result

  #############################################################################
  def cleanActiveJobsUpdateStatus( self, prodID, origStatus, status ):
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
      result = self.dProd.deleteProdJobs( waiting['Value'] )
      if not result['OK']:
        self.log.error( result )

    return self.updateProductionStatus( prodID, origStatus, status )

  #############################################################################
  def updateProductionStatus( self, prodID, origStatus, status ):
    """ This method updates the production status and logs the changes for each
        iteration of the agent.  Most importantly this method only allows status
        transitions based on what the original status should be.
    """
    transformationClient = TransformationClient()
    dProd = DiracProduction()
    result = dProd.getProduction( long( prodID ) )
    if not result['OK']:
      self.log.error( 'Could not update production status for %s to %s:\n%s' % ( prodID, status, result ) )
      return result

    currentStatus = result['Value']['Status']
    if currentStatus.lower() == origStatus.lower():
      #self.updatedProductions[prodID]={'to':status,'from':origStatus}
      #return S_OK()
      self.log.verbose( 'Changing status for prod %s from %s to %s' % ( prodID, currentStatus, origStatus ) )
      res = transformationClient.setTransformationParameter( prodID, 'Status', status )
      if not res['OK']:
        self.log.error( "Failed to update status of production %s from %s to %s" % ( prodID, origStatus, status ) )
      else:
        self.updatedProductions[prodID] = {'to':status, 'from':origStatus}
    elif currentStatus.lower() == status.lower():
      pass
    else:
      self.log.verbose( 'Production %s not updated to %s as it is currently in status %s' % ( prodID, status, currentStatus ) )

    return S_OK()
