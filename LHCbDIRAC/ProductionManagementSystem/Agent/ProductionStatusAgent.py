########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/Agent/ProductionStatusAgent.py,v 1.4 2009/09/11 15:45:17 paterson Exp $
########################################################################

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

__RCSID__   = "$Id: ProductionStatusAgent.py,v 1.4 2009/09/11 15:45:17 paterson Exp $"
__VERSION__ = "$Revision: 1.4 $"

from DIRAC                                                     import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.Core.Base.AgentModule                               import AgentModule
from DIRAC.Core.Utilities.Shifter                              import setupShifterProxyInEnv
from DIRAC.Core.DISET.RPCClient                                import RPCClient
from DIRAC.Interfaces.API.DiracProduction                      import DiracProduction
from DIRAC.LHCbSystem.Client.Production                        import Production
from DIRAC.Interfaces.API.Dirac                                import Dirac
from DIRAC.ProductionManagementSystem.Client.ProductionClient  import ProductionClient
from DIRAC.FrameworkSystem.Client.NotificationClient           import NotificationClient

import string,time

AGENT_NAME = 'ProductionManagement/ProductionStatusAgent'

class ProductionStatusAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """Sets default values.
    """
    self.am_setOption('PollingTime',2*60*60)
    self.am_setModuleParam("shifterProxy", "ProductionManager")
    self.am_setModuleParam("shifterProxyLocation","%s/runit/%s/proxy" % (rootPath,AGENT_NAME))
    #Agent parameters
    self.targetPercentage = self.am_getOption('TargetPercentage',102)
    self.updatedProductions = {}
    self.updatedRequests = []
    return S_OK()

  #############################################################################
  def execute(self):
    """ The execution method, periodically checks productions for requests in the
        Active status.
    """
    self.updatedProductions = {}
    self.updatedRequests = []
    reqClient = RPCClient('ProductionManagement/ProductionRequest',timeout=120)
    prodAPI=Production()
    result =  reqClient.getProductionRequestSummary('Active','Simulation')
    if not result['OK']:
      self.log.error('Could not retrieve production request summary:\n%s\nwill be attempted on next execution cycle' %result)
      return S_OK()

    prodReqSummary = result['Value']
    prodValOutputs = {}
    prodValInputs = {}
    notDone = {}
    for reqID,mdata in prodReqSummary.items():
      totalRequested = mdata['reqTotal']
      bkTotal = mdata['bkTotal']
      if not bkTotal:
        self.log.info('No events in BK for request ID %s' %(reqID))
        continue
      progress = int(bkTotal*100/totalRequested)
      self.log.verbose('Request progress for ID %s is %s' %(reqID,progress))
      res = reqClient.getProductionProgressList(long(reqID))
      if not res['OK']:
        self.log.error('Could not get production progress list for request %s:\n%s' %(reqID,res))
        continue
      prodDictList = res['Value']['Rows']
      if len(prodDictList)>2:
        self.log.error('More than 2 associated productions for request %s, ignoring from further consideration...' %reqID)
        continue
      for p in prodDictList:
        prod = p['ProductionID']
        if progress > int(self.targetPercentage):
          if p['Used']:
            prodValOutputs[prod]=reqID
          else:
            prodValInputs[prod]=reqID
        else:
          notDone[prod]=reqID

    #Check that there is something to do
    if not prodValOutputs.keys():
      self.log.info('No productions have yet reached the necessary number of BK events')
      return S_OK()

    #Now we have the production IDs to change, need to ensure parameters are present before
    #proceeding with other actions, this is temporary (will be added at creation time normally)
    toRemove = []
    for production in prodValOutputs.keys():
      result = self.setProdParams(production)
      if not result['OK']:
        toRemove.append(production)

    for p in toRemove: del prodValOutputs[p]

    toRemove = []
    for production in prodValInputs.keys():
      result = self.setProdParams(production)
      if not result['OK']:
        toRemove.append(production)

    for p in toRemove: del prodValInputs[p]

    #Now have to update productions to ValidatingOutput / Input after cleaning jobs
    for prod,req in prodValOutputs.items():
      self.cleanActiveJobsUpdateStatus(prod,'Active','ValidatingOutput')

    for prod,req in prodValInputs.items():
      self.cleanActiveJobsUpdateStatus(prod,'Active','ValidatingInput')

    #Select productions in ValidatedOutput and recheck #BK events
    #Either request is Done, productions Completed, MC prods go to RemoveInputs
    #or request is Active, productions Active, MC prods Active (and extended correctly)
    productionClient = ProductionClient()
    res = productionClient.getProductionsWithStatus('ValidatedOutput')
    if not res['OK']:
      gLogger.error("Failed to get ValidatedOutput productions",res['Message'])
      return res

    validatedOutput = res['Value']
    if not validatedOutput:
      self.log.info('No productions in ValidatedOutput status to process')
      return S_OK()

    parentRequests = {}
    singleRequests = []
    for reqID,mdata in prodReqSummary.items():
      if not mdata['master']:
        singleRequests.append(reqID)
      else:
        parent = mdata['master']
        if parentRequests.has_key(parent):
          reqs = parentRequests[parent]
          updated = reqs
          updated.append(reqID)
          parentRequests[parent]=updated
        else:
          parentRequests[parent] = [reqID]

    for req in parentRequests.keys():
      if req in singleRequests:
        singleRequests.remove(req)

    self.log.info('Single requests are: %s' %(string.join([str(i) for i in singleRequests],', ')))
    for n,v in parentRequests.items():
      self.log.info('Parent request %s has subrequests: %s' %(n,string.join([str(j) for j in v],', ')))

    self.log.info('The following productions are in ValidatedOutput status: %s' %(string.join([str(i) for i in validatedOutput],', ')))

    #All poductions achieving #BK events will be in prodValOutputs dictionary
    returnToActive = []
    for prod in validatedOutput:
      if not prodValOutputs.has_key(prod):
        if notDone.has_key(prod):
          unfinishedRequest = notDone[prod]
          if unfinishedRequest in singleRequests:
            self.log.info('Prod %s for single request %s has not enough BK events' %(prod,unfinishedRequest))
            returnToActive.append(prod)
        continue
      reqID = prodValOutputs[prod]
      if reqID in singleRequests:
        self.log.info('Production %s for single request ID %s has enough BK events after validation' %(prod,reqID))
        #Note that this assumes again only 2 productions per request maximum
        mcProd = None
        for assocProd,req in prodValInputs.items():
          if req==reqID:
            mcProd = assocProd

        if mcProd:
          self.updateProductionStatus(mcProd,'ValidatingInput','RemovingFiles')

        self.updateProductionStatus(prod,'ValidatedOutput','Completed')
        self.updateRequestStatus(reqID,'Done')

    completedParents = []
    for req,subReqs in parentRequests.items():
      finished = True
      toCheck = []
      for subreq in subReqs:
        for assocProd,valreq in prodValOutputs.items():
          if valreq==subreq:
            toCheck.append(assocProd)
      self.log.info('==> Checking productions: %s for parent request %s' %(string.join([str(i) for i in toCheck],', '),req))
      for prod in toCheck:
        if not prod in validatedOutput:
          self.log.info('Production %s is part of parent request ID %s but not yet in ValidatedOutput status' %(prod,req))
          finished=False
        if not prodValOutputs.has_key(prod):
          self.log.info('Production %s, request ID %s is in ValidatedOutput status but does not have enough BK events' %(prod,req))
          finished=False
          returnToActive.append(prod)
      if finished and toCheck:
        completedParents.append(req)

    if completedParents:
      self.log.info('Completed parent requests to recheck BK events are: %s' %(string.join([str(i) for i in completedParents],', ')))

    for finishedReq in completedParents:
      if not parentRequests.has_key(finishedReq):
        self.log.error('Req %s is not in list of completed parents' %(finishedReq))
        continue
      subreqs = parentRequests[finishedReq]
      for subreq in subreqs:
        mcProd=None
        outputProd=None
        for assocProd,req in prodValInputs.items():
          if req==finishedReq:
            mcProd = assocProd
        for prod,req in prodValOutputs.items():
          if req==finishedReq:
            outputProd = prod

        if mcProd:
          self.updateProductionStatus(mcProd,'ValidatingInput','RemovingFiles')
        if outputProd:
          self.updateProductionStatus(prod,'ValidatedOutput','Completed')
      self.updateRequestStatus(finishedReq,'Done')

    #Must return to active state the following Used productions (and associated MC prods)
    if returnToActive:
      self.log.info('Final productions to be returned to Active status are: %s' %(string.join([str(i) for i in returnToActive],', ')))

    mcReturnToActive = []
    for reqID,mdata in prodReqSummary.items():
      res = reqClient.getProductionProgressList(long(reqID))
      if not res['OK']:
        self.log.error('Could not get production progress list for request %s:\n%s' %(reqID,res))
        continue
      for prod in returnToActive:
        prodDictList = res['Value']['Rows']
        mcProd = None
        match = False
        for p in prodDictList:
          prodReq = p['ProductionID']
          if prodReq == prod:
            self.log.info('Found associated MC production for production %s' %prod)
            match = True
          else:
            mcProd = prodReq
        if match:
          mcReturnToActive.append(mcProd)

    if mcReturnToActive:
      self.log.info('Final MC productions to be returned to active are: %s' %(string.join([str(i) for i in mcReturnToActive],', ')))

    for prodID in returnToActive:
      self.updateProductionStatus(prodID,'ValidatedOutput','Active')
    for prodID in mcReturnToActive:
      self.updateProductionStatus(prodID,'ValidatingInput','Active')

    #Final action is to update MC input productions to completed that have been treated
    res = productionClient.getProductionsWithStatus('RemovedFiles')
    if not res['OK']:
      gLogger.error("Failed to get RemovedFiles productions",res['Message'])
      return res
    if not res['Value']:
      self.log.info('No productions in RemovedFiles status')
    else:
      for prod in res['Value'].items():
        self.updateProductionStatus(prod,'RemovedFiles','Completed')

    self.log.info('Productions updated this cycle:')
    for n,v in self.updatedProductions.items():
      self.log.info('Production %s: %s => %s' %(n,v['from'],v['to']))

    self.log.info('Requests updated to Done status: %s' %(string.join([str(i) for i in self.updatedRequests],', ')))
    self.mailProdManager()
    return S_OK()

  #############################################################################
  def mailProdManager(self):
    """ Notify the production manager of the changes as productions should be
        manually extended in some cases.
    """
    if not self.updatedProductions and not self.updatedRequests:
      self.log.info('No changes this cycle, mail will not be sent')
      return S_OK()

    notify = NotificationClient()
    subject = 'Production Status Updates ( %s )' %(time.asctime())
    msg = ['Productions updated this cycle:\n']
    for n,v in self.updatedProductions.items():
      msg.append('Production %s: %s => %s' %(n,v['from'],v['to']))
    msg.append('\nRequests updated to Done status this cycle:\n')
    msg.append(string.join([str(i) for i in self.updatedRequests],', '))
    res = notify.sendMail('stuart.paterson@cern.ch',subject,string.join(msg,'\n'),'stuart.paterson@cern.ch',localAttempt=False)
    if not res['OK']:
      self.log.error(res)
    else:
      self.log.info('Mail summary sent to production manager')
    return S_OK()

  #############################################################################
  def updateRequestStatus(self,reqID,status):
    """ This method updates the request status.
    """
    self.updatedRequests.append(reqID)
    #return S_OK()
    reqClient = RPCClient('ProductionManagement/ProductionRequest',timeout=120)
    result = reqClient.updateProductionRequest(long(reqID),{'RequestStatus':'Done'})
    if not result['OK']:
      self.log.error(result)

    return result

  #############################################################################
  def setProdParams(self,prodID):
    """ This method checks if production parameters are present and sets them
        if missing.
    """
    #return S_OK()
    prodAPI = Production()
    if not prodAPI.getParameters(prodID)['OK']:
      self.log.info('===>Adding parameters for production %s' %(prodID))
      result = prodAPI._setProductionParameters(prodID)
      if not result['OK']:
        self.log.error('Could not add production parameters for %s with message:\n%s' %(prodID,result['Message']))
        return result

    return S_OK()

  #############################################################################
  def cleanActiveJobsUpdateStatus(self,prodID,origStatus,status):
    """ This method checks if a production having enough BK events has any Waiting
        or Running jobs in the WMS and removes them to ensure the output data sample
        is static.  Then the production status is updated.
    """
    #return self.updateProductionStatus(prodID,origStatus,status)
    dProd = DiracProduction()
    dirac = Dirac()
    running = dProd.selectProductionJobs(int(prodID),Status='Running')
    if running['OK']:
      self.log.info('Killing %s running jobs for production %s' %(len(running['Value']),prodID))
      result = dirac.kill(running['Value'])
      if not result['OK']:
        self.log.error(result)
    waiting = dProd.selectProductionJobs(int(prodID),Status='Waiting')
    if waiting['OK']:
      self.log.info('Deleting %s waiting jobs for production %s' %(len(waiting['Value']),prodID))
      result = dProd.deleteProdJobs(waiting['Value'])
      if not result['OK']:
        self.log.error(result)

    return self.updateProductionStatus(prodID,origStatus,status)

  #############################################################################
  def updateProductionStatus(self,prodID,origStatus,status):
    """ This method updates the production status and logs the changes for each
        iteration of the agent.  Most importantly this method only allows status
        transitions based on what the original status should be.
    """
    productionClient = ProductionClient()
    dProd = DiracProduction()
    result = dProd.getProduction(long(prodID))
    if not result['OK']:
      self.log.error('Could not update production status for %s to %s:\n%s' %(prodID,status,result))
      return result

    currentStatus = result['Value']['Status']
    if currentStatus.lower() == origStatus.lower():
      #self.updatedProductions[prodID]={'to':status,'from':origStatus}
      #return S_OK()
      self.log.verbose('Changing status for prod %s from %s to %s' %(prodID,currentStatus,origStatus))
      res = productionClient.setProductionStatus(prodID,status)
      if not res['OK']:
        self.log.error("Failed to update status of production %s from %s to %s" % (prodID,origStatus,status))
      else:
        self.updatedProductions[prodID]={'to':status,'from':origStatus}
    elif currentStatus.lower()==status.lower():
      pass
    else:
      self.log.verbose('Production %s not updated to %s as it is currently in status %s' %(prodID,status,currentStatus))

    return S_OK()
