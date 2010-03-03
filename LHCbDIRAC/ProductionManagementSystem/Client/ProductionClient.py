""" Client class to access the production manager service """
# $Id$
__RCSID__ = "$Revision: 1.6 $"

from DIRAC                                                                    import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Workflow.Workflow                                             import fromXMLString
from DIRAC.Core.DISET.RPCClient                                               import RPCClient
from LHCbDIRAC.ProductionManagementSystem.Client.TransformationDBClient       import TransformationDBClient  
import os

class ProductionClient(TransformationDBClient):

  """ Exposes the functionality available in the LHCbDIRAC/ProductionManagerHandler

      This inherits the DIRAC base Client for direct execution of server functionality.
      The following methods are available (although not visible here).

      Workflows (table) manipulation
      
          publishWorkflow(body,update=False)
          getWorkflow(name)
          getWorkflowFullDescription(name)
          deleteWorkflow(name)
          getListWorkflows()
          getWorkflowInfo(name)
          
  """
  def __init__(self):
    TransformationDBClient.__init__(self,'ProductionClient')
    self.setServer('ProductionManagement/ProductionManager')

  #############################################################################
  def getParameters(self,prodID,pname='',rpc=False,url='',timeout=120):
    res = self.getTransformation(int(prodID),extraParams=True)
    if not res['OK']: 
      return res
    paramDict = res['Value']
    if not pname:
      return S_OK(paramDict)
    if paramDict.has_key(pname):
      return S_OK(paramDict[pname])
    else:
      return S_ERROR('Parameter %s not found for production' % pname)

  def createProduction(self,workflow,fileMask='',groupSize=1,bkQuery={},plugin='',productionGroup='',productionType='',derivedProd='',maxJobs=0,agentType='Manual',rpc=False,url='',timeout=120):
    """ Create a production, based on the supplied parameters.
        Any input data can be specified by either fileMast or bkQuery. If both are specified then the BKQuery takes precedence.
        Usage: createProduction <workflow or file> <filemask> <groupsize> <bkquery> <plugin> <prodGroup> <prodType> <maxJobs>
    """
    if os.path.exists(workflow):
      fopen = open(workflow,'r')
      workflow = fopen.read()
      fopen.close()
    wf = fromXMLString(workflow)
    transName = wf.getName()
    description = wf.getDescrShort()
    longDescription = wf.getDescription()
    return self.addTransformation(transName,description,longDescription,productionType,plugin,agentType,fileMask,
                                  transformationGroup = productionGroup,
                                  groupSize           = groupSize,
                                  inheritedFrom       = derivedProd,
                                  body                = workflow, 
                                  maxTasks            = maxJobs,
                                  eventsPerJob        = 0,
                                  addFiles            = True,
                                  bkQuery             = bkQuery)

  #############################################################################
  #TODO: Update where used.
  #############################################################################
  
  def setProductionStatus(self,prodID,status,rpc=False,url='',timeout=120):
    return self.setTransformationParameter(prodID,'Status',status,rpc=rpc,url=url,timeout=timeout)

  def getJobWMSStats(self,prodID,rpc=False,url='',timeout=120):
    return self.getTransformationTaskStats(prodID,rpc=rpc,url=url,timeout=timeout)

  def setJobStatusAndWmsID(self,prodID,jobNumber,status,wmsID,rpc=False,url='',timeout=120):
    return self.setTaskStatusAndWmsID(prodID,jobNumber,status,wmsID,rpc=rpc,url=url,timeout=timeout)
