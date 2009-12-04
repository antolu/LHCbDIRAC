""" Client class to access the production manager service """
# $Id$
__RCSID__ = "$Revision: 1.6 $"

from DIRAC                                                            import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient                                       import RPCClient
from LHCbDIRAC.TransformationSystem.Client.TransformationDBClient     import TransformationDBClient  

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
    TransformationDBClient.__init__()
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
  
  def createProduction(self,xmlString,fileMask='',groupSize=1,bkQuery={},plugin='',productionGroup='',productionType='',derivedProd='',maxJobs=0,rpc=False,url='',timeout=120):
    """ Create a production, based on the supplied parameters.
        Any input data can be specified by either fileMast or bkQuery. If both are specified then the BKQuery takes precedence.
        Usage: createProduction <xmlString> <filemask> <groupsize> <bkquery> <plugin> <prodGroup> <prodType> <maxJobs>
    """
    #TODO: Check where agentType comes from
    wf = fromXMLString(xmlString)
    transName = wf.getName()
    description = wf.getDescrShort()
    longDescription = wf.getDescription()
    return self.addTransformation(transName,description,longDescription,productionType,plugin,agentType,fileMask,
                                  transformationGroup = productionGroup,
                                  groupSize           = groupSize,
                                  inheritedFrom       = derivedProd,
                                  body                = xmlString, 
                                  maxJobs             = maxJobs,
                                  eventsPerJob        = 0,
                                  addFiles            = True,
                                  bkQuery             = bkQuery)

  #############################################################################
  #TODO: Update where used.
  #############################################################################
  
  def setProductionStatus(self,prodID,status,rpc=False,url='',timeout=120):
    return self.setTransformationStatus(prodID, status,rpc=rpc,url=url,timeout=timeout)

  def selectWMSJobs(self,prodID,statusList=[],newer=0,rpc=False,url='',timeout=120):
    return self.selectWMSTasks(prodID,statusList,newer,rpc=rpc,url=url,timeout=timeout)

  def getJobWMSStats(self,prodID,rpc=False,url='',timeout=120):
    return self.getTransformationTaskStats(prodID,rpc=rpc,url=url,timeout=timeout)

  def setJobStatusAndWmsID(self,prodID,jobNumber,status,wmsID,rpc=False,url='',timeout=120):
    return self.setTaskStatusAndWmsID(prodID,jobNumber,status,wmsID,rpc=rpc,url=url,timeout=timeout)