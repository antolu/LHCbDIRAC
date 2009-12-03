# $Id$
"""
ProductionManagerHandler is the implementation of the Production service

    The following methods are available in the Service interface
"""
__RCSID__ = "$Revision: 1.56 $"

from DIRAC                                                          import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler                                import RequestHandler
from LHCbDIRAC.ProductionManagementSystem.DB.ProductionDB           import ProductionDB
from LHCbDIRAC.TransformationSystem.Service.TransformationHandler   import TransformationHander
from DIRAC.Core.Workflow.Workflow                                   import *
from types import *

# This is a global instance of the ProductionDB class
productionDB = False

def initializeProductionManagerHandler( serviceInfo ):
  global productionDB
  productionDB = ProductionDB()
  return S_OK()

class ProductionManagerHandler( TransformationHandler ):

  def __init__(self,*args,**kargs):

    self.setDatabase(productionDB)
    TransformationHandler.__init__(self, *args,**kargs)

  types_publishWorkflow = [ StringType ]
  def export_publishWorkflow( self, body, update=False):
    """ Publish new workflow in the repository taking WFname from the workflow itself
    """
    errKey = "Publishing workflow failed:"
    name = "Unknown"
    parent = "Unknown"
    description = "empty description"
    descr_long = "empty long description"
    authorDN = self._clientTransport.peerCredentials['DN']
    authorGroup = self._clientTransport.peerCredentials['group']
    try:
      wf = fromXMLString(body)
      name = wf.getName()
      parent = wf.getType()
      description = wf.getDescrShort()
      descr_long = wf.getDescription()
      result = self.database.publishWorkflow(name, parent, description, descr_long, body, authorDN, authorGroup, update)
      if not result['OK']:
        errExpl = " name=%s because %s" % (name, result['Message'])
        gLogger.error(errKey, errExpl)
      else:
        if update:
          gLogger.verbose('Workflow %s is modified in the Production Repository by the %s'%(name, authorDN) )
        else:
          gLogger.verbose('Workflow %s is added to the Production Repository by the %s'%(name, authorDN) )
      return result

    except Exception,x:
      errExpl = " name=%s because %s" % (name, str(x))
      gLogger.exception(errKey, errExpl)
      return S_ERROR(errKey + str(x))

  types_getWorkflow = [ StringType ]
  def export_getWorkflow(self, name):
    res = self.database.getWorkflow(name)
    return self.__parseRes(res)

  types_getWorkflowFullDescription = [ StringType ]
  def export_getWorkflowFullDescription(self,name):
    res = self.database.getWorkflow(name)
    if not res['OK']:
      return res
    wf = fromXMLString(result['Value'])
    return S_OK(wf.getDescription())

  types_deleteWorkflow = [ StringType ]
  def export_deleteWorkflow( self, name ):
    res = self.database.deleteWorkflow(name)
    return self.__parseRes(res)

  types_getListWorkflows = [ ]
  def export_getListWorkflows(self):
    res = self.database.getListWorkflows()
    return self.__parseRes(res)

  types_getWorkflowInfo = [ StringType ]
  def export_getWorkflowInfo( self, name ):
    res = self.database.getWorkflowInfo(name)
    return self.__parseRes(res)

  types_publishProduction = [StringTypes, StringType, StringType, StringType]
  def export_publishProduction(self, body, type, plugin, agentType, fileMask, 
                               transformationGroup  = 'General',
                               groupSize            = 1,
                               inheritedFrom        = 0,
                               maxJobs              = 0,
                               eventsPerJob         = 0,
                               addFiles             = True):
    """ Publish new transformation in the ProductionDB """
    authorDN = self._clientTransport.peerCredentials['DN']
    authorGroup = self._clientTransport.peerCredentials['group']
    wf = fromXMLString(body)
    name = wf.getName()
    description = wf.getDescrShort()
    longDescription = wf.getDescription()
    res = self.database.addTransformation(name, description, longDescription,authorDN, authorGroup, type, plugin,agentType,fileMask,
                        transformationGroup =transformationGroup,
                        groupSize           = groupSize,
                        inheritedFrom       = inheritedFrom,
                        body                = body, 
                        maxJobs             = maxJobs,
                        eventsPerJob        = eventsPerJob,
                        addFiles            = addFiles)
    if res['OK']:
      gLogger.info("Published production %d" % res['Value'])  
    return self.__parseRes(res)

  #TODO: Remove
  types_getAllProductions = []
  def export_getAllProductions(self):
    res = self.database.getTransformations()
    return self.__parseRes(res)
  
  types_getProductionBody = [list(StringTypes)+[IntType,LongType]]
  def export_getProductionBody(self, transName):
    res = self.getTransformationParameters(transName,['Body'])
    return self.__parseRes(res)
