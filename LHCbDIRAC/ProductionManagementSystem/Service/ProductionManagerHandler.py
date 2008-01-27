# $Id: ProductionManagerHandler.py,v 1.1 2008/01/27 16:45:40 gkuznets Exp $
"""
ProductionManagerHandler is the implementation of the Production service

    The following methods are available in the Service interface
"""
__RCSID__ = "$Revision: 1.1 $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ProductionManagementSystem.DB.ProductionDB import ProductionDB
from DIRAC.Core.Transformation.TransformationHandler import *
from DIRAC.Core.Workflow.WorkflowReader import *
from DIRAC.Interfaces.API.Dirac import Dirac # job submission

# This is a global instance of the ProductionDB class
productionDB = False

def initializeProductionManagerHandler( serviceInfo ):
  global productionDB
  productionDB = ProductionDB()
  global wms
  wms = Dirac()
  return S_OK()

################ WORKFLOW SECTION ####################################

class ProductionManagerHandler( TransformationDBHandler ):

  types_publishWorkflow = [ StringType, BooleanType ]
  def export_publishWorkflow( self, wf_body, update=False):
    """ Publish new workflow in the repositiry taking WFname from the workflow itself
    """
    errKey = "Publishing workflow failed:"
    wf_name = "Unknown"
    wf_parent = "Unknown"
    wf_description = "empty description"
    authorDN = self.transport.peerCredentials['DN']
    #authorName = self.transport.peerCredentials['user']
    authorGroup = self.transport.peerCredentials['group']
    try:
      wf = fromXMLString(wf_body)
      wf_name = wf.getName()
      wf_parent = wf.getType()
      wf_description = wf.getDescrShort()
      wf_descr_long = wf.getDescription()
      result = productionDB.publishWorkflow(wf_name, wf_parent, wf_description, wf_descr_long, wf_body, authorDN, authorGroup, update)
      if not result['OK']:
        errExpl = " name=%s because %s" % (wf_name, result['Message'])
        gLogger.error(errKey, errExpl)
      else:
        if update:
          gLogger.verbose('Workflow %s is updated in the Production Repository by the %s'%(wf_name, authorDN) )
        else:
          gLogger.verbose('Workflow %s is added to the Production Repository by the %s'%(wf_name, authorDN) )
      return result

    except Exception,x:
      errExpl = " name=%s because %s" % (wf_name, str(x))
      gLogger.exception(errKey, errExpl)
      return S_ERROR(errKey + str(x))


  types_getWorkflow = [ StringType ]
  def export_getWorkflow( self, wf_name ):
    result = productionDB.getWorkflow(wf_name)
    if not result['OK']:
        error = 'Failed to read Workflow with the name %s from the repository' % wf_name
        gLogger.error(error)
        return S_ERROR(error)
    gLogger.info('Workflow %s sucessfully read from the Production Repository' % wf_name)
    return result

  types_getWorkflowFullDescription = [ StringType ]
  def export_getWorkflowFullDescription( self, wf_name ):
    result = productionDB.getWorkflow(wf_name)
    if not result['OK']:
        error = 'Failed to read Workflow with the name %s from the repository' % wf_name
        gLogger.error(error)
        return S_ERROR(error)
    gLogger.info('Workflow %s sucessfully read from the Production Repository' % wf_name)
    wf = fromXMLString(result["Value"])
    return S_OK(wf.getDescription())

  types_deleteWorkflow = [ StringType ]
  def export_deleteWorkflow( self, wf_name ):
    result = productionDB.deleteWorkflow(wf_name)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_getListWorkflows = [ ]
  def export_getListWorkflows(self):
    result = productionDB.getListWorkflows()
    if not result['OK']:
      error = 'Failed to read List of Workflows from the repository'
      gLogger.error(error)
      return S_ERROR(error)
    gLogger.verbose('List of Workflows requested from the Production Repository')
    return result

  types_getWorkflow = [ StringType ]
  def export_getWorkflowInfo( self, wf_name ):
    result = productionDB.getWorkflowInfo(wf_name)
    if not result['OK']:
        error = 'Failed to read Workflow Info with the name %s from the repository' % wf_name
        gLogger.error(error)
        return S_ERROR(error)
    gLogger.info('Workflow Info %s sucessfully read from the Production Repository' % wf_name)
    return result

