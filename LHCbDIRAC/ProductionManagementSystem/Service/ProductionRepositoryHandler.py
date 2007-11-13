# $Id: ProductionRepositoryHandler.py,v 1.11 2007/11/13 20:35:05 gkuznets Exp $
"""
ProductionRepositoryHandler is the implementation of the ProductionRepository service
    in the DISET framework

    The following methods are available in the Service interface

    publishWorkflow()
    getWorkflow()

"""
__RCSID__ = "$Revision: 1.11 $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ProductionManagementSystem.DB.ProductionRepositoryDB import ProductionRepositoryDB
from DIRAC.Core.Workflow.WorkflowReader import *

# This is a global instance of the ProductionRepositoryDB class
productionRepositoryDB = False

def initializeProductionRepositoryHandler( serviceInfo ):
  global productionRepositoryDB
  productionRepositoryDB = ProductionRepositoryDB()
  return S_OK()

class ProductionRepositoryHandler( RequestHandler ):

  types_publishWorkflow = [ StringType, BooleanType ]
  def export_publishWorkflow( self, wf_body, update=False):
    """ Publish new workflow in the repositiry taking WFtype from the workflow itself
    """
    errKey = "Publishing workflow failed:"
    wf_type = "Unknown"
    sDN = self.transport.peerCredentials['DN']
    try:
      wf = fromXMLString(wf_body)
      wf_type = wf.getType()
      result = productionRepositoryDB.publishWorkflow(wf_type, wf_body, sDN, update)
      if not result['OK']:
        errExpl = " type=%s because %s" % (wf_type, result['Message'])
        gLogger.error(errKey, errExpl)
      else:
        if update:
          gLogger.verbose('Workflow %s is updated in the Production Repository by the %s'%(wf_type, sDN) )
        else:
          gLogger.verbose('Workflow %s is added to the Production Repository by the %s'%(wf_type, sDN) )
      return result

    except Exception,x:
      errExpl = " type=%s because %s" % (wf_type, str(x))
      gLogger.exception(errKey, errExpl)
      return S_ERROR(errKey + str(x))


  types_getWorkflow = [ StringType ]
  def export_getWorkflow( self, wf_name ):
    result = productionRepositoryDB.getWorkflow(wf_name)
    if not result['OK']:
        error = 'Failed to read Workflow with the name %s from the repository' % wf_name
        gLogger.error(error)
        return S_ERROR(error)
    gLogger.info('Workflow %s sucessfully read from the Production Repository' % wf_name)
    return result

  types_deleteWorkflow = [ StringType ]
  def export_deleteWorkflow( self, wf_name ):
    result = productionRepositoryDB.deleteWorkflow(wf_name)
    return result

  types_getListWorkflows = [ ]
  def export_getListWorkflows(self):
    result = productionRepositoryDB.getListWorkflows()
    if not result['OK']:
      error = 'Failed to read List of Workflows from the repository'
      gLogger.error(error)
      return S_ERROR(error)
    gLogger.info('List of Workflows requested from the Production Repository')
    return result

  types_getWorkflow = [ StringType ]
  def export_getWorkflowInfo( self, wf_name ):
    result = productionRepositoryDB.getWorkflowInfo(wf_name)
    if not result['OK']:
        error = 'Failed to read Workflow Info with the name %s from the repository' % wf_name
        gLogger.error(error)
        return S_ERROR(error)
    gLogger.info('Workflow Info %s sucessfully read from the Production Repository' % wf_name)
    return result

  types_submitProduction = [ StringType, BooleanType ]
  def export_submitProduction( self, wf_body, update=False):
    """ Create new Production in the ProductionRepositoryDB taking Name and Parent from the workflow itself
    """
    errKey = "Publishing Production failed:"
    wf_name = "Unknown"
    wf_parrent = ""
    wf_comment = ""
    sDN = self.transport.peerCredentials['DN']
    try:
      wf = fromXMLString(wf_body)
      wf_name = wf.getName()
      wf_parent = wf.getType()
      wf_comment = wf.getDescrShort()
      result = productionRepositoryDB.publishProduction(wf_name, wf_parent, wf_comment, wf_body, sDN, update)
      if not result['OK']:
        errExpl = " type=%s because %s" % (wf_name, result['Message'])
        gLogger.error(errKey, errExpl)
      else:
        if update:
          gLogger.verbose('Production %s is updated in the Production Repository by the %s'%(wf_name, sDN) )
        else:
          gLogger.verbose('Production %s is added to the Production Repository by the %s'%(wf_name, sDN) )
      return result

    except Exception,x:
      errExpl = " type=%s because %s" % (wf_name, str(x))
      gLogger.exception(errKey, errExpl)
      return S_ERROR(errKey + str(x))

  types_getListProductions = [ ]
  def export_getListProductions(self):
    result = productionRepositoryDB.getListProductions()
    if not result['OK']:
      error = 'Failed to read List of Productions from the repository'
      gLogger.error(error)
      return S_ERROR(error)
    gLogger.info('List of Productions requested from the Production Repository')
    #return S_OK()
    return result
