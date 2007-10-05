# $Id: ProductionRepositoryHandler.py,v 1.9 2007/10/05 14:39:35 gkuznets Exp $
"""
ProductionRepositoryHandler is the implementation of the ProductionRepository service
    in the DISET framework

    The following methods are available in the Service interface

    publishWorkflow()
    getWorkflow()

"""
__RCSID__ = "$Revision: 1.9 $"

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

  types_publishWorkflow = [ StringType ]
  def export_publishWorkflow( self, wf_body ):
    """ Publish new workflow in the repositiry taking WFtype from the workflow itself
    """
    errKey = "Publishing workflow failed:"
    wf_type = "Unknown"
    sDN = self.transport.peerCredentials['DN']
    try:
      wf = fromXMLString(wf_body)
      wf_type = wf.getType()
      result = productionRepositoryDB.publishWorkflow(wf_type, wf_body, sDN, update=True)
      if not result['OK']:
        errExpl = " type=%s because %s" % (wf_type, result['Message'])
        gLogger.error(errKey, errExpl)
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
    return S_OK(result['Value'])

  types_getListWorkflows = [ ]
  def export_getListWorkflows(self):
    result = productionRepositoryDB.getListWorkflows()
    if not result['OK']:
      error = 'Failed to read List of Workflows from the repository'
      gLogger.error(error)
      return S_ERROR(error)
    gLogger.info('List of Workflows requested from the Production Repository')
    print "----KGG ", result['Value']
    #return S_OK()
    return S_OK(result['Value'])

  types_getWorkflow = [ StringType ]
  def export_getWorkflowInfo( self, wf_name ):
    result = productionRepositoryDB.getWorkflowInfo(wf_name)
    if not result['OK']:
        error = 'Failed to read Workflow Info with the name %s from the repository' % wf_name
        gLogger.error(error)
        return S_ERROR(error)
    gLogger.info('Workflow Info %s sucessfully read from the Production Repository' % wf_name)
    return S_OK(result['Value'])

  types_updateWorkflow = [ StringType ]
  def export_updateWorkflow( self, wf_body ):
    result = productionRepositoryDB.publishWorkflow(wf_body, self.sDN, True)
    if not result['OK']:
        return result
    gLogger.info('Workflow %s of type %s updates in the Production Repository by the ' % (wf_name, wf_type, self.sDN) )
    return S_OK()

  types_getWorkflow = [ ]
