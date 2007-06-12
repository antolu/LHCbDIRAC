# $Id: ProductionRepositoryHandler.py,v 1.7 2007/06/12 13:30:00 gkuznets Exp $
"""
ProductionRepositoryHandler is the implementation of the ProductionRepository service
    in the DISET framework

    The following methods are available in the Service interface

    publishWF()
    getWorkflow()

"""
__RCSID__ = "$Revision: 1.7 $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ProductionManagementSystem.DB.ProductionRepositoryDB import ProductionRepositoryDB

# This is a global instance of the ProductionRepositoryDB class
productionRepositoryDB = False

def initializeProductionRepositoryHandler( serviceInfo ):
  global productionRepositoryDB
  productionRepositoryDB = ProductionRepositoryDB()
  return S_OK()

class ProductionRepositoryHandler( RequestHandler ):

  types_publishWorkflow = [ StringType ]
  def export_publishWorkflow( self, wf_body ):
    result = productionRepositoryDB.publishWorkflow(wf_body, self.transport.peerCredentials['DN'])
    if not result['OK']:
        return result
    gLogger.info('Workflow %s of type %s added to the Production Repository by the '%(wf_name, wf_type, self.sDN) )
    return S_OK()

  types_getWorkflow = [ StringType ]
  def export_getWorkflow( self, wf_name ):
    result = productionRepositoryDB.getWorkflow(wf_name)
    if not result['OK']:
        error = 'Failed to read Workflow with the name %s from the repository' % wf_name
        gLogger.error(error)
        return S_ERROR(error)
    gLogger.info('Workflow %s sucessfully read from the Production Repository' % wf_name)
    return S_OK(result['Value'])

  types_getWorkflowsList = [ ]
  def export_getWorkflowsList(self):
    result = productionRepositoryDB.getWorkflowsList()
    if not result['OK']:
      error = 'Failed to read List of Workflows from the repository'
      gLogger.error(error)
      return S_ERROR(error)
    gLogger.info('List of Workflows requested from the Production Repository')
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
