# $Id: ProductionManagerHandler.py,v 1.12 2008/02/15 11:15:51 atsareg Exp $
"""
ProductionManagerHandler is the implementation of the Production service

    The following methods are available in the Service interface
"""
__RCSID__ = "$Revision: 1.12 $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ProductionManagementSystem.DB.ProductionDB import ProductionDB
from DIRAC.Core.Transformation.TransformationHandler import TransformationHandler
from DIRAC.Core.Workflow.Workflow import *
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

class ProductionManagerHandler( TransformationHandler ):

  def __init__(self,*args,**kargs):

    self.setDatabase(productionDB)
    TransformationHandler.__init__(self, *args,**kargs)


  types_publishWorkflow = [ StringType, BooleanType ]
  def export_publishWorkflow( self, body, update=False):
    """ Publish new workflow in the repositiry taking WFname from the workflow itself
    """
    errKey = "Publishing workflow failed:"
    name = "Unknown"
    parent = "Unknown"
    description = "empty description"
    descr_long = "empty long description"
    authorDN = self.transport.peerCredentials['DN']
    #authorName = self.transport.peerCredentials['user']
    authorGroup = self.transport.peerCredentials['group']
    try:
      wf = fromXMLString(wf_body)
      name = wf.getName()
      parent = wf.getType()
      description = wf.getDescrShort()
      descr_long = wf.getDescription()
      result = productionDB.publishWorkflow(name, parent, description, descr_long, body, authorDN, authorGroup, update)
      if not result['OK']:
        errExpl = " name=%s because %s" % (name, result['Message'])
        gLogger.error(errKey, errExpl)
      else:
        if update:
          gLogger.verbose('Workflow %s is updated in the Production Repository by the %s'%(name, authorDN) )
        else:
          gLogger.verbose('Workflow %s is added to the Production Repository by the %s'%(name, authorDN) )
      return result

    except Exception,x:
      errExpl = " name=%s because %s" % (name, str(x))
      gLogger.exception(errKey, errExpl)
      return S_ERROR(errKey + str(x))


  types_getWorkflow = [ StringType ]
  def export_getWorkflow( self, name ):
    result = productionDB.getWorkflow(name)
    if not result['OK']:
        error = 'Failed to read Workflow with the name %s from the repository' % name
        gLogger.error(error)
        return S_ERROR(error)
    gLogger.info('Workflow %s sucessfully read from the Production Repository' % name)
    return result

  types_getWorkflowFullDescription = [ StringType ]
  def export_getWorkflowFullDescription( self, name ):
    result = productionDB.getWorkflow(name)
    if not result['OK']:
        error = 'Failed to read Workflow with the name %s from the repository' % name
        gLogger.error(error)
        return S_ERROR(error)
    gLogger.info('Workflow %s sucessfully read from the Production Repository' % name)
    wf = fromXMLString(result["Value"])
    return S_OK(wf.getDescription())

  types_deleteWorkflow = [ StringType ]
  def export_deleteWorkflow( self, name ):
    result = productionDB.deleteWorkflow(name)
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
  def export_getWorkflowInfo( self, name ):
    result = productionDB.getWorkflowInfo(name)
    if not result['OK']:
        error = 'Failed to read Workflow Info with the name %s from the repository' % name
        gLogger.error(error)
        return S_ERROR(error)
    gLogger.info('Workflow Info %s sucessfully read from the Production Repository' % name)
    return result

################ TRANSFORMATION SECTION ####################################

  types_publishProduction = [ StringType, StringType, IntType, BooleanType ]
  def export_publishProduction( self, body, filemask='', groupsize=0, update=False):
    """ Publish new transformation in the ProductionDB
    """
    errKey = "Publishing Production failed: "
    authorDN = self.transport.peerCredentials['DN']
    #authorName = self.transport.peerCredentials['user']
    authorGroup = self.transport.peerCredentials['group']

    wf = fromXMLString(body)
    name = wf.getName()
    parent = wf.getType()
    description = wf.getDescrShort()
    long_description = wf.getDescription()

    try:
      if update:
        result = productionDB.updateProduction(name, parent, description, long_description, body, filemask, groupsize, authorDN, authorGroup)
      else:
        result = productionDB.addProduction(name, parent, description, long_description, body, filemask, groupsize, authorDN, authorGroup)
      if not result['OK']:
        errExpl = " name=%s because %s" % (name, result['Message'])
        gLogger.error(errKey, errExpl)
      else:
        if update:
          gLogger.verbose('Transformation %s is updated in the ProductionDB by the %s'%(name, authorDN) )
        else:
          gLogger.verbose('Transformation %s is added to the ProductionDB by the %s'%(name, authorDN) )
      return result

    except Exception,x:
      errExpl = " name=%s because %s" % (name, str(x))
      gLogger.exception(errKey, errExpl)
      return S_ERROR(errKey + str(x))

  types_deleteProduction = [ StringType ]
  def export_deleteProduction( self, name ):
    result = productionDB.deleteProduction(name)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_deleteProductionByID = [ LongType ]
  def export_deleteProductionByID( self, id ):
    result = productionDB.deleteProduction(id)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  # Obsoleted to keep temporarily
  types_getListProductions = [ ]
  def export_getListProductions(self):
    gLogger.verbose('Getting list of Productions')
    result = productionDB.getProductionList()
    if not result['OK']:
      error = 'Can not get list of Transformations because %s' % result['Message']
      gLogger.error( error )
      return S_ERROR( error )
    return result

  types_getProductionList = [ ]
  def export_getProductionList(self):
    gLogger.verbose('Getting list of Productions')
    result = productionDB.getProductionList()
    if not result['OK']:
      error = 'Can not get list of Transformations because %s' % result['Message']
      gLogger.error( error )
      return S_ERROR( error )
    return result

  types_getAllProductions = [ ]
  def export_getAllProductions(self):
    gLogger.verbose('Getting list of Productions')
    result = productionDB.getAllProductions()
    if not result['OK']:
      error = 'Can not get list of Transformations because %s' % result['Message']
      gLogger.error( error )
      return S_ERROR( error )
    return result

  types_addProductionJob = [ LongType,  StringType, StringType]
  def export_addProductionJob( self, productionID, inputVector, se ):
    result = productionDB.addProductionJob(productionID, inputVector, se)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_getProductionBodyByID = [ LongType ]
  def export_getProductionBodyByID( self, id ):
    result = productionDB.getProductionBodyByID(id)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_getProductionBody = [ StringType ]
  def export_getProductionBody( self, id ):
    result = productionDB.getProductionBodyByID(id)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_setProductionStatusByID = [ LongType, StringType ]
  def export_setProductionStatusByID( self, id, status ):
    result = productionDB.setTransformationStatus(id, status)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_setProductionStatus = [ StringType, StringType ]
  def export_setProductionStatus( self, id, status ):
    result = productionDB.setTransformationStatus(id, status)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_setTransformationMaskID = [ LongType, StringType ]
  def export_setTransformationMaskID( self, id, status ):
    result = productionDB.setTransformationMask(id, status)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_setTransformationMask = [ StringType, StringType ]
  def export_setTransformationMask( self, id, status ):
    result = productionDB.setTransformationMask(id, status)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_getInputData2 = [ LongType, StringType ]
  def export_getInputData2( self, id, status ):

    print "Production handler 1:",id, status

    result = productionDB.getInputData(id, status)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_updateTransformation = [ LongType]
  def export_updateTransformation( self, id ):
    result = productionDB.updateTransformation(id)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_setFileStatusForTransformation = [ LongType, StringType, ListType ]
  def export_setFileStatusForTransformation( self, id,status,lfns ):
    result = productionDB.setFileStatusForTransformation(id,status,lfns)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_setFileSEForTransformation = [ LongType, StringType, ListType ]
  def export_setFileSEForTransformation( self, id,se,lfns ):
    result = productionDB.setFileSEForTransformation(id,se,lfns)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result
    
  types_setFileTargetSEForTransformation = [ LongType, StringType, ListType ]
  def export_setFileTargetSEForTransformation( self, id,se,lfns ):
    result = productionDB.setFileTargetSEForTransformation(id,se,lfns)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result  

  types_setFileJobID = [ LongType, StringType, ListType ]
  def export_setFileJobID( self, id,se,lfns ):
    result = productionDB.setFileJobID(id,se,lfns)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_getJobsToSubmit = [ LongType, IntType ]
  def export_getJobsToSubmit(self,production,numJobs):
    """ Get information necessary for submission for a given number of jobs 
        for a given production
    """
    
    # Get the production body first
    result = productionDB.getProductionBodyByID(int(production))
    if not result['OK']:
      return S_ERROR('Failed to get production body for production '+str(production))
      
    body = result['Value']  
    result = productionDB.selectJobs(production,['Created'],numJobs)
    if not result['OK']:
      return S_ERROR('Failed to get jobs for production '+str(production))
    
    jobDict = result['Value']
    
    resultDict = {}
    resultDict['Body'] = body
    resultDict['JobDictionary'] = jobDict
    
    return S_OK(resultDict) 
