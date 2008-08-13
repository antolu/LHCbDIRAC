# $Id: ProductionManagerHandler.py,v 1.39 2008/08/13 13:57:34 atsareg Exp $
"""
ProductionManagerHandler is the implementation of the Production service

    The following methods are available in the Service interface
"""
__RCSID__ = "$Revision: 1.39 $"

from types import *
import threading
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.ProductionManagementSystem.DB.ProductionDB import ProductionDB
from DIRAC.Core.Transformation.TransformationHandler import TransformationHandler
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.DISET.RPCClient import RPCClient

# This is a global instance of the ProductionDB class
productionDB = False

def initializeProductionManagerHandler( serviceInfo ):
  global productionDB
  productionDB = ProductionDB()
  return S_OK()

################ WORKFLOW SECTION ####################################

class ProductionManagerHandler( TransformationHandler ):

  def __init__(self,*args,**kargs):

    self.setDatabase(productionDB)
    TransformationHandler.__init__(self, *args,**kargs)
    self.lock = threading.Lock()


  types_publishWorkflow = [ StringType ]
  def export_publishWorkflow( self, body, update=False):
    """ Publish new workflow in the repositiry taking WFname from the workflow itself
    """
    errKey = "Publishing workflow failed:"
    name = "Unknown"
    parent = "Unknown"
    description = "empty description"
    descr_long = "empty long description"
    authorDN = self._clientTransport.peerCredentials['DN']
    #authorName = self._clientTransport.peerCredentials['user']
    authorGroup = self._clientTransport.peerCredentials['group']
    try:
      wf = fromXMLString(body)
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
          gLogger.verbose('Workflow %s is modified in the Production Repository by the %s'%(name, authorDN) )
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

  types_getWorkflowInfo = [ StringType ]
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
    authorDN = self._clientTransport.peerCredentials['DN']
    #authorName = self._clientTransport.peerCredentials['user']
    authorGroup = self._clientTransport.peerCredentials['group']

    wf = fromXMLString(body)
    name = wf.getName()
    parent = wf.getType()
    description = wf.getDescrShort()
    long_description = wf.getDescription()

    try:
      result = productionDB.addProduction(name, parent, description, long_description, body, filemask, groupsize, authorDN, authorGroup, update)
      if not result['OK']:
        errExpl = " name=%s because %s" % (name, result['Message'])
        gLogger.error(errKey, errExpl)
      else:
        if update:
          gLogger.verbose('Transformation %s is modified in the ProductionDB by the %s'%(name, authorDN) )
          message = "Transformation %s updated" % name
          resultlog = productionDB.updateTransformationLogging(long(result['Value']),message,authorDN)
        else:
          gLogger.verbose('Transformation %s is added to the ProductionDB by the %s'%(name, authorDN) )
          message = "Transformation %s published" % name
          resultlog = productionDB.updateTransformationLogging(long(result['Value']),message,authorDN)
      return result

    except Exception,x:
      errExpl = " name=%s because %s" % (name, str(x))
      gLogger.exception(errKey, errExpl)
      return S_ERROR(errKey + str(x))

  types_publishDerivedProduction = [ [LongType, IntType, StringType], StringType, StringType, IntType, BooleanType ]
  def export_publishDerivedProduction( self, originaProdIDOrName, body, filemask='', groupsize=0, update=False):
    """ Publish new transformation in the ProductionDB
    """
    originalTransID = productionDB.getTransformationID(originaProdIDOrName)
    if originalTransID == 0:
      return S_ERROR("No Production with the name '%s' in the TransformationDB" % originaProdIDOrName)

    errKey = "Publishing Production failed: "
    authorDN = self._clientTransport.peerCredentials['DN']
    #authorName = self._clientTransport.peerCredentials['user']
    authorGroup = self._clientTransport.peerCredentials['group']

    wf = fromXMLString(body)
    name = wf.getName()
    parent = wf.getType()
    description = wf.getDescrShort()
    long_description = wf.getDescription()

    try:
      result = productionDB.addDerivedProduction(name, parent, description, long_description, body, filemask, groupsize, authorDN, authorGroup, originalTransID)
      if not result['OK']:
        errExpl = " name=%s because %s" % (name, result['Message'])
        gLogger.error(errKey, errExpl)
      else:
        gLogger.verbose('Transformation %s is added to the ProductionDB by the %s as derived from %s'%(name, authorDN, originalTransID) )
        message = "Transformation %s published as derived from %s" % (name, originalTransID)
        resultlog = productionDB.updateTransformationLogging(long(result['Value']),message,authorDN)
      return result

    except Exception,x:
      errExpl = " name=%s because %s" % (name, str(x))
      gLogger.exception(errKey, errExpl)
      return S_ERROR(errKey + str(x))

  types_deleteProduction = [ [LongType, IntType, StringType] ]
  def export_deleteProduction( self, name ):
    transID = productionDB.getTransformationID(name)
    result = productionDB.deleteProduction(name)
    if not result['OK']:
      gLogger.error(result['Message'])
    else:
      authorDN = self._clientTransport.peerCredentials['DN']
      message = "Transformation %s deleted" % transID
      resultlog = productionDB.updateTransformationLogging(transID,message,authorDN)
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

  types_addProductionJob = [ [LongType, IntType, StringType], StringType, StringType]
  def export_addProductionJob( self, productionID, inputVector='', se=''):
    result = productionDB.addProductionJob(productionID, inputVector, se)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_extendProduction = [ [LongType, IntType, StringType], [LongType, IntType]]
  def export_extendProduction( self, productionID, nJobs):
    result = productionDB.extendProduction(productionID, nJobs)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_getProductionInfo = [ [LongType, IntType, StringType]]
  def export_getProductionInfo( self, productionID):
    result = productionDB.getProductionInfo(productionID)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_getProductionBody = [ [LongType, IntType, StringType] ]
  def export_getProductionBody( self, id_ ):
    result = productionDB.getProductionBody(id_)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_setProductionBody = [ [LongType, IntType, StringType], StringType ]
  def export_setProductionBody( self, id_ , body):
    wf = fromXMLString(body)
    name = wf.getName()
    parent = wf.getType()
    description = wf.getDescrShort()
    long_description = wf.getDescription()
    result = productionDB.setProductionBody(id_, body, name, parent, description, long_description)
    if not result['OK']:
      gLogger.error(result['Message'])
      return result
    authorDN = self._clientTransport.peerCredentials['DN']
    message = "New Production Body uploaded"
    resultlog = productionDB.updateTransformationLogging(id_,message,authorDN)
    return result

  types_setFileStatusForTransformation = [ [IntType,LongType]+list(StringTypes), ListType ]
  def export_setFileStatusForTransformation( self, prod_id, statusList):
    """ Set file status in the context of a given production
    """
    for status,lfns in statusList:
      result = productionDB.setFileStatusForTransformation(prod_id,status,lfns)
      if not result['OK']:
        gLogger.error(result['Message'])
    return result

  types_setFileSEForTransformation = [ [IntType,LongType]+list(StringTypes), StringType, ListType ]
  def export_setFileSEForTransformation( self, id_,se,lfns ):
    result = productionDB.setFileSEForTransformation(id_,se,lfns)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_setFileTargetSEForTransformation = [ [IntType,LongType]+list(StringTypes), StringType, ListType ]
  def export_setFileTargetSEForTransformation( self, id_,se,lfns ):
    result = productionDB.setFileTargetSEForTransformation(id_,se,lfns)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

######################## Job section ############################

  types_setFileJobID = [ LongType, LongType, ListType ]
  def export_setFileJobID( self, id_, jobid,lfns ):
    result = productionDB.setFileJobID(id_, jobid,lfns)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_getJobsToSubmit = [ LongType, IntType, StringType ]
  def export_getJobsToSubmit(self,production,numJobs,site=''):
    """ Get information necessary for submission for a given number of jobs
        for a given production
    """
    # Get the production body first
    result = productionDB.getProduction(production)
    if not result['OK'] or not ['Value']:
      return S_ERROR('Failed to get production body for production %d with message %s'%(production,result["Message"]))

    resultDict = result['Value']
    status_corrected = resultDict['Status'].lower().capitalize() # capitalize status
    jobDict={}

    if status_corrected == 'Active' or status_corrected == 'Flush':

      self.lock.acquire()
      result = productionDB.selectJobs(production,['Created'],numJobs,site)
      if not result['OK']:
        self.lock.release()
        return S_ERROR('Failed to get jobs for production %d with message %s'%(production,result["Message"]))
      jobDict = result['Value']
      # lets change jobs statuses
      for jobid in jobDict:
        result = productionDB.setJobStatus(production, long(jobid), "Reserved")
        if not result['OK']:
          gLogger.error('Failed to change status of the job %s to Reserved or production %d with message %s, Removing bad job'%(jobid, production,result["Message"]))
          # we also have to remove job from the list
          del jobDict[jobid]

      self.lock.release()

    resultDict['JobDictionary'] = jobDict # adding additional element
    return S_OK(resultDict)

  types_getJobsWithStatus = [ LongType, StringType, IntType, StringType]
  def export_getJobsWithStatus(self, production, status, numJobs, site):
    """ Get jobs with specified status limited by given number
        for a given production
    """
    result = productionDB.selectJobs(production,[status],numJobs, site)
    if not result['OK']:
      return S_ERROR('Failed to get jobs with the status %s site=%s for production=%d '%(status, site, production))
    return result

  types_setJobStatus = [ LongType, LongType, StringType ]
  def export_setJobStatus(self, productionID, jobID, status):
    """ Set status for a given Job
    """
    result = productionDB.setJobStatus(productionID, jobID, status)
    if not result['OK']:
      error = 'Could not change job status=%s in TransformationID=%d JobID=%d because %s' % (status, productionID, jobID, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )
    return result

  types_setJobWmsID = [ LongType, LongType, StringType ]
  def export_setJobWmsID(self, productionID, jobID, jobWmsID):
    """ Set jobWmsID for a given Job
    """
    result = productionDB.setJobWmsID(productionID, jobID, jobWmsID)
    if not result['OK']:
      error = 'Couldt set JobWmsID=%s in TransformationID=%d JobID=%d because %s' % (jobWmsID, productionID, jobID, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )
    return result

  types_setJobStatusAndWmsID = [ LongType, LongType, StringType, StringType]
  def export_setJobStatusAndWmsID(self, productionID, jobID, status, jobWmsID):
    """ Set jobWmsID for a given Job
    """
    result = productionDB.setJobStatusAndWmsID(productionID, jobID, status, jobWmsID)
    if not result['OK']:
      error = 'Could set job status=%s and WmsID=%s in TransformationID=%d JobID=%d because %s' % (status, jobWmsID, productionID, jobID, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )

    # Send data logging records
    if status == "Submitted":
      result = productionDB.getJobInfo(productionID, jobID)
      if not result['OK']:
        gLogger.error('Could not get job info from Production DB in TransformationID=%d JobID=%d ' % (productionID,jobID))
      jobDict = result['Value']
      lfns = jobDict['InputVector']
      if lfns:
        lfns = lfns.split(',')
      else:
        lfns = []
      for lfn in lfns:
        lfn = lfn.replace('LFN:','')
        dataLog = RPCClient('DataManagement/DataLogging')
        result = dataLog.addFileRecord(lfn,'Job submitted', 'WMS JobID: %s' % jobWmsID, '','ProductionManager')
        if not result['OK']:
          gLogger.warn('Failed to send Jobsubmitted status for lfn: '+lfn)

    return result

  types_getJobStats = [ [LongType, IntType, StringType] ]
  def export_getJobStats(self, productionID):
    """ Returns number of jobs in each status for a given production
    """
    return productionDB.getJobStats(productionID)

  types_getJobWmsStats = [ [LongType, IntType, StringType] ]
  def export_getJobWmsStats(self, productionID):
    """ Returns number of jobs in each status for a given production
    """
    return productionDB.getJobWmsStats(productionID)

  types_getJobInfo = [ [LongType, IntType, StringType], [LongType, IntType] ]
  def export_getJobInfo(self, prodNameOrID, jobID):
    """ Get job information for a given JobID and Production ID
    """
    return productionDB.getJobInfo(prodNameOrID, jobID)

  types_deleteJobs = [ [LongType, IntType, StringType], [LongType, IntType], [LongType, IntType] ]
  def export_deleteJobs(self, prodNameOrID, jobIDmin, jobIDmax):
    """ Delete Jobs from Production ID between jobIDmin, jobIDmax
    """
    return productionDB.deleteJobs(prodNameOrID, jobIDmin, jobIDmax)

  types_getProductionSummary = []
  def export_getProductionSummary(self):
    """ Get the summary of the currently existing productions
    """

    result = productionDB.getProductionList()
    if not result['OK']:
      return result

    prodList = result['Value']
    resultDict = {}
    for prod in prodList:
      prodID = prod['TransformationID']
      result = productionDB.getJobStats(prodID)
      if not result['OK']:
        gLogger.warn('Failed to get job statistics for production %d' % prodID)
        continue
      statDict = result['Value']
      prod['JobStats'] = statDict
      resultDict[prodID] = prod

    return S_OK(resultDict)

  types_createProductionRequest = [DictType]
  def export_createProductionRequest(self,requestDict):
    """ Create production request
    """

    return productionDB.createProductionRequest(requestDict)

  types_getProductionRequest = [ListType]
  def export_getProductionRequest(self,requestIDList):
    """ Get production request(s) specified by the list of requestIDs
    """

    return productionDB.getProductionRequest(requestIDList)

  types_updateProductionRequest = [[LongType,IntType],DictType]
  def export_updateProductionRequest(self,requestID,requestDict):
    """ Update production request specified by requestID
    """

    return productionDB.updateProductionRequest(requestID,requestDict)