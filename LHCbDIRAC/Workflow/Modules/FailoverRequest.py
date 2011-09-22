########################################################################
# $Id$
########################################################################
""" Create and send a combined request for any pending operations at
    the end of a job.
"""

__RCSID__ = "$Id$"

from DIRAC                                                 import S_OK, S_ERROR, gLogger
from LHCbDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase

class FailoverRequest( ModuleBase ):

  #############################################################################

  def __init__( self ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "FailoverRequest" )
    super( FailoverRequest, self ).__init__( self.log )

    self.version = __RCSID__

    #Workflow parameters
    self.jobReport = None
    self.fileReport = None
    self.request = None
    self.inputData = []

  #############################################################################

  def resolveInputVariables( self ):
    """ By convention the module input parameters are resolved here.
    """
    self.log.debug( self.workflow_commons )
    self.log.debug( self.step_commons )

    if self.workflow_commons.has_key( 'JobReport' ):
      self.jobReport = self.workflow_commons['JobReport']

    if self.workflow_commons.has_key( 'FileReport' ):
      self.fileReport = self.workflow_commons['FileReport']

    if self.workflow_commons.has_key( 'InputData' ):
      self.inputData = self.workflow_commons['InputData']
      if self.inputData:
        if type( self.inputData ) != type( [] ):
          self.inputData = self.inputData.split( ';' )
      else:
        self.inputData = []

      self.inputData = [x.replace( 'LFN:', '' ) for x in self.inputData]

    if self.workflow_commons.has_key( 'Request' ):
      self.request = self.workflow_commons['Request']
    else:
      from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
      self.request = RequestContainer()
      self.request.setRequestName( 'job_%s_request.xml' % self.jobID )
      self.request.setJobID( self.jobID )
      self.request.setSourceComponent( "Job_%s" % self.jobID )

      #useless, IMHO
#    if self.workflow_commons.has_key( 'PRODUCTION_ID' ):
#      self.productionID = self.workflow_commons['PRODUCTION_ID']
#
#    if self.workflow_commons.has_key( 'JOB_ID' ):
#      self.prodJobID = self.workflow_commons['JOB_ID']

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
                workflowStatus = None, stepStatus = None,
                wf_commons = None, step_commons = None,
                step_number = None, step_id = None ):
    """ Main execution function.
    """

    super( FailoverRequest, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                            workflowStatus, stepStatus,
                                            wf_commons, step_commons, step_number, step_id )

    if not self._enableModule():
      return S_OK()

    self.resolveInputVariables()

    if not self.fileReport:
      self.log.debug( 'Getting FileReport object' )
      from DIRAC.TransformationSystem.Client.FileReport import FileReport
      self.fileReport = FileReport( 'ProductionManagement/ProductionManager' )

    if self.inputData:
      inputFiles = self.fileReport.getFiles()
      for lfn in self.inputData:
        if not lfn in inputFiles:
          self.log.verbose( 'No status populated for input data %s, setting to "Unused"' % lfn )
          result = self.fileReport.setFileStatus( int( self.productionID ), lfn, 'Unused' )

    if not self._checkWFAndStepStatus():
      inputFiles = self.fileReport.getFiles()
      for lfn in inputFiles:
        if inputFiles[lfn] != 'ApplicationCrash':
          self.log.info( 'Forcing status to "Unused" due to workflow failure for: %s' % ( lfn ) )
          self.fileReport.setFileStatus( int( self.productionID ), lfn, 'Unused' )
    else:
      inputFiles = self.fileReport.getFiles()

      if inputFiles:
        self.log.info( 'Workflow status OK, setting input file status to Processed' )
      for lfn in inputFiles:
        self.log.info( 'Setting status to "Processed" for: %s' % ( lfn ) )
        self.fileReport.setFileStatus( int( self.productionID ), lfn, 'Processed' )

    result = self.fileReport.commit()

    if not result['OK']:
      self.log.error( 'Failed to report file status to ProductionDB, request will be generated', result['Message'] )
    else:
      self.log.info( 'Status of files have been properly updated in the ProcessingDB' )

    # Must ensure that the local job report instance is used to report the final status
    # in case of failure and a subsequent failover operation
    if self.workflowStatus['OK'] and self.stepStatus['OK']:
      self.setApplicationStatus( 'Job Finished Successfully', jr = self.jobReport )

    # Retrieve the accumulated reporting request
    reportRequest = None
    if self.jobReport:
      result = self.jobReport.generateRequest()
      if not result['OK']:
        self.log.warn( 'Could not generate request for job report with result:\n%s' % ( result ) )
      else:
        reportRequest = result['Value']
    if reportRequest:
      self.log.info( 'Populating request with job report information' )
      self.request.update( reportRequest )

    fileReportRequest = None
    if self.fileReport:
      result = self.fileReport.generateRequest()
      if not result['OK']:
        self.log.warn( 'Could not generate request for file report with result:\n%s' % ( result ) )
      else:
        fileReportRequest = result['Value']
    if fileReportRequest:
      self.log.info( 'Populating request with file report information' )
      result = self.request.update( fileReportRequest )

    accountingReport = None
    if self.workflow_commons.has_key( 'AccountingReport' ):
      accountingReport = self.workflow_commons['AccountingReport']
    if accountingReport:
      result = accountingReport.commit()
      if not result['OK']:
        self.log.info( 'Populating request with accounting report information' )
        self.request.setDISETRequest( result['rpcStub'] )

    if self.request.isEmpty()['Value']:
      self.log.info( 'Request is empty, nothing to do.' )
      return self.finalize()

    request_string = self.request.toXML()['Value']
    self.log.debug( request_string )
    # Write out the request string
    fname = '%s_%s_request.xml' % ( self.production_id, self.prod_job_id )
    xmlfile = open( fname, 'w' )
    xmlfile.write( request_string )
    xmlfile.close()
    self.log.info( 'Creating failover request for deferred operations for job %s:' % self.jobID )
    result = self.request.getDigest()
    if result['OK']:
      digest = result['Value']
      self.log.info( digest )

    return self.finalize()

  #############################################################################

  def finalize( self ):
    """ Finalize and report correct status for the workflow based on the workflow
        or step status.
    """
    self.log.verbose( 'Workflow status = %s, step status = %s' % ( self.workflowStatus['OK'], self.stepStatus['OK'] ) )
    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.warn( 'Workflow status is not ok, will not overwrite status' )
      self.log.info( 'Workflow failed, end of FailoverRequest module execution.' )
      return S_ERROR( 'Workflow failed, FailoverRequest module completed' )

    self.log.info( 'Workflow successful, end of FailoverRequest module execution.' )
    return S_OK( 'FailoverRequest module completed' )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
