""" Create and send a combined request for any pending operations at
    the end of a job:
      fileReport (for the transformation)
      jobReport (for jobs)
      accounting
      request (for failover)
"""

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DEncode
from DIRAC.RequestManagementSystem.Client.Operation import Operation

from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase

__RCSID__ = "$Id$"

class FailoverRequest( ModuleBase ):

  #############################################################################

  def __init__( self, bkClient = None, dm = None ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "FailoverRequest" )
    super( FailoverRequest, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__

  #############################################################################

  def _resolveInputVariables( self ):
    """ By convention the module input parameters are resolved here.
    """
    super( FailoverRequest, self )._resolveInputVariables()
    super( FailoverRequest, self )._resolveInputStep()

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None ):
    """ Main execution function.
    """

    try:

      super( FailoverRequest, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                              workflowStatus, stepStatus,
                                              wf_commons, step_commons, step_number, step_id )

      if not self._enableModule():
        return S_OK()

      self._resolveInputVariables()

      # preparing the request, just in case
      self.request.RequestName = 'job_%d_request.xml' % self.jobID
      self.request.JobID = self.jobID
      self.request.SourceComponent = "Job_%d" % self.jobID

      # report on the status of the input data, by default they are 'Processed', unless the job failed
      # failures happening before (e.g. in previous steps, or while inspecting the XML summary) are not touched.
      filesInFileReport = self.fileReport.getFiles() #It's normally empty, unless there are some Problematic files

      if not self._checkWFAndStepStatus( noPrint = True ):
        # To overcome race condition issues, the file status for this case is reported by the failover request
        statusDict = {}
        for lfn in self.inputDataList:
          if lfn not in filesInFileReport:
            self.log.info( "Setting status to 'Unused' due to workflow failure for input file: %s" % ( lfn ) )
            statusDict[lfn] = 'Unused'
        setFileStatusOp = Operation()
        setFileStatusOp.Type = 'SetFileStatus'
        setFileStatusOp.Arguments = DEncode.encode( {'transformation':int(self.production_id),
                                                     'statusDict':statusDict,
                                                     'force':False} )
        self.request.addOperation(setFileStatusOp)
      else:
        for lfn in self.inputDataList:
          if lfn not in filesInFileReport:
            self.log.verbose( "No status populated for input data %s, setting to 'Processed'" % lfn )
            self.fileReport.setFileStatus( int( self.production_id ), lfn, 'Processed' )

      result = self.fileReport.commit()
      if not result['OK']:
        self.log.error( "Failed to report file status to TransformationDB" )
        result = self.fileReport.generateForwardDISET() #This will try a second time a commit, before generating a SetFileStatus operation
        if not result['OK']:
          self.log.warn( "Could not generate Operation for file report with result:\n%s" % ( result['Value'] ) )
        else:
          if result['Value'] is None: #Means the FileReport managed to report, no need for a new operation
            self.log.info( "On second trial, files correctly reported to TransformationDB" )
          else:
            self.log.info( "Populating request with file report info (SetFileStatus operation)" )
            result = self.request.addOperation( result['Value'] )
            if not result['OK']:
              return result
      else:
        self.log.info( "Status of files have been properly updated in the TransformationDB" )

      # Must ensure that the local job report instance is used to report the final status
      # in case of failure and a subsequent failover operation
      if self.workflowStatus['OK'] and self.stepStatus['OK']:
        self.setApplicationStatus( "Job Finished Successfully" )

      self.generateFailoverFile()

      return S_OK()

    except Exception as e: #pylint:disable=broad-except
      self.log.exception( "Failure in FailoverRequest execute module", lException = e )
      self.setApplicationStatus( e )
      return S_ERROR( e )

    finally:
      super( FailoverRequest, self ).finalize( self.version )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
