########################################################################
# $Id$
########################################################################
""" Module to remove input data files for given workflow. Initially written
    for use after merged outputs have been successfully uploaded to an SE.
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase

class RemoveInputData( ModuleBase ):

  #############################################################################

  def __init__( self, bkClient = None, rm = None ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "RemoveInputData" )
    super( RemoveInputData, self ).__init__( self.log, bkClientIn = bkClient, rm = rm )

    self.version = __RCSID__

    #List all parameters here
    self.request = None
    self.inputDataList = []

  #############################################################################

  def _resolveInputVariables( self ):
    """ By convention the module parameters are resolved here.
    """

    super( RemoveInputData, self )._resolveInputVariables()

    #Get job input data files to be removed if previous modules were successful
    if self.InputData:
      self.inputDataList = self.InputData.split( ';' )
    self.inputDataList = [x.replace( 'LFN:', '' ) for x in self.inputDataList]

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
                workflowStatus = None, stepStatus = None,
                wf_commons = None, step_commons = None,
                step_number = None, step_id = None ):
    """ Main execution function.
    """

    try:

      super( RemoveInputData, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                              workflowStatus, stepStatus,
                                              wf_commons, step_commons, step_number, step_id )

      if not self._checkWFAndStepStatus():
        return S_OK()

      if not self._enableModule():
        return S_OK()

      result = self._resolveInputVariables()

      self.request.setRequestName( 'job_%s_request.xml' % self.jobID )
      self.request.setJobID( self.jobID )
      self.request.setSourceComponent( "Job_%s" % self.jobID )

      #Try to remove the file list with failover if necessary
      failover = []
      self.log.info( 'Attempting rm.removeFile("%s")' % ( self.inputDataList ) )
      result = self.rm.removeFile( self.inputDataList )
      self.log.verbose( result )
      if not result['OK']:
        self.log.error( 'Could not remove files with message:\n"%s"\n\
        Will set removal requests just in case.' % ( result['Message'] ) )
        failover = self.inputDataList
      try:
        if result['Value']['Failed']:
          failureDict = result['Value']['Failed']
          if failureDict:
            self.log.info( 'Not all files were successfully removed, see "LFN : reason" below\n%s' % ( failureDict ) )
          failover = failureDict.keys()
      except KeyError:
        self.log.error( 'Setting files for removal request to be the input data: %s' % self.inputDataList )
        failover = self.inputDataList

      for lfn in failover:
        self.__setFileRemovalRequest( lfn )

      self.workflow_commons['Request'] = self.request

      return S_OK( 'Input Data Removed' )

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( RemoveInputData, self ).finalize( self.version )

  #############################################################################

  def __setFileRemovalRequest( self, lfn ):
    """ Sets a removal request for a file including all replicas.
    """
    self.log.info( 'Setting file removal request for %s' % lfn )
    lastOperationOnFile = self.request._getLastOrder( lfn )
    result = self.request.addSubRequest( {'Attributes':{'Operation':'removeFile',
                                                       'TargetSE':'',
                                                       'ExecutionOrder':lastOperationOnFile + 1}},
                                         'removal' )
    index = result['Value']
    fileDict = {'LFN':lfn, 'Status':'Waiting'}
    result = self.request.setSubRequestFiles( index, 'removal', [fileDict] )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
