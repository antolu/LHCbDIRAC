""" Module to remove input data files for given workflow. Initially written
    for use after merged outputs have been successfully uploaded to an SE.
"""

__RCSID__ = "$Id: RemoveInputData.py 76563 2014-06-26 09:22:49Z fstagni $"

from DIRAC                                          import S_OK, S_ERROR, gLogger
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File      import File
from LHCbDIRAC.Workflow.Modules.ModuleBase          import ModuleBase

class RemoveInputData( ModuleBase ):

  #############################################################################

  def __init__( self, bkClient = None, dm = None ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "RemoveInputData" )
    super( RemoveInputData, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__

    # List all parameters here
    self.inputDataList = []

  #############################################################################

  def _resolveInputVariables( self ):
    """ By convention the module parameters are resolved here.
    """

    super( RemoveInputData, self )._resolveInputVariables()

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

      # Try to remove the file list with failover if necessary
      failover = []
      self.log.info( 'Attempting dm.removeFile("%s")' % ( self.inputDataList ) )
      result = self.dataManager.removeFile( self.inputDataList )
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
    removeFile = Operation()
    removeFile.Type = 'RemoveFile'
    rmFile = File()
    rmFile.LFN = lfn
    removeFile.addFile( rmFile )
    self.request.addOperation( removeFile )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
