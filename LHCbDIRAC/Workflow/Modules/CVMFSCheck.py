""" CVMFSCheck module for SAM jobs
"""

__RCSID__ = "$Id$"

import os

from DIRAC import gLogger, S_ERROR, S_OK
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase


class CVMFSCheck( ModuleBase ):
  """
  CVMFSCheck module extending more generic LHCb jobs ModuleBase.
  """

  def __init__( self ):
    """ Constructor
    """
    logger = gLogger.getSubLogger( self.__class__.__name__ )
    super( CVMFSCheck, self ).__init__( loggerIn = logger )
    self.version = __RCSID__

  def _resolveInputVariables( self ):
    """ Resolve all input variables for the module here.
    """

    super( CVMFSCheck, self )._resolveInputVariables()
    super( CVMFSCheck, self )._resolveInputStep()

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None, ):
    """ Main method. If ${VO_LHCB_SW_DIR}/lib/etc/cernvmfs is present, we continue the SAM job.
    """
    try:
      super( CVMFSCheck, self ).execute( self.version, 'SAM', '0000', wms_job_id,
                                         workflowStatus, stepStatus,
                                         wf_commons, step_commons, '1', step_id )


      self._resolveInputVariables()

      self.log.info( 'Checking presence of CVMFS' )

      if not 'VO_LHCB_SW_DIR' in os.environ:
        self.workflow_commons.setdefault( 'SAMResults', {})[ 'CVMFS' ]  = 'CRITICAL'
        self.workflow_commons.setdefault( 'SAMDetails', {})[ 'CVMFS' ] = 'Environment variable VO_LHCB_SW_DIR not found'
        self.setApplicationStatus( 'CVMFS KO' )
        self.log.error( "Environment variable VO_LHCB_SW_DIR not found" )
        return S_ERROR( "Environment variable VO_LHCB_SW_DIR not found" )

      swDir = os.environ[ 'VO_LHCB_SW_DIR' ]

      if not swDir:
        self.workflow_commons.setdefault( 'SAMResults', {})[ 'CVMFS' ]  = 'CRITICAL'
        self.workflow_commons.setdefault( 'SAMDetails', {})[ 'CVMFS' ] = \
                               'Environment variable VO_LHCB_SW_DIR is empty'
        self.setApplicationStatus( 'CVMFS KO' )
        self.log.error( "Environment variable VO_LHCB_SW_DIR is empty" )
        return S_ERROR( "Environment variable VO_LHCB_SW_DIR is empty" )

      self.log.info( "VO_LHCB_SW_DIR: '%s'" % swDir )

      cvmfsFilePath = os.path.join( swDir, 'lib/etc/cernvmfs' )
      self.log.info( "CVMFS file path '%s'" % cvmfsFilePath )

      if not os.path.exists( cvmfsFilePath ):
        self.workflow_commons.setdefault( 'SAMResults', {})[ 'CVMFS' ]  = 'CRITICAL'
        self.workflow_commons.setdefault( 'SAMDetails', {})[ 'CVMFS' ]  = \
             "CVMFS file path '%s' does not exist" % cvmfsFilePath
        self.setApplicationStatus( 'CVMFS KO' )
        self.log.error( "CVMFS file path '%s' does not exist" % cvmfsFilePath )
        return S_ERROR( "CVMFS file path does not exist" )

      self.workflow_commons.setdefault( 'SAMResults', {})[ 'CVMFS' ]  = 'OK'
      self.workflow_commons.setdefault( 'SAMDetails', {})[ 'CVMFS' ] = 'CVMFS is present. '
      self.setApplicationStatus( 'CVMFS OK' )
      self.log.info( 'CVMFS is present' )
      return S_OK( 'CVMFS is present' )

    except Exception as e: #pylint:disable=broad-except
      self.log.exception( "Failure in CVMFSCheck execute module", lException = e )
      self.workflow_commons.setdefault( 'SAMResults', {})[ 'CVMFS' ] = 'CRITICAL'
      self.workflow_commons.setdefault( 'SAMDetails', {})[ 'CVMFS' ] = e
      self.setApplicationStatus( repr(e) )
      return S_ERROR( str(e) )

    finally:
      super( CVMFSCheck, self ).finalize( self.version )

# ...............................................................................
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
