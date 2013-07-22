""" LHCbDIRAC.SAMSystem.Modules.UploadSAMLogs

"""

import glob
import os
import shutil
import time

from DIRAC                                               import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager

from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase

__RCSID__ = '$Id$'

class UploadSAMLogs( ModuleBase ):
  """
    UploadSAMLogs extends Workflow.Modules.ModuleBase

      it is used by SAMJobs to upload their outputs, as they do not need a very
      complex module to do it.

      Takes all logs and uploads to LogSE.
  """

  __logExtensions = [ '*.log' ]

  def __init__( self ):

    logger = gLogger.getSubLogger( self.__class__.__name__ )
    super( UploadSAMLogs, self ).__init__( loggerIn = logger )

    self.opsH = Operations()
    self.rManager = ReplicaManager()

    self.logSE = 'LogSE'
    self.logURL = 'http://lhcb-logs.cern.ch/storage/'

    self.version = __RCSID__

  def _resolveInputVariables( self ):
    """ Resolve all input variables for the module here.
    """

    super( UploadSAMLogs, self )._resolveInputVariables()
    super( UploadSAMLogs, self )._resolveInputStep()

  def execute( self ):
    """
      Main method.
    """
    try:
      super( UploadSAMLogs, self ).execute( self.version, production_id = 'SAM', prod_job_id = '0000', step_number = '2' )

      self._resolveInputVariables()

      self.log.verbose( 'WORKFLOW_COMMONS' )
      self.log.verbose( self.workflow_commons )
      self.log.verbose( 'STEP_COMMONS' )
      self.log.verbose( self.step_commons )

      self.log.info( 'Starting %s module execution' % self.__class__.__name__ )

      logDir = '%s/%s' % ( os.getcwd(), 'log' )
      self.log.verbose( 'Creating log directory %s' % logDir )

      try:
        os.mkdir( logDir )
      except OSError:
        return S_ERROR( 'Could not create log directory %s' % logDir )

      logExtensions = self.opsH.getValue( 'SAM/LogFiles', self.__logExtensions )
      self.log.verbose( 'logExtensions %s' % logExtensions )


      self.log.verbose( 'Files to be uploaded:' )
      for logExtension in logExtensions:

        # Usage of iglob, which is much faster
        for filePath in glob.iglob( logExtension ):
          if os.path.isfile( filePath ):
            self.log.verbose( filePath )
            shutil.copy( filePath, logDir )

      ce = self.workflow_commons.get( 'GridRequiredCEs', '' )
      if not ce:
        self.log.error( 'No GridRequiredCE on workflow_commons' )
        return S_ERROR( 'No GridRequiredCE on workflow_commons' )

      date = time.strftime( '%Y-%m-%d' )
      lfnPath = '/lhcb/test/sam/%s/%s/%d' % ( ce, date, self.jobID )

      self.log.verbose( 'lfnPath: %s' % lfnPath )

      if not self._enableModule():
        return S_OK( 'No logs to upload' )

      result = self.rManager.putStorageDirectory( { lfnPath : os.path.realpath( logDir ) },
                                                   self.logSE, singleDirectory = True )

      self.log.verbose( result )
      if not result[ 'OK' ]:
        self.log.error( result )
        return result

      logReference = '<a href="%s%s">Log file directory</a>' % ( self.logURL, lfnPath )
      self.log.verbose( 'Adding Log URL job parameter: %s' % logReference )
      res = self.setJobParameter( 'Log URL', logReference )
      self.log.info( res )

      self.finalize( self.__class__.__name__ )

      return S_OK( 'Logs uploaded' )

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( UploadSAMLogs, self ).finalize( self.version )


################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
