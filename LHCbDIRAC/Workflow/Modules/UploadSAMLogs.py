""" LHCbDIRAC.Workflow.Modules.UploadSAMLogs

"""

import glob
import os
import shutil
import time

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.Core.Utilities.ReturnValues import returnSingleResult
from DIRAC.Core.Utilities.File import mkDir

from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from LHCbDIRAC.Core.Utilities.NagiosConnector import NagiosConnector

__RCSID__ = "$Id$"

class UploadSAMLogs( ModuleBase ):
  """ UploadSAMLogs extends Workflow.Modules.ModuleBase

      it is used by SAMJobs to upload their outputs, as they do not need a very
      complex module to do it.

      Takes all logs and uploads to LogSE.
  """

  __logExtensions = [ '*.log' ]

  def __init__( self ):
    """Module initialization.
    """

    logger = gLogger.getSubLogger( self.__class__.__name__ )
    super( UploadSAMLogs, self ).__init__( loggerIn = logger )

    self.opsH = Operations()

    self.logSE = 'LogSE'
    self.logURL = 'http://lhcb-logs.cern.ch/storage/'
    self.nagiosConnector = NagiosConnector()

    self.storageElement = StorageElement( self.logSE )
    self.version = __RCSID__

  def _resolveInputVariables( self ):
    """ Resolve all input variables for the module here.
    """

    super( UploadSAMLogs, self )._resolveInputVariables()
    super( UploadSAMLogs, self )._resolveInputStep()

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None, ):
    """ Main method.
    """
    try:
      super( UploadSAMLogs, self ).execute( self.version, 'SAM', '0000', wms_job_id,
                                            workflowStatus, stepStatus,
                                            wf_commons, step_commons, '2', step_id )

      self._resolveInputVariables()

      self.log.verbose( 'WORKFLOW_COMMONS' )
      self.log.verbose( self.workflow_commons )
      self.log.verbose( 'STEP_COMMONS' )
      self.log.verbose( self.step_commons )

      self.log.info( 'Starting %s module execution' % self.__class__.__name__ )

      logDir = '%s/%s' % ( os.getcwd(), 'log' )
      self.log.verbose( 'Creating log directory %s' % logDir )
      mkDir( logDir )

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
      self.workflow_commons['logURL'] =  self.logURL + lfnPath[1:]
      if not self._enableModule():
        return S_OK( 'No logs to upload' )

      result = returnSingleResult( self.storageElement.putDirectory( { lfnPath : os.path.realpath( logDir ) } ) )

      self.log.verbose( result )
      if not result[ 'OK' ]:
        self.log.error( result )
        return result

      logReference = '<a href="%s%s">Log file directory</a>' % ( self.logURL, lfnPath )
      self.log.verbose( 'Adding Log URL job parameter: %s' % logReference )
      res = self.setJobParameter( 'Log URL', logReference )
      self.log.info( res )

      self.finalize( self.__class__.__name__ )

      # publish information to SAM-Nagios
      try:
        self.nagiosConnector.readConfig()
        self.nagiosConnector.initializeConnection()
        self.nagiosConnector.assembleMessage( serviceURI = self.workflow_commons[ 'GridRequiredCEs' ],
                                              # if one step fails, set the status to 'CRITICAL'
                                              # in a SAMJob only OK (False) and CRITICAL (True)  are used
                                              # this function will also accept appropriate strings or numbers 0-3
                                              status = 'CRITICAL' in self.workflow_commons[ 'SAMResults' ].values(),
                                              details = "Job_ID: %s. Logfile: %s Details: %s " % ( self.workflow_commons[ 'logURL' ].split( '/' )[-1],
                                                                                                   self.workflow_commons[ 'logURL' ],
                                                                                                   ". ".join( self.workflow_commons['SAMDetails'].values() ) ),
                                              nagiosName = 'org.lhcb.DiracTest-lhcb' )

        self.nagiosConnector.sendMessage()
        self.nagiosConnector.endConnection()
      except Exception as e: #pylint:disable=broad-except
        self.log.exception( 'Exception in UploadSAMLogs', lException = e )
        return S_ERROR( str(e) )

      return S_OK( 'Logs uploaded' )

    except Exception as e: #pylint:disable=broad-except
      self.log.exception( "Failure in UploadSAMLogs execute module", lException = e )
      return S_ERROR( e )

    finally:
      super( UploadSAMLogs, self ).finalize( self.version )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
