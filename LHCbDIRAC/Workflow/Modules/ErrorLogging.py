""" The ErrorLogging module is used to perform error analysis using AppConfig
    utilities. This occurs at the end of each workflow step such that the
    step_commons dictionary can be utilized.

    Since not all projects are instrumented to work with the AppConfig
    error suite any failures will not be propagated to the workflow.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Utilities.Subprocess                       import shellCall

from LHCbDIRAC.Core.Utilities.ProductionEnvironment         import getProjectEnvironment, addCommandDefaults, createDebugScript
from LHCbDIRAC.Workflow.Modules.ModuleBase                  import ModuleBase

from DIRAC import S_OK, S_ERROR, gLogger

import os, shutil, sys

class ErrorLogging( ModuleBase ):

  #############################################################################

  def __init__( self, bkClient = None, rm = None ):
    """ c'tor
    """

    self.log = gLogger.getSubLogger( "ErrorLogging" )
    super( ErrorLogging, self ).__init__( self.log, bkClientIn = bkClient, rm = rm )

    self.version = __RCSID__
    # Step parameters
    self.applicationName = ''
    self.applicationVersion = ''
    self.applicationLog = ''
    # Workflow commons parameters
    self.systemConfig = ''
    # Internal parameters
    self.executable = '$APPCONFIGROOT/scripts/LogErr.py'
    self.errorLogFile = ''
    self.errorLogName = ''
    self.stdError = ''
    # Error log parameters
    self.defaultName = 'errors.html'

  #############################################################################

  def _resolveInputVariables( self ):
    """ By convention the module input parameters are resolved here.
    """
    super( ErrorLogging, self )._resolveInputVariables()
    super( ErrorLogging, self )._resolveInputStep()

    self.errorLogFile = 'Error_Log_%s_%s_%s.log' % ( self.applicationName,
                                                     self.applicationVersion,
                                                     self.step_number )
    self.errorLogName = '%d_Errors_%s_%s_%s.html' % ( self.jobID,
                                                      self.applicationName,
                                                      self.applicationVersion,
                                                      self.step_number )

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None ):
    """ Main execution function. Always return S_OK() because we don't want the
        job execution result to depend on retrieving errors from logs.

        This module will run regardless of the workflow status.
    """

    try:

      super( ErrorLogging, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                           workflowStatus, stepStatus,
                                           wf_commons, step_commons, step_number, step_id )

      result = self._resolveInputVariables()

      if self.applicationName.lower() not in ( 'gauss', 'boole' ):
        self.log.info( 'Not Gauss nor Boole, exiting' )
        return S_OK()

      self.log.info( 'Executing ErrorLogging module for: %s %s %s' % ( self.applicationName,
                                                                       self.applicationVersion,
                                                                       self.applicationLog ) )
      if not os.path.exists( self.applicationLog ):
        self.log.info( 'Application log file from previous module not found locally: %s' % self.applicationLog )
        return S_OK()

      # Now obtain the project environment for execution
      result = getProjectEnvironment( self.systemConfig,
                                      self.applicationName,
                                      applicationVersion = self.applicationVersion,
                                      extraPackages = self.extraPackages )
      if not result['OK']:
        self.log.error( 'Could not obtain project environment with result: %s' % ( result ) )
        return S_OK()

      projectEnvironment = result['Value']
      command = 'python $APPCONFIGROOT/scripts/LogErr.py %s %s %s' % ( self.applicationLog,
                                                                       self.applicationName,
                                                                       self.applicationVersion )

      # Set some parameter names
      scriptName = 'Error_Log_%s_%s_Run_%s.sh' % ( self.applicationName,
                                                   self.applicationVersion,
                                                   self.step_number )
      dumpEnvName = 'Environment_Dump_ErrorLogging_Step%s.log' % ( self.step_number )
      coreDumpName = 'ErrorLogging_Step%s' % ( self.step_number )

      # Wrap final execution command with defaults
      finalCommand = addCommandDefaults( command,
                                         envDump = dumpEnvName,
                                         coreDumpLog = coreDumpName )['Value']  # should always be S_OK()

      # Create debug shell script to reproduce the application execution
      debugResult = createDebugScript( scriptName,
                                       command,
                                       env = projectEnvironment,
                                       envLogFile = dumpEnvName,
                                       coreDumpLog = coreDumpName )  # will add command defaults internally
      if debugResult['OK']:
        self.log.verbose( 'Created debug script %s for Step %s' % ( debugResult['Value'], self.step_number ) )

      for x in [self.defaultName, scriptName, self.errorLogFile]:
        if os.path.exists( x ): os.remove( x )

      result = shellCall( 120, finalCommand, env = projectEnvironment, callbackFunction = self.redirectLogOutput )
      status = result['Value'][0]
      self.log.info( "Status after the ErrorLogging execution is %s (if non-zero this is ignored)" % ( status ) )

      if status:
        self.log.info( "Error logging for %s %s step %s completed with errors:" % ( self.applicationName,
                                                                                    self.applicationVersion,
                                                                                    self.step_number ) )
        self.log.info( "==================================\n StdError:\n" )
        self.log.info( self.stdError )
        self.log.info( 'Exiting without affecting workflow status' )
        return S_OK()

      if not os.path.exists( self.defaultName ):
        self.log.info( '%s not found locally, exiting without affecting workflow status' % self.defaultName )
        return S_OK()

      self.log.info( "Error logging for %s %s step %s completed successfully:" % ( self.applicationName,
                                                                                  self.applicationVersion,
                                                                                  self.step_number ) )
      shutil.copy( self.defaultName, self.errorLogName )

      # TODO - report to error logging service when suitable method is available
      return S_OK()

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( ErrorLogging, self ).finalize( self.version )

  #############################################################################

  def redirectLogOutput( self, fd, message ):
    sys.stdout.flush()
    if message:
      if self.errorLogFile:
        log = open( self.errorLogFile, 'a' )
        log.write( message + '\n' )
        log.close()
      else:
        self.log.error( "Error Log file not defined" )
      if fd == 1:
        self.stdError += message

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
