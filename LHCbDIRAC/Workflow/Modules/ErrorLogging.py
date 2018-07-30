""" The ErrorLogging module is used to perform error analysis using AppConfig
    utilities. This occurs at the end of each workflow step such that the
    step_commons dictionary can be utilized.

    Since not all projects are instrumented to work with the AppConfig
    error suite any failures will not be propagated to the workflow.
"""

import os
import shutil

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DErrno

from LHCbDIRAC.Core.Utilities.RunApplication import RunApplication, LbRunError, LHCbApplicationError, LHCbDIRACError
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase

__RCSID__ = "$Id$"

class ErrorLogging( ModuleBase ):

  #############################################################################

  def __init__( self, bkClient = None, dm = None ):
    """ c'tor
    """

    self.log = gLogger.getSubLogger( "ErrorLogging" )
    super( ErrorLogging, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

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
    self.errorLogNameHTML = ''
    self.errorLogNamejson = ''
    self.stdError = ''
    # Error log parameters
    self.defaultNameHTML = 'errors.html'
    self.defaultNamejson = 'errors.json'

  #############################################################################

  def _resolveInputVariables( self ):
    """ By convention the module input parameters are resolved here.
    """
    super( ErrorLogging, self )._resolveInputVariables()
    super( ErrorLogging, self )._resolveInputStep()

    self.errorLogFile = 'Error_Log_%s_%s_%s.log' % ( self.applicationName,
                                                     self.applicationVersion,
                                                     self.step_number )
    self.errorLogNameHTML = '%d_Errors_%s_%s_%s.html' % ( self.jobID,
                                                      self.applicationName,
                                                      self.applicationVersion,
                                                      self.step_number )
    self.errorLogNamejson = '%d_Errors_%s_%s_%s.json' % ( self.jobID,
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

      self._resolveInputVariables()

      if self.applicationName.lower() not in ( 'gauss', 'boole' ):
        self.log.info( 'Not Gauss nor Boole, exiting' )
        return S_OK()

      self.log.info( 'Executing ErrorLogging module for: %s %s %s' % ( self.applicationName,
                                                                       self.applicationVersion,
                                                                       self.applicationLog ) )
      if not os.path.exists( self.applicationLog ):
        self.log.info( 'Application log file from previous module not found locally: %s' % self.applicationLog )
        return S_OK()

      command = 'python %s %s %s %s %s %s %s' % ( self.executable, 
                                                  self.applicationLog, 
                                                  self.applicationName, 
                                                  self.applicationVersion,
                                                  prod_job_id,
                                                  production_id,
                                                  wms_job_id )

      # Set some parameter names
      scriptName = 'Error_Log_%s_%s_Run_%s.sh' % ( self.applicationName,
                                                   self.applicationVersion,
                                                   self.step_number )

      for x in [self.defaultNameHTML, self.defaultNamejson, scriptName, self.errorLogFile]:
        if os.path.exists( x ):
          os.remove( x )

      # How to run the application
      ra = RunApplication()
      # lb-run stuff
      ra.applicationName = self.applicationName
      ra.applicationVersion = self.applicationVersion
      ra.systemConfig = self.systemConfig
      # actual stuff to run
      ra.command = command

      # Now really running
      try:
        ra.run() # This would trigger an exception in case of failure, or application status != 0
      except RuntimeError as e:
        self.log.info( "Error logging for %s %s step %s completed with errors:" % ( self.applicationName,
                                                                                    self.applicationVersion,
                                                                                    self.step_number ) )
        self.log.info( 'Exiting without affecting workflow status' )
        return S_OK()

      if not os.path.exists( self.defaultNameHTML ):
        self.log.info( '%s not found locally, exiting without affecting workflow status' % self.defaultNameHTML )
        return S_OK()

      if not os.path.exists( self.defaultNamejson ):
        self.log.info( '%s not found locally, exiting without affecting workflow status' % self.defaultNamejson )
        return S_OK()

      self.log.info( "Error logging for %s %s step %s completed successfully:" % ( self.applicationName,
                                                                                   self.applicationVersion,
                                                                                   self.step_number ) )
      shutil.copy( self.defaultNameHTML, self.errorLogNameHTML )
      shutil.copy(self.defaultNamejson, self.errorLogNamejson)

      return S_OK()

    except LbRunError as lbre: # This is the case for lb-run/environment errors
      self.setApplicationStatus( repr(lbre) )
      return S_ERROR( DErrno.EWMSRESC, str(lbre) )
    except LHCbApplicationError as lbae: # This is the case for real application errors
      self.setApplicationStatus( repr(lbae) )
      return S_ERROR( str(lbae) )
    except LHCbDIRACError as lbde: # This is the case for LHCbDIRAC errors (e.g. subProcess call failed)
      self.setApplicationStatus( repr(lbde) )
      return S_ERROR( str(lbde) )
    except Exception as e: #pylint:disable=broad-except
      self.log.exception( "Failure in ErrorLogging execute module", lException = e )
      return S_ERROR( "Error in ErrorLogging module" )

    finally:
      super( ErrorLogging, self ).finalize( self.version )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
