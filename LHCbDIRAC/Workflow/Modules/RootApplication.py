""" Root Application Class """

import os

from DIRAC import S_OK, S_ERROR, gLogger

from LHCbDIRAC.Core.Utilities.RunApplication import RunApplication
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase

__RCSID__ = "$Id$"

class RootApplication( ModuleBase ):

  #############################################################################
  def __init__( self, bkClient = None, dm = None ):
    """ Module initialization
    """

    self.log = gLogger.getSubLogger( "RootApplication" )
    super( RootApplication, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__

    self.applicationLog = ''
    self.applicationVersion = ''
    self.applicationName = ''
    self.rootType = ''
    self.arguments = ''
    self.systemConfig = ''

  #############################################################################
  def _resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """

    super( RootApplication, self )._resolveInputVariables()
    super( RootApplication, self )._resolveInputStep()

    if self.step_commons.has_key( 'rootScript' ):
      self.applicationName = self.step_commons['rootScript']
    else:
      self.log.warn( 'No rootScript defined' )

    if self.step_commons.has_key( 'rootVersion' ):
      self.applicationVersion = self.step_commons['rootVersion']
    else:
      self.log.warn( 'No rootVersion defined' )

    if self.step_commons.has_key( 'rootType' ):
      self.rootType = self.step_commons['rootType']
    else:
      self.log.warn( 'No rootType defined' )

    if self.step_commons.has_key( 'arguments' ):
      self.arguments = self.step_commons['arguments']
      tmp = []
      for argument in self.arguments:
        if argument or not isinstance( argument, list ):
          tmp.append( argument )
      self.arguments = tmp
    else:
      self.log.warn( 'No arguments specified' )

    print self.arguments

  #############################################################################
  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_id = None, step_number = None ):
    """The main execution method of the RootApplication module: runs a ROOT app using RunApplication module
    """

    try:

      super( RootApplication, self ).execute( self.version, production_id, prod_job_id, wms_job_id, workflowStatus,
                                              stepStatus, wf_commons, step_commons, step_number, step_id )

      self._resolveInputVariables()

      if not self.applicationVersion:
        raise RuntimeError( 'No Root Version defined' )
      if not self.systemConfig:
        raise RuntimeError( 'No system configuration selected' )
      if not self.applicationName:
        raise RuntimeError( 'No script defined' )
      if not self.applicationLog:
        self.applicationLog = '%s.log' % self.applicationName
      if self.rootType.lower() not in ( 'c', 'py', 'bin', 'exe' ):
        raise RuntimeError( 'Wrong root type defined' )

      self.setApplicationStatus( 'Initializing RootApplication module' )

      self.log.info( "Executing application Root %s with CMT config %s " % ( self.applicationVersion, self.systemConfig ) )

      if not os.path.exists( self.applicationName ):
        self.log.error( 'rootScript not Found at %s' % os.path.abspath( '.' ) )
        return S_ERROR( 'rootScript not Found' )

      if self.rootType.lower() == 'c':
        rootCmd = ['root']
        rootCmd.append( '-b' )
        rootCmd.append( '-f' )
        if self.arguments:
          escapedArgs = []
          for arg in self.arguments:
            if isinstance( arg, str ):
              escapedArgs.append( '\'"%s"\'' % ( arg ) )
            else:
              escapedArgs.append( '%s' % ( arg ) )

          macroArgs = r'%s\(%s\)' % ( self.applicationName, ','.join( escapedArgs ) )
          rootCmd.append( macroArgs )
        else:
          rootCmd.append( self.applicationName )

      elif self.rootType.lower() == 'py':

        rootCmd = ['python']
        rootCmd.append( self.applicationName )
        if self.arguments:
          rootCmd += self.arguments

      elif self.rootType.lower() in ( 'bin', 'exe' ):
        rootCmd = [os.path.abspath( self.applicationName )]
        if self.arguments:
          rootCmd += self.arguments

      if os.path.exists( self.applicationLog ):
        os.remove( self.applicationLog )

      # How to run the application
      ra = RunApplication()
      # lb-run stuff
      ra.applicationName = self.applicationName
      ra.applicationVersion = self.applicationVersion
      ra.systemConfig = self.systemConfig
      # actual stuff to run
      ra.command = rootCmd
      ra.applicationLog = self.applicationLog

      # Now really running
      ra.run() # This would trigger an exception in case of failure, or application status != 0

      # Return OK assuming that subsequent module will spot problems
      self.setApplicationStatus( '%s (Root) Successful' % self.applicationName )
      return S_OK()

    except Exception as e: #pylint:disable=broad-except
      self.log.exception( "Failure in RootApplication execute module", lException = e )
      return S_ERROR( str(e) )

    finally:
      super( RootApplication, self ).finalize( self.version )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
