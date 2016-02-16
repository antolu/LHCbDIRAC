""" Root Application Class """

__RCSID__ = "$Id: RootApplication.py 82023 2015-03-23 14:22:01Z fstagni $"

import os, sys, fnmatch

from DIRAC                                            import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Subprocess                  import shellCall

from LHCbDIRAC.Core.Utilities.ProductionEnvironment   import getProjectEnvironment
from LHCbDIRAC.Workflow.Modules.ModuleBase            import ModuleBase

class RootApplication( ModuleBase ):

  #############################################################################
  def __init__( self, bkClient = None, dm = None ):
    """ Module initialization
    """

    self.log = gLogger.getSubLogger( "RootApplication" )
    super( RootApplication, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__

    self.applicationLog = ''
    self.rootVersion = ''
    self.rootScript = ''
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
      self.rootScript = self.step_commons['rootScript']
    else:
      self.log.warn( 'No rootScript defined' )

    if self.step_commons.has_key( 'rootVersion' ):
      self.rootVersion = self.step_commons['rootVersion']
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
               step_id = None, step_number = None,
               projectEnvironment = None ):
    """The main execution method of the RootApplication module.
    """

    try:

      super( RootApplication, self ).execute( self.version, production_id, prod_job_id, wms_job_id, workflowStatus,
                                              stepStatus, wf_commons, step_commons, step_number, step_id )

      self._resolveInputVariables()

      if not self.rootVersion:
        raise RuntimeError( 'No Root Version defined' )
      elif not self.systemConfig:
        raise RuntimeError( 'No system configuration selected' )
      elif not self.rootScript:
        raise RuntimeError( 'No script defined' )
      elif not self.applicationLog:
        self.applicationLog = '%s.log' % self.rootScript
      elif not self.rootType.lower() in ( 'c', 'py', 'bin', 'exe' ):
        raise RuntimeError( 'Wrong root type defined' )

      self.setApplicationStatus( 'Initializing RootApplication module' )

      self.log.debug( self.version )
      self.log.info( "Executing application Root %s with CMT config %s " % ( self.rootVersion, self.systemConfig ) )

      # Now obtain the project environment for execution
      result = getProjectEnvironment( self.systemConfig, 'ROOT', self.rootVersion )
      if not result['OK']:
        self.log.error( 'Could not obtain project environment with result: %s' % ( result ) )
        return result  # this will distinguish between LbLogin / SetupProject / actual application failures

      projectEnvironment = result['Value']

      if not os.path.exists( self.rootScript ):
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

          macroArgs = '%s\(%s\)' % ( self.rootScript, ','.join( escapedArgs ) )
          rootCmd.append( macroArgs )
        else:
          rootCmd.append( self.rootScript )

      elif self.rootType.lower() == 'py':

        rootCmd = ['python']
        rootCmd.append( self.rootScript )
        if self.arguments:
          rootCmd += self.arguments

      elif self.rootType.lower() in ( 'bin', 'exe' ):
        rootCmd = [os.path.abspath( self.rootScript )]
        if self.arguments:
          rootCmd += self.arguments

      if os.path.exists( self.applicationLog ):
        os.remove( self.applicationLog )

      self.log.info( 'Running:', ' '.join( rootCmd ) )
      self.setApplicationStatus( 'Running ROOT %s' % self.rootVersion )

      ret = shellCall( 0, ' '.join( rootCmd ), env = projectEnvironment, callbackFunction = self.redirectLogOutput )
      if not ret['OK']:
        self.log.warn( 'Error during: %s ' % rootCmd )
        self.log.warn( ret )
        return ret

      resultTuple = ret['Value']
      status = resultTuple[0]
      stdError = resultTuple[2]

      self.log.info( "Status after %s execution is %s" % ( self.rootScript, str( status ) ) )
      failed = False
      if status != 0:
        self.log.info( "%s execution completed with non-zero status:" % self.rootScript )
        failed = True
      elif len( stdError ) > 0:
        self.log.info( "%s execution completed with application warning:" % self.rootScript )
        self.log.info( stdError )
      else:
        self.log.info( "%s execution completed successfully:" % self.rootScript )

      if failed == True:
        self.log.error( "==================================\n StdError:\n" )
        self.log.error( stdError )
        self.setApplicationStatus( '%s Exited With Status %s' % ( self.rootScript, status ) )
        return S_ERROR( "Script execution completed with errors" )

      # Return OK assuming that subsequent module will spot problems
      self.setApplicationStatus( '%s (Root) Successful' % self.rootScript )
      return S_OK()

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( RootApplication, self ).finalize( self.version )

  #############################################################################

  def redirectLogOutput( self, fd, message ):
    """ just redirector
    """
    print message
    sys.stdout.flush()
    if message:
      if self.applicationLog:
        log = open( self.applicationLog, 'a' )
        log.write( message + '\n' )
        log.close()
      else:
        self.log.error( "Application Log file not defined" )

  #############################################################################

  def getPythonFromRoot( self, rootsys ):
    """Locate the external python version that root was built with.
    """
    includedir = os.path.join( rootsys, 'include' )
    pythondir = ''
    for fname in os.listdir( includedir ):
      if fnmatch.fnmatch( fname, '*[cC]onfig*' ):
        f = file( os.path.join( includedir, fname ) )
        for l in f:
          i = l.find( 'PYTHONDIR' )
          if not i == -1:
            pythondir = l[i:].split()[0].split( '=' )[1]

    if not pythondir:
      return S_ERROR( 'Root python version not found' )
    pythondir = pythondir.split( '/lcg/external/' )[1]
    extdir = includedir.split( '/lcg/external/' )[0]
    pythondir = os.path.join( extdir, 'lcg', 'external', pythondir )
    if not os.path.exists( pythondir ):
      return S_ERROR( 'External python %s not found' % ( pythondir ) )
    return S_OK( pythondir )


# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
