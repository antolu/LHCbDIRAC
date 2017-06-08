""" Utility for invoking running LHCb applications
"""

import sys
import os
import multiprocessing
import shlex

from DIRAC import gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.List import fromChar
from DIRAC.Core.Utilities.Subprocess import systemCall

__RCSID__ = "$Id$"

class LbRunError(RuntimeError):
  """ Exception for command errors
  """
  pass

class LHCbApplicationError(RuntimeError):
  """ Exception for application errors
  """
  pass

class RunApplication(object):
  """ Encapsulate logic for running an LHCb application
  """

  def __init__(self):
    """ c'tor - holds common variables
    """
    # Standard LHCb scripts
    self.runApp = 'lb-run'
    self.lhcbEnvironment = None # This may be added (the result of LbLogin), but by default it won't be

    # What to run
    self.applicationName = '' # e.g. Gauss
    self.applicationVersion = '' # e.g v42r1

    # Define the environment
    self.extraPackages = []
    self.systemConfig = 'ANY'
    self.runTimeProject = ''
    self.runTimeProjectVersion = ''
    self.site = ''

    # What to run and how
    self.command = 'gaudirun.py'
    self.commandOptions = []
    self.prodConf = False
    self.prodConfFileName = 'prodConf.py'
    self.step_number = 0
    self.extraOptionsLine = ''
    self.multicore = False

    self.applicationLog = 'applicationLog.txt'
    self.stdError = 'applicationError.txt'

    # Utilities
    self.log = gLogger.getSubLogger( "RunApplication" )
    self.opsH = Operations()

  def run( self ):
    """ Invokes lb-run (what you call after having setup the object)
    """
    self.log.info( "Executing application %s %s for CMT configuration '%s'" % ( self.applicationName,
                                                                                self.applicationVersion,
                                                                                self.systemConfig ) )

    lbRunOptions = self.opsH.getValue('GaudiExecution/lbRunOptions', '--use-grid')

    extraPackagesString, runtimeProjectString, externalsString = self._lbRunCommandOptions()

    # "CMT" Config
    # if a CMTConfig is provided, then only that should be called (this should be safeguarded with the following method)
    # if not, we call "-c best"
    if self.systemConfig.lower() == 'any':
      self.systemConfig = 'best'
    configString = "-c %s" % self.systemConfig

    # App
    app = self.applicationName
    if self.applicationVersion:
      app += '/' + self.applicationVersion

    # Command
    if self.command == 'gaudirun.py':
      command = self._gaudirunCommand()
    else:
      command = self.command

    finalCommand = ' '.join( [self.runApp, lbRunOptions,
                              configString, extraPackagesString,
                              runtimeProjectString, externalsString,
                              app, command] )

    # Run it!
    runResult = self._runApp( finalCommand, self.lhcbEnvironment )
    if not runResult['OK']:
      self.log.error( "Problem executing lb-run: %s" % runResult['Message'] )
      if self.lhcbEnvironment:
        self.log.error( "LHCb environment used: %s" % self.lhcbEnvironment )
      else:
        self.log.error( "Environment: %s" % os.environ )
      raise LbRunError( "Can not start %s %s" % ( self.applicationName, self.applicationVersion ) )

    if runResult['Value'][0]: # if exit status != 0
      self.log.error( "lb-run or its application exited with status %d" % runResult['Value'][0] )
      raise LHCbApplicationError( "%s %s exited with status %d" % ( self.applicationName, self.applicationVersion, runResult['Value'][0] ) )

    self.log.info( "%s execution completed successfully" % self.applicationName )

    return runResult

  def _lbRunCommandOptions( self ):
    """ Return lb-run command options
    """

    # extra packages (for setup phase) (added using '--use')
    extraPackagesString = ''
    for ep in self.extraPackages:
      if len(ep) == 1:
        epName = ep[0]
        epVer = ''
      elif len(ep) == 2:
        epName, epVer = ep

      if epName.lower() == 'prodconf':
        self.prodConf = True
      if epVer:
        extraPackagesString += ' --use="%s %s" ' % ( epName, epVer )
      else:
        extraPackagesString += ' --use="%s"' % epName

    # run time project
    runtimeProjectString = ''
    if self.runTimeProject:
      self.log.verbose( 'Requested run time project: %s' % ( self.runTimeProject ) )
      runtimeProjectString = ' --runtime-project %s/%s' % ( self.runTimeProject , self.runTimeProjectVersion )

    externalsString = ''

    externals = []
    if self.site:
      externals = self.opsH.getValue( 'ExternalsPolicy/%s' % ( self.site ), [] )
      if externals:
        self.log.info( 'Found externals policy for %s = %s' % ( self.site, externals ) )
        for external in externals:
          externalsString += ' --ext=%s' % external

    if not externalsString:
      externals = self.opsH.getValue( 'ExternalsPolicy/Default', [] )
      if externals:
        self.log.info( 'Using default externals policy for %s = %s' % ( self.site, externals ) )
        for external in externals:
          externalsString += ' --ext=%s' % external

    return extraPackagesString, runtimeProjectString, externalsString



  def _gaudirunCommand( self ):
    """ construct a gaudirun command
    """
    command = self.opsH.getValue( '/GaudiExecution/gaudirunFlags', 'gaudirun.py' )

    # multicore?
    if self.multicore:
      cpus = multiprocessing.cpu_count()
      if cpus > 1:
        if _multicoreWN():
          command += ' --ncpus -1 '
        else:
          self.log.info( "Would have run with option '--ncpus -1', but it is not allowed here" )

    if self.commandOptions:
      command += ' '
      command += ' '.join(self.commandOptions)

    if self.prodConf:
      command += ' '
      command += self.prodConfFileName

    if self.extraOptionsLine:
      command += ' '
      with open( 'gaudi_extra_options.py', 'w' ) as fopen:
        fopen.write( self.extraOptionsLine )
      command += 'gaudi_extra_options.py'

    return command



  def _runApp( self, command, env = None ):
    """ Actual call of a command

    Args:
        env (dict): LHCb environment (from LbLogin)
    """
    print 'Command called: \n%s' % command # Really printing here as we want to see and maybe cut/paste

    return systemCall( timeout = 0,
                       cmdSeq = shlex.split(command),
                       env = env, #this may be the LbLogin env
                       callbackFunction = self.__redirectLogOutput )

  def __redirectLogOutput( self, fd, message ):
    """ Callback function for the Subprocess calls (manages log files)

    Args:
        fd (int): stdin/stderr file descriptor
        message (str): line to log
    """
    sys.stdout.flush()
    if message:
      if 'INFO Evt' in message or 'Reading Event record' in message: # These ones will appear in the std.out log too
        print message
      if self.applicationLog:
        with open( self.applicationLog, 'a' ) as log:
          log.write( message + '\n' )
          log.flush()
      else:
        log.error( "Application Log file not defined" )
      if fd == 1:
        if self.stdError:
          with open( self.stdError, 'a' ) as error:
            error.write( message + '\n' )
            error.flush()


def _multicoreWN():
  """ Returns "True" if the CE, or the Queue is marked as one where multi-processing is allowed
      (by having Tag "MultiProcessor")
  """
  siteName = gConfig.getValue( '/LocalSite/Site' )
  gridCE = gConfig.getValue( '/LocalSite/GridCE' )
  queue = gConfig.getValue( '/LocalSite/CEQueue' )
  # Tags of the CE
  tags = fromChar( gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/Tag' % ( siteName.split( '.' )[0],
                                                                             siteName, gridCE ), '' ) )
  # Tags of the Queue
  tags = fromChar( gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/Queues/%s/Tag' % ( siteName.split( '.' )[0], queue,
                                                                                       siteName, gridCE ), '' ) )

  return bool(tags and 'MultiProcessor' in tags)

def getLHCbEnvironment():
  """ Run LbLogin and returns the environment created.
      If LbLogin has run before and saved the environment (like for pilots), we use that.
  """
  pass
