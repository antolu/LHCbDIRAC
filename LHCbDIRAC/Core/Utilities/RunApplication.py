""" Utility for invoking running LHCb applications
"""

import sys
import re
import multiprocessing
from distutils.version import LooseVersion #pylint: disable=import-error,no-name-in-module

from DIRAC import gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.List import fromChar
from DIRAC.Core.Utilities.Subprocess import shellCall

__RCSID__ = "$Id$"


class RunApplication(object):
  """ Encapsulate logic for running an LHCb application
  """

  def __init__(self):
    """ c'tor - holds common variables
    """
    # Standard LHCb scripts
    self.runApp = 'lb-run'
    self.lhcbEnvironment = {} # This may be added (the result of LbLogin), but by default it won't be

    # What to run
    self.applicationName = ''
    self.applicationVersion = ''

    # Define the environment
    self.extraPackages = []
    self.systemConfig = 'Any'
    self.runTimeProject = ''
    self.runTimeProjectVersion = ''
    self.site = ''

    # What to run
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
    """ Invoke lb-run
    """
    self.log.info( "Executing application %s %s for CMT configuration '%s'" % ( self.applicationName,
                                                                                self.applicationVersion,
                                                                                self.systemConfig ) )


    extraPackagesString, runtimeProjectString, externalsString = self.lbRunCommandOptions()

    # "CMT" Configs
    # FIXME: this is not the way to do:
    # if a CMTConfig is provided, then only that should be called (this should be safeguarded with the following method)
    # if not, we should call --list-configs (waiting for "-c BEST") and then:
    #   - try only the first that is compatible with the current setup
    #     (what's in LocalSite/Archicture, and this should be already checked by the method below)
    if not compatibleCMTConfigs['OK']:
      return compatibleCMTConfigs
    compatibleCMTConfigs = compatibleCMTConfigs['Value']
    compatibleCMTConfigs.sort( key = LooseVersion, reverse = True )
    self.log.verbose( "Compatible ordered CMT Configs list: %s" % ','.join( compatibleCMTConfigs ) )

    if self.command == 'gaudirun.py':
      command = self.gaudirunCommand()
    else:
      command = self.command

    runResult = ''
    for compatibleCMTConfig in compatibleCMTConfigs: #FIXME: this loop should desappear
      self.log.verbose( "Using %s for setup" % compatibleCMTConfig )
      configString = "-c %s" % compatibleCMTConfig

      app = self.applicationName
      if self.applicationVersion:
        app += '/' + self.applicationVersion
      finalCommandAsList = [self.runApp, configString, extraPackagesString, runtimeProjectString, externalsString, app, command]
      finalCommand = ' '.join( finalCommandAsList )

      runResult = self._runApp( finalCommand, self.lhcbEnvironment )
      if not runResult['OK']:
        self.log.error( "Problem executing lb-run: %s" % runResult['Message'] )
        raise RuntimeError( "Can't start %s %s" % ( self.applicationName, self.applicationVersion ) )

      if runResult['Value'][0]: # if exit status != 0
        self.log.error( "lb-run or its application exited with status %d" % runResult['Value'][0] )
        raise RuntimeError( "%s %s exited with status %d" % ( self.applicationName, self.applicationVersion, runResult['Value'][0] ) )

    self.log.info( "%s execution completed successfully" % self.applicationName )

    return runResult


  def lbRunCommandOptions( self ):
    """ Return lb-run command options
    """

    # extra packages (for setup phase) (added using '--use')
    extraPackagesString = ''
    for epName, epVer in self.extraPackages:
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



  def gaudirunCommand( self ):
    """ construct a gaudirun command
    """
    command = self.opsH.getValue( '/GaudiExecution/gaudirunFlags', 'gaudirun.py' )

    # multicore?
    if self.multicore or self.multicore == 'True':
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
      fopen = open( 'gaudi_extra_options.py', 'w' )
      fopen.write( self.extraOptionsLine )
      fopen.close()
      command += 'gaudi_extra_options.py'

    # this is for avoiding env variable to be interpreted by the current shell
    command = command.replace('$', r'\$')

    self.log.always( 'Command = %s' % command )
    return command

    # FIXME: check if this is still needed
    # Set some parameter names
    # dumpEnvName = 'Environment_Dump_%s_%s_Step%s.log' % ( self.applicationName,
    #                                                       self.applicationVersion,
    #                                                       self.step_number )
    # scriptName = '%s_%s_Run_%s.sh' % ( self.applicationName,
    #                                    self.applicationVersion,
    #                                    self.step_number )
    # coreDumpName = '%s_Step%s' % ( self.applicationName,
    #                                self.step_number )
    #
    # # Wrap final execution command with defaults
    # finalCommand = addCommandDefaults( command,
    #                                    envDump = dumpEnvName,
    #                                    coreDumpLog = coreDumpName )['Value']  # should always be S_OK()
    #
    # return finalCommand



  def _runApp( self, finalCommand, env = None ):
    """ Actual call of a command
    """
    self.log.always( "Calling %s" % finalCommand )

    res = shellCall( timeout = 0,
                     cmdSeq = finalCommand,
                     env = env, #this may be the LbLogin env
                     callbackFunction = self.__redirectLogOutput )
    return res


  def __redirectLogOutput( self, fd, message ):
    """ Callback function for the Subprocess.shellcall
        Manages log files

        fd is stdin/stderr
        message is every line (?)
    """
    sys.stdout.flush()
    if message:
      if re.search( 'INFO Evt', message ):
        print message
      if re.search( 'Reading Event record', message ):
        print message
      if self.applicationLog:
        log = open( self.applicationLog, 'a' )
        log.write( message + '\n' )
        log.close()
      else:
        log.error( "Application Log file not defined" )
      if fd == 1:
        if self.stdError:
          error = open( self.stdError, 'a' )
          error.write( message + '\n' )
          error.close()


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

  if 'MultiProcessor' in tags:
    return True
  else:
    return False
