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

from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getCMTConfig, getScriptsLocation, runEnvironmentScripts, addCommandDefaults

__RCSID__ = "$Id$"


class RunApplication(object):
  """ Encapsulate logic for running an LHCb application
  """

  def __init__(self):
    """ c'tor - holds common variables
    """
    # Standard LHCb scripts
    self.groupLogin = 'LbLogin.sh'
    self.runApp = 'lb-run'

    # What to run
    self.applicationName = 'Gauss'
    self.applicationVersion = 'v1r0'

    # How to run it
    self.command = 'gaudirun.py'
    self.extraPackages = []
    self.systemConfig = 'Any'
    self.runTimeProject = ''
    self.runTimeProjectVersion = ''

    self.prodConf = False
    self.prodConfFileName = 'prodConf.py'
    self.step_number = 0
    self.site = ''
    self.optFile = ''
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


    # First, getting lbLogin location
    result = getScriptsLocation()
    if not result['OK']:
      return result
    lbLogin = result['Value'][self.groupLogin]

    # FIXME: need to use or not?
  #   mySiteRoot = result['Value']['MYSITEROOT']

    # Then, running lbLogin
    lbLoginEnv = runEnvironmentScripts( [lbLogin] )
    if not lbLoginEnv['OK']:
      raise RuntimeError( lbLoginEnv['Message'] )
    lbLoginEnv = lbLoginEnv['Value']

    # extra packages (for setup phase) (added using '--use')
    extraPackagesString = ''
    for epName, epVer in self.extraPackages:
      if epName.lower() == 'prodconf':
        self.prodConf = True
      if epVer:
        extraPackagesString = extraPackagesString + ' --use="%s %s" ' % ( epName, epVer )
      else:
        extraPackagesString = extraPackagesString + ' --use="%s"' % epName

    # run time project
    runtimeProjectString = ''
    if self.runTimeProject:
      self.log.verbose( 'Requested run time project: %s' % ( self.runTimeProject ) )
      runtimeProjectString = '--runtime-project %s/%s' % ( self.runTimeProject , self.runTimeProjectVersion )

    externalsString = ''

    externals = []
    if self.site:
      externals = self.opsH.getValue( 'ExternalsPolicy/%s' % ( self.site ), [] )
      if externals:
        self.log.info( 'Found externals policy for %s = %s' % ( self.site, externals ) )
        for external in externals:
          externalsString = externalsString + ' --ext=%s' % external

    if not externalsString:
      externals = self.opsH.getValue( 'ExternalsPolicy/Default', [] )
      if externals:
        self.log.info( 'Using default externals policy for %s = %s' % ( self.site, externals ) )
        for external in externals:
          externalsString = externalsString + ' --ext=%s' % external

    # "CMT" Configs
    compatibleCMTConfigs = getCMTConfig( self.systemConfig )
    if not compatibleCMTConfigs['OK']:
      return compatibleCMTConfigs
    compatibleCMTConfigs = compatibleCMTConfigs['Value']
    compatibleCMTConfigs.sort( key = LooseVersion, reverse = True )
    self.log.verbose( "Compatible ordered CMT Configs list: %s" % ','.join( compatibleCMTConfigs ) )

    if self.command == 'gaudirun.py':
      command = self.gaudirunCommand()

    # Trying all the CMT configs available
    runResult = ''
    for compatibleCMTConfig in compatibleCMTConfigs:
      self.log.verbose( "Using %s for setup" % compatibleCMTConfig )
      configString = "-c %s" % compatibleCMTConfig

      app = self.applicationName + '/' + self.applicationVersion
      finalCommandAsList = [self.runApp, configString, extraPackagesString, runtimeProjectString, externalsString, app, command]
      finalCommand = ' '.join( finalCommandAsList )

      runResult = self._runApp( finalCommand, compatibleCMTConfig, lbLoginEnv )
      if not runResult['OK']:
        self.log.warn( "Problem executing lb-run: %s" % runResult['Message'] )
        self.log.warn( "Can't call lb-run using %s, trying the next, if any\n\n" % compatibleCMTConfig )
        continue

    if not runResult['OK']:
      raise RuntimeError( "Can't start %s %s" % ( self.applicationName, self.applicationVersion ) )

    self.log.info( "Status after the application execution is %s" % str( runResult ) )



  def gaudirunCommand( self ):
    """ construct a gaudirun command
    """
    gaudiRunFlags = self.opsH.getValue( '/GaudiExecution/gaudirunFlags', 'gaudirun.py' )

    # multicore?
    if self.multicore:
      cpus = multiprocessing.cpu_count()
      if cpus > 1:
        if _multicoreWN():
          gaudiRunFlags = gaudiRunFlags + ' --ncpus -1 '
        else:
          self.log.info( "Would have run with option '--ncpus -1', but it is not allowed here" )

    # if self.optionsLine or self.jobType.lower() == 'user':
    if not self.prodConf:
      command = '%s %s %s' % ( gaudiRunFlags, self.optFile, 'gaudi_extra_options.py' )
    else:  # everything but user jobs
      if self.extraOptionsLine:
        fopen = open( 'gaudi_extra_options.py', 'w' )
        fopen.write( self.extraOptionsLine )
        fopen.close()
        command = '%s %s %s %s' % ( gaudiRunFlags, self.optFile, self.prodConfFileName, 'gaudi_extra_options.py' )
      else:
        command = '%s %s %s' % ( gaudiRunFlags, self.optFile, self.prodConfFileName )
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



  def _runApp( self, finalCommand, compatibleCMTConfig, env = None ):
    """ Actual call of a command
    """
    self.log.verbose( "Calling %s" % finalCommand )

    res = shellCall( 0, finalCommand,
                     env = env,
                     callbackFunction = self.__redirectLogOutput,
                     bufferLimit = 20971520 )

    print res

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
