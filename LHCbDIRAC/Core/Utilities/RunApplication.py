""" Utility for invoking running LHCb applications
"""

__RCSID__ = "$Id: $"

import sys
import subprocess
import multiprocessing
from distutils.version import LooseVersion

from DIRAC import gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.List import fromChar

from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getCMTConfig, getScriptsLocation, runEnvironmentScripts, addCommandDefaults


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
    self.cmtConfig = 'Any'
    self.runTimeProject = ''
    self.runTimeProjectVersion = ''

    self.prodConf = False
    self.prodConfFileName = ''
    self.step_number = 0
    self.site = ''
    self.optFile = ''
    self.extraOptionsLine = ''
    self.prodConfFileName = ''
    self.multicore = False

    # Utilities
    self.log = gLogger.getSubLogger( "RunApplication" )
    self.opsH = Operations()


  def run( self ):
    """ Invoke lb-run
    """
    # First, getting lbLogin location
    result = getScriptsLocation()
    if not result['OK']:
      return result

    lbLogin = result['Value'][self.groupLogin]

    # FIXME: need to use or not?
  #   mySiteRoot = result['Value']['MYSITEROOT']

    lbLoginEnv = runEnvironmentScripts( [lbLogin] )
    if not lbLoginEnv['OK']:
      raise RuntimeError( lbLoginEnv['Message'] )

    lbLoginEnv = lbLoginEnv['Value']

    # extra packages (for setup phase)
    extraPackagesString = ''
    for epName, epVer in self.extraPackages:
      if epVer:
        extraPackagesString = extraPackagesString + ' --use="%s %s" ' % ( epName, epVer )
      else:
        extraPackagesString = extraPackagesString + ' --use="%s"' % epName

    # run time project
    runtimeProjectString = ''
    if self.runTimeProject:
      self.log.verbose( 'Requested run time project: %s' % ( self.runTimeProject ) )
      runtimeProjectString = '--runtime-project %s %s' % ( self.runTimeProject , self.runTimeProjectVersion )

    externals = ''

    if self.opsH.getValue( 'ExternalsPolicy/%s' % ( self.site ) ):
      externals = self.opsH.getValue( 'ExternalsPolicy/%s' % ( self.site ), [] )
      externals = ' '.join( externals )
      self.log.info( 'Found externals policy for %s = %s' % ( self.site, externals ) )
    else:
      externals = self.opsH.getValue( 'ExternalsPolicy/Default', [] )
      externals = ' '.join( externals )
      self.log.info( 'Using default externals policy for %s = %s' % ( self.site, externals ) )

    # Config
    compatibleCMTConfigs = getCMTConfig( self.cmtConfig )
    if not compatibleCMTConfigs['OK']:
      return compatibleCMTConfigs
    compatibleCMTConfigs = compatibleCMTConfigs['Value']
    compatibleCMTConfigs.sort( key = LooseVersion, reverse = True )
    self.log.verbose( "Compatible ordered CMT Configs list: %s" % ','.join( compatibleCMTConfigs ) )

    if self.command == 'gaudirun.py':
      command = self.gaudirunCommand()

    # Trying all the CMT configs available
    lbRunResult = ''
    for compatibleCMTConfig in compatibleCMTConfigs:
      self.log.verbose( "Using %s for setup" % compatibleCMTConfig )
      configString = "-c %s" % compatibleCMTConfig

      finalCommandAsList = [self.runApp, self.applicationName, self.applicationVersion, extraPackagesString, configString,
                            command, runtimeProjectString, externals]
      finalCommand = ' '.join( finalCommandAsList )

      try:
        lbRunResult = self._runApp( finalCommand, compatibleCMTConfig, lbLoginEnv )
      except OSError, osE:
        self.log.warn( "Problem executing lb-run: %s" % osE )
        self.log.warn( "Can't call lb-run using %s, trying the next, if any\n\n" % compatibleCMTConfig )
        continue

    if not lbRunResult:
      raise RuntimeError( "Can't start %s %s" % ( self.applicationName, self.applicationVersion ) )

    self.log.info( "Status after the application execution is %s" % str( lbRunResult ) )



  def gaudirunCommand( self ):
    """ construct a gaudirun command
    """
    gaudiRunFlags = self.opsH.getValue( '/GaudiExecution/gaudirunFlags', 'gaudirun.py' )

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

    # multicore?
    if self.multicore:
      cpus = multiprocessing.cpu_count()
      if cpus > 1:
        if _multicoreWN():
          gaudiRunFlags = gaudiRunFlags + ' --ncpus -1 '
        else:
          self.log.info( "Would have run with option '--ncpus -1', but it is not allowed here" )


    # Set some parameter names
    dumpEnvName = 'Environment_Dump_%s_%s_Step%s.log' % ( self.applicationName,
                                                          self.applicationVersion,
                                                          self.step_number )
    scriptName = '%s_%s_Run_%s.sh' % ( self.applicationName,
                                       self.applicationVersion,
                                       self.step_number )
    coreDumpName = '%s_Step%s' % ( self.applicationName,
                                   self.step_number )

    # Wrap final execution command with defaults
    finalCommand = addCommandDefaults( command,
                                       envDump = dumpEnvName,
                                       coreDumpLog = coreDumpName )['Value']  # should always be S_OK()

    return finalCommand



  def _runApp( self, finalCommand, compatibleCMTConfig, env = None ):
    """ Actual call of a command
    """
    self.log.verbose( "Calling %s" % finalCommand )

    lbRunResult = subprocess.Popen( finalCommand, bufsize = 20971520,
                                    shell = True, env = env,
                                    stdout = subprocess.PIPE, stderr = subprocess.PIPE, close_fds = False )


    if lbRunResult.wait() != 0:
      self.log.warn( "Problem executing %s" % finalCommand )
      for line in lbRunResult.stderr:
        sys.stdout.write( line )
      raise OSError( "Can't do %s" % finalCommand )

    for line in lbRunResult.stdout:
      sys.stdout.write( line )

    return lbRunResult





def _multicoreWN():
  """ return "True" if the WN is marked as one where multicore processing is allowed
  """
  siteName = gConfig.getValue( '/LocalSite/Site' )
  gridCE = gConfig.getValue( '/LocalSite/GridCE' )
  tags = fromChar( gConfig.getValue( '/Resources/Sites/%s/%s/CEs/%s/Tag' % ( siteName.split( '.' )[0],
                                                                             siteName, gridCE ), '' ) )
  if 'MultiCore' in tags:
    return True
  else:
    return False

