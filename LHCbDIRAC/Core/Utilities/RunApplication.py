""" Utility for invoking lb-run
"""

__RCSID__ = "$Id: $"

import sys
import subprocess
from distutils.version import LooseVersion

from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getCMTConfig, getScriptsLocation, runEnvironmentScripts

groupLogin = 'LbLogin.sh'
lbRun = 'lb-run'

log = gLogger.getSubLogger( 'LbRun' )
opsH = Operations()

def lbrun( applicationName, applicationVersion,
           command = 'gaudirun.py', commandOptions = [],
           extraPackages = [], cmtConfig = 'Any',  #
           runTimeProject = '', runTimeProjectVersion = '',
           site = '' ):
  """ Invoke lb-run
  """
  # First, getting lbLogin location
  result = getScriptsLocation()
  if not result['OK']:
    return result

  lbLogin = result['Value'][groupLogin]

  # FIXME: need to use or not?
#   mySiteRoot = result['Value']['MYSITEROOT']

  lbLoginEnv = runEnvironmentScripts( [lbLogin] )
  if not lbLoginEnv['OK']:
    raise RuntimeError( lbLoginEnv['Message'] )

  lbLoginEnv = lbLoginEnv['Value']

  # extra packages (for setup phase)
  extraPackagesString = ''
  for epName, epVer in extraPackages:
    if epVer:
      extraPackagesString = extraPackagesString + ' --use="%s %s" ' % ( epName, epVer )
    else:
      extraPackagesString = extraPackagesString + ' --use="%s"' % epName

  # command option
  commandOptionsString = ' '.join( commandOptions )

  # run time project
  runtimeProjectString = ''
  if runTimeProject:
    log.verbose( 'Requested run time project: %s' % ( runTimeProject ) )
    runtimeProjectString = '--runtime-project %s %s' % ( runTimeProject , runTimeProjectVersion )

  externals = ''

  if opsH.getValue( 'ExternalsPolicy/%s' % ( site ) ):
    externals = opsH.getValue( 'ExternalsPolicy/%s' % ( site ), [] )
    externals = ' '.join( externals )
    log.info( 'Found externals policy for %s = %s' % ( site, externals ) )
  else:
    externals = opsH.getValue( 'ExternalsPolicy/Default', [] )
    externals = ' '.join( externals )
    log.info( 'Using default externals policy for %s = %s' % ( site, externals ) )

  # Config
  compatibleCMTConfigs = getCMTConfig( cmtConfig )
  if not compatibleCMTConfigs['OK']:
    return compatibleCMTConfigs
  compatibleCMTConfigs = compatibleCMTConfigs['Value']
  compatibleCMTConfigs.sort( key = LooseVersion, reverse = True )
  log.verbose( "Compatible ordered CMT Configs list: %s" % ','.join( compatibleCMTConfigs ) )

  # Trying all the CMT configs available
  lbRunResult = ''
  for compatibleCMTConfig in compatibleCMTConfigs:
    log.verbose( "Using %s for setup" % compatibleCMTConfig )
    configString = "-c %s" % compatibleCMTConfig

    finalCommandAsList = [lbRun, applicationName, applicationVersion, extraPackagesString, configString,
                          command, commandOptionsString, runtimeProjectString, externals]
    finalCommand = ' '.join( finalCommandAsList )

    try:
      lbRunResult = _runApp( finalCommand, compatibleCMTConfig, lbLoginEnv )
    except OSError, osE:
      log.warn( "Problem executing lb-run: %s" % osE )
      log.warn( "Can't call lb-run using %s, trying the next, if any\n\n" % compatibleCMTConfig )
      continue

  if not lbRunResult:
    raise RuntimeError( "Can't start %s %s" % ( applicationName, applicationVersion ) )

  log.info( "Status after the application execution is %s" % str( lbRunResult ) )


def _runApp( finalCommand, compatibleCMTConfig, env = None ):
  """ Actual call of a command
  """
  log.verbose( "Calling %s" % finalCommand )

  lbRunResult = subprocess.Popen( finalCommand, bufsize = 20971520,
                                  shell = True, env = env,
                                  stdout = subprocess.PIPE, stderr = subprocess.PIPE, close_fds = False )


  if lbRunResult.wait() != 0:
    log.warn( "Problem executing %s" % finalCommand )
    for line in lbRunResult.stderr:
      sys.stdout.write( line )
    raise OSError( "Can't do %s" % finalCommand )

  for line in lbRunResult.stdout:
    sys.stdout.write( line )

  return lbRunResult

