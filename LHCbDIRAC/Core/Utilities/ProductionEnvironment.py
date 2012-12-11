""" Production environment is a utility to neatly wrap all LHCb production
    environment settings.  This includes all calls to set up the environment
    or run projects via wrapper scripts. The methods here are intended for
    use by workflow modules or client tools.
"""

__RCSID__ = "$Id$"

import re, os, shutil
import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.Os import sourceEnv

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation  import mySiteRoot


gLogger = gLogger.getSubLogger( 'ProductionEnvironment' )
groupLogin = 'LbLogin.sh'
projectEnv = 'SetupProject.sh'
defaultCatalogName = 'pool_xml_catalog.xml'
opsH = Operations()
timeout = opsH.getValue( 'EnvironmentScripts/Default', 600 )
siteSpecificTimeout = opsH.getValue( 'EnvironmentScripts/%s' % ( DIRAC.siteName() ), 0 )
if siteSpecificTimeout:
  gLogger.info( 'Using timeout of %ss for site %s, overrides global default of %ss' % ( siteSpecificTimeout,
                                                                                        DIRAC.siteName(),
                                                                                        timeout ) )
  timeout = siteSpecificTimeout

#############################################################################
def getProjectEnvironment( systemConfiguration, applicationName, applicationVersion = '', extraPackages = '',
                           runTimeProject = '', runTimeProjectVersion = '', site = '', directory = '',
                           poolXMLCatalogName = defaultCatalogName, env = None ):
  """ This function uses all below methods to get the complete project environment
      thus ensuring consistent behaviour from all modules.  The environment is
      returned as a dictionary and can be passed directly to a shellCall as well
      as saved to a debug script.
  """
  result = getScriptsLocation()
  if not result['OK']:
    return result

  lbLogin = result['Value'][groupLogin]
  setupProjectLocation = result['Value'][projectEnv]
  mySiteRoot = result['Value']['MYSITEROOT']

  result = setDefaultEnvironment( applicationName, applicationVersion, mySiteRoot, systemConfiguration,
                                  directory, poolXMLCatalogName, env )
  if not result['OK']:
    return result

  environment = result['Value']
  result = getProjectCommand( setupProjectLocation, applicationName, applicationVersion, extraPackages,
                              site, runTimeProject, runTimeProjectVersion, '', directory ) #leave out additional options initially

  if not result['OK']:
    return result

  setupProject = result['Value']

  result = runEnvironmentScripts( [lbLogin, setupProject], environment )
  if not result['OK']:
    return result

  environment = result['Value']

  #Have to repeat this with the resulting environment since LbLogin / SetupProject overwrite any changes...
  return setDefaultEnvironment( applicationName, applicationVersion, mySiteRoot, systemConfiguration,
                                directory, poolXMLCatalogName, environment )

#############################################################################
def addCommandDefaults( command, postExecution = '', envDump = 'localEnv.log', coreDumpLog = 'Step' ):
  """ Wrap the actual execution command with some defaults that are useful for
      debugging. This is always executed by a shellCall so can use standard
      commands.
  """
  #First some preamble
  cmdList = []
  cmdSep = 'echo "%s"' % ( '=' * 50 )
  cmdList.append( cmdSep )
  cmdList.append( 'echo "Log file from execution of: %s"' % ( command ) )
  for variable in ['LD_LIBRARY_PATH', 'PYTHONPATH', 'PATH']:
    cmdList.append( cmdSep )
    cmdList.append( 'echo "%s is:"' % ( variable ) )
    cmdList.append( 'echo $%s | tr ":" "\n"' % ( variable ) )

  cmdList.append( cmdSep )
  cmdList.append( 'env | sort >> %s' % ( envDump ) )
  #Now do what is requested
  cmdList.append( command )
  cmdList.append( 'declare -x appstatus=$?' )
  #Add any requested postExecution commands
  if postExecution:
    cmdList.append( postExecution )
  #Now add some standard post execution commands
  cmdList.append( 'if [ -e core.* ] ; then  gdb python core.* >> %s_coredump.log << EOF' % ( coreDumpLog ) )
  cmdList.append( 'where' )
  cmdList.append( 'quit' )
  cmdList.append( 'EOF' )
  cmdList.append( 'fi' )
  cmdList.append( 'exit $appstatus' )
  return S_OK( ';'.join( cmdList ) )

#############################################################################
def createDebugScript( name, command, env = None, postExecution = '', envLogFile = 'localEnv.log', coreDumpLog = 'Step' ):
  """ Create a shell script for the specified commands to be executed. If no
      environment is passed it defaults to os.environ.
  """
  if not env:
    env = dict( os.environ )

  version = __RCSID__
  if os.path.exists( name ):
    os.remove( name )

  script = []
  msg = '# Dynamically generated script to reproduce execution environment.'
  script.append( '#!/bin/sh' )
  script.append( '#' * len( msg ) )
  script.append( msg )
  script.append( '#' * len( msg ) )
  script.append( '# %s' % ( version ) )
  script.append( '#' * len( msg ) + '\n' )

  for var in env:
    script.append( 'export %s="%s"' % ( var, env[var] ) )

  command = addCommandDefaults( command, postExecution, envLogFile, coreDumpLog )['Value']
  for cmd in command.split( ';' ):
    script.append( '%s' % ( cmd ) )

  fopen = open( name, 'w' )
  fopen.write( '%s\n' % ( '\n'.join( script ) ) )
  fopen.close()
  os.chmod( name, 0755 )
  return S_OK( name )

#############################################################################
def runEnvironmentScripts( commandsList, env = None ):
  """ Wrapper to run the provided commands using the specified initial environment
      (defaults to the os.environ) and return the final resulting environment as
      a dictionary.
  """
  if not env:
    env = dict( os.environ )
  else:
    env = dict( env )

  names = []
  for command in commandsList:
    gLogger.info( 'Attempting to run: %s' % ( command ) )
    name = os.path.basename( command.split( ' ' )[0] )
    names.append( name )
    #very annoying sourceEnv feature, implies .sh will be added for you so have to remove it!
    exeCommand = command.replace( groupLogin, groupLogin[:-3] ).replace( projectEnv, projectEnv[:-3] )
    exeCommand = exeCommand.split( ' ' )
    result = sourceEnv( timeout, exeCommand, env )
    if not result['OK']:
      gLogger.error( 'Problem executing %s: %s' % ( command, result['Message'] ) )
      if result['stdout']:
        gLogger.info( result['stdout'] )
      if result['stderr']:
        gLogger.error( result['stderr'] )
      return S_ERROR( '%s Execution Failed' % ( name ) )

    env = result['outputEnv']

  gLogger.info( '%s were executed successfully' % ( ', '.join( names ) ) )
  return S_OK( env )

#############################################################################
def setDefaultEnvironment( applicationName, applicationVersion, mySiteRoot, systemConfig, directory = '',
                           poolXMLCatalogName = defaultCatalogName, env = None ):
  """ Sets default environment variables for project execution, will use the
      environment passed or the current os.environ if not provided.  The
      current working directory is assumed if not provided.
  """
  if not env:
    env = dict( os.environ )
  if not directory:
    directory = os.getcwd()

  if not poolXMLCatalogName == defaultCatalogName:
    if not os.path.exists( poolXMLCatalogName ):
      if os.path.exists( defaultCatalogName ):
        gLogger.info( 'Creating requested POOL XML Catalog file: %s' % ( poolXMLCatalogName ) )
        shutil.copy( os.path.join( directory, defaultCatalogName ), os.path.join( directory, poolXMLCatalogName ) )
      else:
        gLogger.error( 'Could not find default catalog %s in directory %s' % ( defaultCatalogName, directory ) )
        return S_ERROR( 'Could not find %s' % defaultCatalogName )

  if 'CMTPROJECTPATH' in env:
    gLogger.verbose( 'Removing CMTPROJECTPATH from environment; %s' % env['CMTPROJECTPATH'] )
    del env['CMTPROJECTPATH']

  if not os.path.exists( directory ):
    return S_ERROR( 'Working directory %s does not exist!' % ( directory ) )

  if os.path.exists( os.path.join( directory, 'lib' ) ):
    gLogger.info( 'Found local lib/ directory, prepending to LD_LIBRARY_PATH' )
    env['LD_LIBRARY_PATH'] = '%s:%s' % ( os.path.join( directory, 'lib' ), env['LD_LIBRARY_PATH'] )

  if os.path.exists( os.path.join( directory, 'python' ) ):
    gLogger.info( 'Found local python/ directory, prepending to PYTHONPATH' )
    env['PYTHONPATH'] = '%s:%s' % ( os.path.join( directory, 'python' ), env['PYTHONPATH'] )

  if os.path.exists( os.path.join( directory, 'InstallArea', 'python.zip' ) ):
    gLogger.info( 'Found local InstallArea/python.zip, prepending to PYTHONPATH' )
    env['PYTHONPATH'] = '%s:%s' % ( os.path.join( directory, 'InstallArea', 'python.zip' ), env['PYTHONPATH'] )

  if os.path.exists( os.path.join( directory, 'InstallArea', 'python' ) ):
    gLogger.info( 'Found local InstallArea/python directory, prepending to PYTHONPATH' )
    env['PYTHONPATH'] = '%s:%s' % ( os.path.join( directory, 'InstallArea', 'python' ), env['PYTHONPATH'] )

  gLogger.info( 'Setting MYSITEROOT to %s' % ( mySiteRoot ) )
  env['MYSITEROOT'] = mySiteRoot

  if systemConfig.lower() == 'any':
    systemConfig = gConfig.getValue( '/LocalSite/Architecture', '' )
    if not systemConfig:
      gLogger.error( '/LocalSite/Architecture is not defined' )
      return S_ERROR( 'SystemConfig Not Found' )
    compatibleArchs = gConfig.getValue( '/Resources/Computing/OSCompatibility/%s' % ( systemConfig ), [] )
    if not compatibleArchs:
      gLogger.error( 'Could not find matching section for %s in /Resources/Computing/OSCompatibility/' % ( systemConfig ) )
      return S_ERROR( 'SystemConfig Not Found' )
    systemConfig = compatibleArchs[0]
    gLogger.verbose( 'Setting system config to compatible platform %s since it was set to "ANY" in the job description' % ( systemConfig ) )

  gLogger.info( 'Setting CMTCONFIG to %s' % ( systemConfig ) )
  env['CMTCONFIG'] = systemConfig

  #miscellaneous vars
  env['CSEC_TRACE'] = '1'
  env['CSEC_TRACEFILE'] = "csec.log"

  #in case of CMT requirements (this is not nice)
  if os.path.exists( os.path.join( directory, 'lib', 'requirements' ) ) and applicationVersion:
    gLogger.info( 'Setting environment variables for fake CMT package' )
    env['User_release_area'] = directory
    package = os.path.join( directory, '%s_%s' % ( applicationName, applicationVersion ) )
    env['CMTPATH'] = package
    if not os.path.exists( package ):
      os.mkdir( package )
    if not os.path.exists( os.path.join( package, 'cmttemp' ) ):
      os.mkdir( os.path.join( directory, 'cmttemp' ) )
    if not os.path.exists( os.path.join( package, 'cmttemp', 'v1' ) ):
      os.mkdir( os.path.join( directory, 'cmttemp', 'v1' ) )
    if not os.path.exists( os.path.join( package, 'cmttemp', 'v1', 'cmt' ) ):
      os.mkdir( os.path.join( directory, 'cmttemp', 'v1', 'cmt' ) )
    if not os.path.exists( os.path.join( package, 'cmt' ) ):
      os.mkdir( os.path.join( package, 'cmt' ) )

    if os.path.exists( os.path.join( package, 'cmt', 'project.cmt' ) ):
      os.remove( os.path.join( package, 'cmt', 'project.cmt' ) )

    fopen = open( os.path.join( package, 'cmt', 'project.cmt' ), 'w' )
    fopen.write( 'use %s %s_%s' % ( applicationName.upper(), applicationName.upper(), applicationVersion.upper() ) )
    fopen.close()
    shutil.copy( os.path.join( directory, 'lib', 'requirements' ), os.path.join( package, 'cmttemp', 'v1', 'cmt' ) )

  return S_OK( env )

#############################################################################
def getProjectCommand( location, applicationName, applicationVersion, extraPackages = [], site = '',
                       runTimeProject = '', runTimeProjectVersion = '', additional = '', directory = '' ):
  """ Returns (without executing) the SetupProject command line string and requires
      the following arguments:

      - location - full path to SetupProject.sh e.g. output of getScriptsLocation()
      - applicationName
      - applicationVersion

      optionally with the following additional arguments:

      - extraPackages - i.e. a list of [<name>.<version>] strings which are expressed as --use "<name> version>"
      - site - will default to the current site name but can be specified, governs the externals policy
      - runTimeProject - i.e. --runtime-project <runTimeProject>
      - additional - add additional arbitrary options.

      The directory parameter is used to check whether any shipped requirements are
      present that require an additional tag (conventionally this is --use="cmttemp v1").
  """
  cmd = [location]
  cmd.append( '--debug' )

  if extraPackages:
    if not type( extraPackages ) == type( [] ) and extraPackages:
      extraPackages = [extraPackages]

    gLogger.verbose( 'Requested extra package versions: %s' % ( ', '.join( extraPackages ) ) )
    for package in extraPackages:
#      if not re.search( '.', package ):
##        gLogger.error( 'Not sure what to do with "%s", expected "<Application>.<Version>", will be left out' )
#        gLogger.warn( 'Package %s: expected "<Application>.<Version>", will try anyway' % package )
#        cmd.append( '--use="%s" ' % package )
#      else:
      try:
        cmd.append( '--use="%s %s" ' % ( package.split( '.' )[0], package.split( '.' )[1] ) )
      except IndexError:
        cmd.append( '--use="%s" ' % package )

  if os.path.exists( os.path.join( directory, 'lib', 'requirements' ) ):
    gLogger.info( 'Adding tag for user shipped requirements file --use="cmttemp v1"' )
    cmd.append( '--use="cmttemp v1"' )

  cmd.append( applicationName )
  if applicationVersion:
    cmd.append( applicationVersion )

  if runTimeProject:
    gLogger.verbose( 'Requested run time project: %s' % ( runTimeProject ) )
    cmd.append( '--runtime-project %s %s' % ( runTimeProject , runTimeProjectVersion ) )

  if not site:
    site = DIRAC.siteName()

  externals = ''

  if opsH.getValue( 'ExternalsPolicy/%s' % ( site ) ) != '':
    externals = opsH.getValue( 'ExternalsPolicy/%s' % ( site ), [] )
    externals = ' '.join( externals )
    gLogger.info( 'Found externals policy for %s = %s' % ( site, externals ) )
  else:
    externals = opsH.getValue( 'ExternalsPolicy/Default', [] )
    externals = ' '.join( externals )
    gLogger.info( 'Using default externals policy for %s = %s' % ( site, externals ) )

  cmd.append( externals )

  if additional:
    gLogger.info( 'Requested additional options: %s' % ( additional ) )
    cmd.append( additional )

  finalCommand = ' '.join( cmd )
  gLogger.verbose( '%s command = %s' % ( projectEnv, finalCommand ) )
  return S_OK( finalCommand )

#############################################################################
def getScriptsLocation():
  """ This function determines the location of LbLogin / SetupProject based on the
      local site configuration.  The order of preference for the local software
      location is:
        - LocalSite/LocalArea - only if software is installed locally
        - $VO_LHCB_SW_DIR/lib - typically defined for Grid jobs
        - LocalSite/SharedArea - for locally running jobs
      If LbLogin / SetupProject are not found in the above locations this function
      returns an error. Otherwise the location of the environment scripts is returned
      in a dictionary with the name as the key.
  """
  softwareArea = mySiteRoot()
  if softwareArea == '':
    gLogger.error( 'Could not determine MYSITEROOT: "%s"' % softwareArea )
    return S_ERROR( 'MYSITEROOT Not Found' )

  gLogger.info( 'MYSITEROOT = %s' % softwareArea )
  localArea = ''
  if re.search( ':', softwareArea ):
    jobAgentSoftware = softwareArea.split( ':' )[0]
    if os.path.exists( os.path.join( jobAgentSoftware, groupLogin ) ):
      localArea = jobAgentSoftware
      gLogger.info( 'Will use %s from local software area at %s' % ( groupLogin, localArea ) )
    else:
      localArea = softwareArea.split( ':' )[1]
      if os.path.exists( os.path.join( localArea, groupLogin ) ):
        gLogger.info( 'Using %s from the site shared area directory at %s' % ( groupLogin, localArea ) )
      else:
        gLogger.error( '%s not found in local area or shared area %s' % ( groupLogin, softwareArea ) )
        return S_ERROR( '%s not found in local or shared areas' % ( groupLogin ) )
  else:
    if os.path.exists( os.path.join( softwareArea, groupLogin ) ):
      localArea = softwareArea
      gLogger.info( 'Using %s from MYSITEROOT = %s' % ( groupLogin, localArea ) )
    else:
      gLogger.error( '%s not found in MYSITEROOT directory %s' % ( groupLogin, localArea ) )
      return S_ERROR( '%s not found in MYSITEROOT' % ( groupLogin ) )

  gLogger.verbose( 'Using scripts from software area at %s' % localArea )
  groupLoginPath = os.path.join( localArea, groupLogin )
  projectScriptPath = os.path.join( os.path.dirname( os.path.realpath( os.path.join( localArea, groupLogin ) ) ), projectEnv )

  if not os.path.exists( projectScriptPath ):
    gLogger.error( '%s was found at %s but %s was not found at the expected relative path %s' % ( groupLogin, localArea,
                                                                                                  projectEnv, projectScriptPath ) )
    return S_ERROR( '%s was not found in software area' % ( projectEnv ) )

  gLogger.info( '%s = %s' % ( groupLogin, groupLoginPath ) )
  gLogger.info( '%s = %s' % ( projectEnv, projectScriptPath ) )
  result = {groupLogin:groupLoginPath, projectEnv:projectScriptPath, 'MYSITEROOT':softwareArea}
  return S_OK( result )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
