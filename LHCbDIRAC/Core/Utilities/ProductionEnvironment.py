""" Production environment is a utility to neatly wrap all LHCb production
    environment settings.  This includes all calls to set up the environment
    or run projects via wrapper scripts. The methods here are intended for
    use by workflow modules or client tools.
"""

from DIRAC import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

__RCSID__ = "$Id$"

opsH = Operations()
global PlatformsConfigsDict
PlatformsConfigsDict = None

def _getPlatformsConfigsDict():
  """ Just utility function
  """
  global PlatformsConfigsDict
  if PlatformsConfigsDict is None:
    result = opsH.getOptionsDict( 'PlatformsToConfigs' )
    if not result['OK'] or not result['Value']:
      raise ValueError( "PlatformsToConfigs info not found" )
    configDict = dict( ( plat, [config.strip() for config in configList.split( ',' )] ) \
                       for plat, configList in result['Value'].iteritems() )
    for platform, configList in configDict.iteritems():
      fullList = [conf for config in configList for conf in configDict.get( config, [config] )]
      configDict[platform] = fullList
    PlatformsConfigsDict = configDict
  return PlatformsConfigsDict

def getConfigsCompatibleWithPlatforms( originalPlatforms ):
  """ Get a list of platforms compatible with the given list
      Looks into operation section PlatformsToConfigs
  """
  if isinstance( originalPlatforms, str ):
    platforms = [originalPlatforms]
  else:
    platforms = originalPlatforms
  platformsDict = _getPlatformsConfigsDict()
  configsList = set( config for plat in platforms for config in platformsDict.get( plat, [] ) )

  return S_OK( list( configsList ) )

def _comparePlatforms( platform1, platform2 ):
  """ Function to be used for ordering the platforms
  Returns 0 if same configs, -1 if config1 in within config2, 1 else
  """
  platformsDict = _getPlatformsConfigsDict()
  config1 = set( platformsDict[platform1] )
  config2 = set( platformsDict[platform2] )
  return 0 if ( config1 == config2 ) else -1 if not ( config1 - config2 ) else 1

def getPlatformsCompatibilities( platform1, platform2 ):
  """ Is platform1 compatible with platform2? (e.g. can slc5 jobs run on a slc6 machine?)
  """
  return bool( _comparePlatforms( platform1, platform2 ) <= 0 )

def getPlatformsFromConfig( config ):
  """ Returns the DIRAC platforms compatible with the given config, sorted in increasing order
  """
  platformsDict = _getPlatformsConfigsDict()
  platformsList = [plat for plat in platformsDict if config in platformsDict[plat]]

  if platformsList:
    platformsList.sort( cmp = ( lambda p1, p2: _comparePlatforms( p1, p2 ) ) )
    return platformsList
  else:
    return None

def getPlatformFromConfig( config ):
  """ Returns the minimal DIRAC platform compatible with the given config
  """
  platforms = getPlatformsFromConfig( config )
  return platforms[0] if platforms else None

def getPlatform():
  """ Determine which is the platform on the current machine
  """
  try:
    from LbPlatformUtils import dirac_platform, can_run, requires
  except ImportError:
    return S_ERROR( "Could not import LbPlatformUtils" )

  platformsDict = _getPlatformsConfigsDict()
  architecture = dirac_platform()

  # This is the list of platforms in increasing order of capabilities
  orderedPlatforms = sorted( platformsDict, cmp = ( lambda p1, p2: _comparePlatforms( p1, p2 ) ) )
  preferedPlatform = None
  for platform in orderedPlatforms:
    for config in platformsDict[platform]:
      try:
        if can_run( architecture, requires( config ) ):
          preferedPlatform = platform
          break
      except:
        pass
  if not preferedPlatform:
    return S_ERROR( "No compatible platform found" )
  return preferedPlatform


# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
