""" Production environment is a utility to neatly wrap all LHCb production
    environment settings.  This includes all calls to set up the environment
    or run projects via wrapper scripts. The methods here are intended for
    use by workflow modules or client tools.
"""

__RCSID__ = "$Id$"

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

platformsConfigsDict = None


def _getPlatformsDefinitions():
  """ Just utility function
  """
  global platformsConfigsDict
  if platformsConfigsDict is None:
    result = Operations().getOptionsDict('PlatformsToConfigs')
    if not result['OK'] or not result['Value']:
      raise ValueError("PlatformsToConfigs info not found")
    configDict = dict((plat, [config.strip() for config in configList.split(',')])
                      for plat, configList in result['Value'].iteritems())
    for platform, configList in configDict.iteritems():
      fullList = [conf for config in configList for conf in configDict.get(config, [config])]
      configDict[platform] = fullList
    platformsConfigsDict = configDict
  return platformsConfigsDict


def getLHCbConfigsForPlatform(originalPlatforms):
  """ Get a list of platforms compatible with the given list
      Looks into operation section PlatformsToConfigs
  """
  if isinstance(originalPlatforms, str):
    platforms = [originalPlatforms]
  else:
    platforms = originalPlatforms
  platformsDict = _getPlatformsDefinitions()
  configsList = set(config for plat in platforms for config in platformsDict.get(plat, []))

  return list(configsList)


def _comparePlatforms(platform1, platform2):
  """ Function to be used for ordering the platforms
  Returns 0 if same configs, -1 if config1 in within config2, 1 else
  """
  platformsDict = _getPlatformsDefinitions()
  config1 = set(platformsDict[platform1])
  config2 = set(platformsDict[platform2])
  return 0 if (config1 == config2) else -1 if not (config1 - config2) else 1


def getPlatformsCompatibilities(platform1, platform2):
  """ Is platform1 compatible with platform2? (e.g. can slc5 jobs run on a slc6 machine?)
  """
  return bool(_comparePlatforms(platform1, platform2) <= 0)


def getPlatformsFromLHCbConfig(config):
  """ Returns the DIRAC platforms compatible with the given config, sorted in increasing order
  """
  platformsDict = _getPlatformsDefinitions()
  platformsList = [plat for plat in platformsDict if config in platformsDict[plat]]

  if platformsList:
    platformsList.sort(cmp=_comparePlatforms)
    return platformsList
  return None


def getPlatformFromLHCbConfig(config):
  """ Returns the minimal DIRAC platform compatible with the given config
  """
  platforms = getPlatformsFromLHCbConfig(config)
  return platforms[0] if platforms else None


def getPlatform():
  """ Determine which is the platform on the current machine
  """
  from LbPlatformUtils import dirac_platform, can_run, requires

  platformsDict = _getPlatformsDefinitions()
  architecture = dirac_platform()

  # This is the list of platforms in increasing order of capabilities
  orderedPlatforms = sorted(platformsDict, cmp=_comparePlatforms)
  preferedPlatform = None
  for platform in orderedPlatforms:
    for config in platformsDict[platform]:
      try:
        if can_run(architecture, requires(config)):
          preferedPlatform = platform
          break
      except Exception:
        pass
  return preferedPlatform
