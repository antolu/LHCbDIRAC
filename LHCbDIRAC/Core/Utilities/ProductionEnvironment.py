""" Production environment is a utility to neatly wrap all LHCb production
    environment settings.  This includes all calls to set up the environment
    or run projects via wrapper scripts. The methods here are intended for
    use by workflow modules or client tools.
"""

from distutils.version import LooseVersion #pylint: disable=import-error,no-name-in-module

from DIRAC import S_OK
from DIRAC.Core.Utilities.List import uniqueElements
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

__RCSID__ = "$Id$"

opsH = Operations()

def getPlatformsConfigsDict():
  """ Just utility function
  """
  result = opsH.getOptionsDict( 'PlatformsToConfigs' )
  if not result['OK'] or not result['Value']:
    raise ValueError( "PlatformsToConfigs info not found" )
  return dict( [( k, v.replace( ' ', '' ).split( ',' ) ) for k, v in result['Value'].iteritems()] )

def getCMTConfigsCompatibleWithPlatforms( originalPlatforms ):
  """ Get a list of platforms compatible with the given list
      Looks into operation section PlatformsToConfigs
  """
  if isinstance( originalPlatforms, str ):
    platforms = [originalPlatforms]
  else:
    platforms = originalPlatforms
  platformsDict = getPlatformsConfigsDict()
  CMTConfigsList = list()

  for plat in platforms:
    CMTConfigsList += platformsDict.get( plat, [] )

  return S_OK( list( set( CMTConfigsList ) ) )


def getPlatformFromConfig( CMTConfig ):
  """ Returns the DIRAC platform compatible with the given CMTConfig
  """
  platformsDict = getPlatformsConfigsDict()
  platformsList = list()
  for plat in platformsDict:
    if CMTConfig in platformsDict[plat]:
      platformsList.append( plat )

  platformsList.sort( key = LooseVersion )
  return uniqueElements( platformsList )

def getPlatformsCompatibilities( platform1, platform2 ):
  """ Is platform1 compatible with platform2? (e.g. can slc5 jobs run on a slc6 machine?)
  """
  platformsConfigsDict = getPlatformsConfigsDict()
  return set( platformsConfigsDict[platform1] ) <= set( platformsConfigsDict[platform2] )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
