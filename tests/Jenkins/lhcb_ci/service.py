""" lhcb_ci.utils.service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

# DIRAC
from DIRAC.Core.Utilities import InstallTools

# lhcb_ci
from lhcb_ci            import logger
from lhcb_ci.extensions import getExtensions


def getSoftwareServices():
  """ getCodedServices
  
  Gets the available services inspecting the CODE.
  """

  logger.debug( 'getSoftwareServices' )
  
  extensions = getExtensions()
  res = InstallTools.getSoftwareComponents( extensions )
  # Always return S_OK
  return res[ 'Value' ][ 'Services' ]


def getInstalledServices():
  """ getRunitServices
  
  Gets the available services inspecting runit ( aka installed ).
  """

  logger.debug( 'getInstalledServices' )
  
  res = InstallTools.getInstalledComponents()
  # Always return S_OK
  return res[ 'Value' ][ 'Services' ]


def setupService( system, component ):
  """ setupService
  
  Setups service and runs it
  """  

  logger.debug( 'setupService' )
  
  extensions = getExtensions()
  
  return InstallTools.setupComponent( 'service', system, component, extensions )


def uninstallService( system, component ):
  """ uninstallService
  
  Stops the service.
  """

  logger.debug( 'uninstallService' )
  
  return InstallTools.uninstallComponent( system, component )

  

#...............................................................................
#EOF