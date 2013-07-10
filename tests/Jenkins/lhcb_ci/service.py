""" lhcb_ci.utils.service

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

# DIRAC
from DIRAC.Core.Utilities import InstallTools

# lhcb_ci
from lhcb_ci import logger


def getCodedServices():
  """ getCodedServices
  
  Gets the available services inspecting the CODE.
  """

  logger.debug( 'getCodedServices' )  
  res = InstallTools.getSoftwareComponents( 'LHCb' )
  # Always return S_OK
  return { 'OK' : True, 'Value' : res[ 'Services' ] }
  

#...............................................................................
#EOF