""" lhcb_ci.agent

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


from DIRAC                import gConfig
from DIRAC.Core.Utilities import InstallTools

# lhcb_ci
from lhcb_ci            import logger
from lhcb_ci.extensions import getCSExtensions


def getSoftwareAgents():
  """ getCodedAgents
  
  Gets the available agents inspecting the CODE.
  """

  logger.debug( 'getSoftwareAgents' )
  
  extensions = getCSExtensions()
  res = InstallTools.getSoftwareComponents( extensions )
  # Always return S_OK
  agentDict = res[ 'Value' ][ 'Agents' ]
  # The method is a bit buggy, so we have to fix it here.
  for systemName, serviceList in agentDict.items():
    agentDict[ systemName ] = list( set( serviceList ) )
  
  return agentDict  


def configureAgent( systemName, agentName ):
  """ configureDB
  
  Configures systemName/agentName in the CS
  """
  
  logger.debug( 'Configuring Agent %s/%s' % ( systemName, agentName ) )
  return InstallTools.addDefaultOptionsToCS( gConfig, 'agent', systemName, 
                                             agentName, getCSExtensions() )

#...............................................................................
#EOF