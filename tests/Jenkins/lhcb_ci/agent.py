""" lhcb_ci.agent

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


from DIRAC                import gConfig
from DIRAC.Core.Utilities import gComponentInstaller

# lhcb_ci
from lhcb_ci            import logger
from lhcb_ci.extensions import getCSExtensions


def getSoftwareAgents():
  """ getCodedAgents
  
  Gets the available agents inspecting the CODE.
  """

  logger.debug( 'getSoftwareAgents' )
  
  extensions = getCSExtensions()
  res = gComponentInstaller.getSoftwareComponents( extensions )
  # Always return S_OK
  agentDict = res[ 'Value' ][ 'Agents' ]
  # The method is a bit buggy, so we have to fix it here.
  for systemName, serviceList in agentDict.items():
    agentDict[ systemName ] = list( set( serviceList ) )
  
  return agentDict


#def setupAgent( system, agent ):
#  """ setupAgent
#  
#  Setups agent and runs it
#  """  
#
#  logger.debug( 'setupAgent' )
#  
#  extensions = getCSExtensions()
#  
#  return gComponentInstaller.setupComponent( 'agent', system, agent, extensions )


#def uninstallAgent( system, agent ):
#  """ uninstallAgent
#  
#  Stops the agent.
#  """
#
#  logger.debug( 'uninstallAgent for %s/%s' % ( system, agent ) )
#  
#  return gComponentInstaller.uninstallComponent( system, agent )


def getInstalledAgents():
  """ getInstalledAgents
  
  Gets the available agents inspecting runit ( aka installed ).
  """

  logger.debug( 'getInstalledAgents' )
  
  res = gComponentInstaller.getInstalledComponents()
  # Always return S_OK
  return res[ 'Value' ][ 'Agents' ]


#...............................................................................
#EOF