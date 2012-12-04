"""  TransformationAgent is and LHCb class just for overwriting some of the DIRAC methods
"""

__RCSID__ = "$Id: TransformationAgent.py 43068 2011-09-28 16:21:29Z phicharp $"


from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.Agent.TransformationAgent import TransformationAgent as DIRACTransformationAgent
from DIRAC.TransformationSystem.Agent.TransformationAgent import AGENT_NAME
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

AGENT_NAME = 'Transformation/LHCbTransformationAgent'

class TransformationAgent( DIRACTransformationAgent ):
  """ Extends base class
  """

  def __init__( self, agentName, loadName, baseAgentName = False, properties = dict() ):
    """ c'tor

    :param self: self reference
    :param str agentName: name of agent
    :param bool baseAgentName: whatever
    :param dict properties: whatever else
    """
    DIRACTransformationAgent.__init__( self, agentName, loadName, baseAgentName, properties )

    self.pluginLocation = self.am_getOption( 'PluginLocation',
                                             'LHCbDIRAC.TransformationSystem.Agent.TransformationPlugin' )
    self.workDirectory = self.am_getWorkDirectory()

    self.debug = self.am_getOption( 'verbosePlugin', False )

  def _getClients( self ):
    """ returns the clients used in the threads
    """
    res = DIRACTransformationAgent._getClients( self )

    threadTransformationClient = TransformationClient()
    threadRMClient = ResourceManagementClient()
    threadResourceStatus = ResourceStatus()
    threadBkk = BookkeepingClient()

    res.update( {'TransformationClient':threadTransformationClient,
                 'ResourceManagementClient':threadRMClient,
                 'ResourceStatus':threadResourceStatus,
                 'BookkeepingClient':threadBkk} )

    return res

  def __generatePluginObject( self, plugin, clients ):
    """ Generates the plugin object
    """
    try:
      plugModule = __import__( self.pluginLocation, globals(), locals(), ['TransformationPlugin'] )
    except Exception, x:
      gLogger.exception( "%s.__generatePluginObject: Failed to import 'TransformationPlugin'" % AGENT_NAME, '', x )
      return S_ERROR()
    try:
      oPlugin = getattr( plugModule, 'TransformationPlugin' )( '%s' % plugin,
                                                               replicaManager = clients['ReplicaManager'],
                                                               transClient = clients['TransformationClient'],
                                                               bkkClient = clients['BookkeepingClient'],
                                                               rmClient = clients['ResourceManagementClient'],
                                                               rss = clients['ResourceStatus'] )
    except Exception, x:
      gLogger.exception( "%s.__generatePluginObject: Failed to create %s()." % ( AGENT_NAME, plugin ), '', x )
      return S_ERROR()
    oPlugin.workDirectory = self.workDirectory
    oPlugin.pluginCallback = self.pluginCallback
    if self.debug:
      oPlugin.setDebug()
    return S_OK( oPlugin )
