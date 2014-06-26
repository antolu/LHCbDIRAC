""" :mod:  WorkflowTaskAgent
    ========================

  .. module:  WorkflowTaskAgent
  :synopsis:  Extension of the DIRAC WorkflowTaskAgent, to use LHCb clients.

"""

from DIRAC.TransformationSystem.Agent.WorkflowTaskAgent  import WorkflowTaskAgent as DIRACWorkflowTaskAgent

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

from LHCbDIRAC.Interfaces.API.LHCbJob                           import LHCbJob
from LHCbDIRAC.TransformationSystem.Client.TaskManager          import LHCbWorkflowTasks
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/WorkflowTaskAgent'

class WorkflowTaskAgent( DIRACWorkflowTaskAgent ):
  """ An AgentModule class to submit workflow tasks
  """
  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    DIRACWorkflowTaskAgent.__init__( self, *args, **kwargs )

  def _getClients( self ):
    """ LHCb clients
    """
    res = DIRACWorkflowTaskAgent._getClients( self )

    outputDataModule = Operations().getValue( "Transformations/OutputDataModule",
                                              "LHCbDIRAC.Core.Utilities.OutputDataPolicy" )

    threadTransformationClient = TransformationClient()
    threadTaskManager = LHCbWorkflowTasks( outputDataModule = outputDataModule,
                                           jobClass = LHCbJob )
    res.update( {'TransformationClient':threadTransformationClient,
                 'TaskManager': threadTaskManager} )
    return res

