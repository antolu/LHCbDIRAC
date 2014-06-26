""" :mod:  WorkflowTaskAgent
    ========================

  .. module:  WorkflowTaskAgent
  :synopsis:  Extension of the DIRAC WorkflowTaskAgent, to use LHCb clients.

"""

from DIRAC import S_OK
from DIRAC.TransformationSystem.Agent.WorkflowTaskAgent  import WorkflowTaskAgent as DIRACWorkflowTaskAgent

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.WorkloadManagementSystem.Client.WMSClient     import WMSClient

from LHCbDIRAC.Interfaces.API.LHCbJob                           import LHCbJob
from LHCbDIRAC.TransformationSystem.Client.TaskManager          import LHCbWorkflowTasks
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

AGENT_NAME = 'Transformation/WorkflowTaskAgent'

class WorkflowTaskAgent( DIRACWorkflowTaskAgent ):
  """ An AgentModule class to submit workflow tasks
  """
  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    DIRACWorkflowTaskAgent.__init__( self, *args, **kwargs )

  def initialize( self ):
    """ standard initialize method
    """
    res = DIRACWorkflowTaskAgent.initialize( self )
    if not res['OK']:
      return res

    self.transClient = TransformationClient()
    outputDataModule = Operations().getValue( "Transformations/OutputDataModule",
                                              "LHCbDIRAC.Core.Utilities.OutputDataPolicy" )
    self.taskManager = LHCbWorkflowTasks( transClient = self.transClient,
                                          submissionClient = WMSClient(),
                                          outputDataModule = outputDataModule,
                                          jobClass = LHCbJob )

    return S_OK()

