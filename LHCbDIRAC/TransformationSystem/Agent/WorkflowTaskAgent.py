""" :mod:  WorkflowTaskAgent
    ========================

  .. module:  WorkflowTaskAgent
  :synopsis:  Extension of the DIRAC WorkflowTaskAgent, to use LHCb clients.

"""
from DIRAC.TransformationSystem.Agent.WorkflowTaskAgent import WorkflowTaskAgent as DIRACWorkflowTaskAgent
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

from LHCbDIRAC.TransformationSystem.Client.TaskManager import LHCbWorkflowTasks
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

AGENT_NAME = 'Transformation/WorkflowTaskAgent'

class WorkflowTaskAgent( DIRACWorkflowTaskAgent ):
  ''' An AgentModule class to submit workflow tasks
  '''
  def __init__( self, agentName, loadName, baseAgentName, properties ):
    ''' c'tor
    '''
    DIRACWorkflowTaskAgent.__init__( self, agentName, loadName, baseAgentName, properties )
    self.transClient = TransformationClient()
    outputDataModule = Operations().getValue( "Transformations/OutputDataModule",
                                              "LHCbDIRAC.Core.Utilities.OutputDataPolicy" )
    self.taskManager = LHCbWorkflowTasks( transClient = self.transClient,
                                          outputDataModule = outputDataModule,
                                          jobClass = LHCbJob )
