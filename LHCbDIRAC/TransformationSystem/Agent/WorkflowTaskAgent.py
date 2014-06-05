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
  def __init__( self, *args, **kwargs ):
    ''' c'tor
    '''
    # FIXME: should port the clients initializations in the initialize(). Also for the basic class.
    DIRACWorkflowTaskAgent.__init__( self, *args, **kwargs )
    self.transClient = TransformationClient()
    outputDataModule = Operations().getValue( "Transformations/OutputDataModule",
                                              "LHCbDIRAC.Core.Utilities.OutputDataPolicy" )
    self.taskManager = LHCbWorkflowTasks( transClient = self.transClient,
                                          submissionClient = self.submissionClient,
                                          outputDataModule = outputDataModule,
                                          jobClass = LHCbJob )
