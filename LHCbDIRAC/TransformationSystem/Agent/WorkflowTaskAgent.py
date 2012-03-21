""" Extension of the DIRAC WorkflowTaskAgent, to use LHCb clients 
"""

from DIRAC import S_OK
from DIRAC.TransformationSystem.Agent.WorkflowTaskAgent import WorkflowTaskAgent as DIRACWorkflowTaskAgent
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase import TaskManagerAgentBase
from LHCbDIRAC.TransformationSystem.Client.TaskManager import LHCbWorkflowTasks
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

AGENT_NAME = 'Transformation/WorkflowTaskAgent'

class WorkflowTaskAgent( DIRACWorkflowTaskAgent ):
  """ An AgentModule class to submit workflow tasks, using LHCbWorklowTasks, which extends the DIRAC base class.  
  """

  #############################################################################
  def initialize( self ):
    """ Sets defaults """

    taskManager = LHCbWorkflowTasks()
    LHCbTSClient = TransformationClient()

    TaskManagerAgentBase.initialize( self, tsClient = LHCbTSClient, taskManager = taskManager )
    self.transType = self.am_getOption( "TransType", ['MCSimulation', 'DataReconstruction',
                                                      'DataReprocessing', 'DataStripping', 'Merge',
                                                      'DataSwimming', 'WGProduction'] )

    self.log.info( 'LHCb Workflow task agent: looking for  %s' % self.transType )
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    return S_OK()
