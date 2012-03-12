from DIRAC import S_OK
from DIRAC.TransformationSystem.Agent.WorkflowTaskAgent import WorkflowTaskAgent as DIRACWorkflowTaskAgent
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase import TaskManagerAgentBase
from LHCbDIRAC.TransformationSystem.Client.TaskManager import LHCbWorkflowTasks

AGENT_NAME = 'Transformation/WorkflowTaskAgent'

class WorkflowTaskAgent( DIRACWorkflowTaskAgent ):
  """ An AgentModule class to submit workflow tasks, using LHCbWorklowTasks. Use this instead of the DIRAC WorkflowTasks 
  """

  #############################################################################
  def initialize( self ):
    """ Sets defaults """

    taskManager = LHCbWorkflowTasks()

    TaskManagerAgentBase.initialize( self, taskManager = taskManager )
    self.transType = self.am_getOption( "TransType", ['MCSimulation', 'DataReconstruction',
                                                      'DataReprocessing', 'DataStripping', 'Merge'] )

    self.log.info( 'LHCb Workflow task agent: looking for  %s' % self.transType )
    self.am_setOption( 'shifterProxy', 'ProductionManager' )

    return S_OK()
