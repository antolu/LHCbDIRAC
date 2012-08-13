#######################################################################
# $HeadURL $
# File:  WorkflowTaskAgent.py
########################################################################

""" :mod:  WorkflowTaskAgent
    ========================
 
  .. module:  WorkflowTaskAgent
  :synopsis:  Extension of the DIRAC WorkflowTaskAgent, to use LHCb clients. 

"""

__RCSID__ = "$Id $"

## imports
from DIRAC import S_OK, gConfig
from DIRAC.TransformationSystem.Agent.WorkflowTaskAgent import WorkflowTaskAgent as DIRACWorkflowTaskAgent
from DIRAC.TransformationSystem.Agent.TaskManagerAgentBase import TaskManagerAgentBase
from LHCbDIRAC.TransformationSystem.Client.TaskManager import LHCbWorkflowTasks
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob

AGENT_NAME = 'Transformation/WorkflowTaskAgent'

class WorkflowTaskAgent( DIRACWorkflowTaskAgent ):
  """ 
  .. class:: WorkflowTaskAgent
 
  An agent to submit workflow tasks, using LHCbWorklowTasks, which extends the DIRAC base class.  
 
  :param list transType: Transformation types list
  """
  transType = None

  #############################################################################
  def initialize( self ):
    """ Sets defaults """
    outputDataModule = gConfig.getValue( "/DIRAC/VOPolicy/OutputDataModule",
                                         "LHCbDIRAC.Core.Utilities.OutputDataPolicy" )
    lhcbTSClient = TransformationClient()
    taskManager = LHCbWorkflowTasks( transClient = lhcbTSClient,
                                     logger = None,
                                     submissionClient = None,
                                     jobMonitoringClient = None,
                                     outputDataModule = outputDataModule,
                                     jobClass = LHCbJob )

    TaskManagerAgentBase.initialize( self, tsClient = lhcbTSClient, taskManager = taskManager )
    self.transType = self.am_getOption( "TransType", ['MCSimulation', 'DataReconstruction',
                                                      'DataReprocessing', 'DataStripping', 'Merge',
                                                      'DataSwimming', 'WGProduction'] )
    self.log.info( 'LHCb Workflow task agent: looking for  %s' % self.transType )
    self.am_setOption( 'shifterProxy', 'ProductionManager' )
    return S_OK()
