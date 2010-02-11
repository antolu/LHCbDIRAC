########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/ProductionManagementSystem/Agent/WorkflowTaskAgent.py $
########################################################################
"""  This exists just to take the correct location of the transformation handler service. """
__RCSID__ = "$Id: WorkflowTaskAgent.py 20001 2010-01-20 12:47:38Z acsmith $"

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.TransformationSystem.Agent.WorkflowTaskAgent             import DIRACWorkflowTaskAgent

AGENT_NAME = 'ProductionManagement/WorkflowTaskAgent'

class WorkflowTaskAgent(DIRACWorkflowTaskAgent):

  #############################################################################
  def initialize(self):
    """ Sets defaults """
    res = DIRACWorkflowTaskAgent.initialize(self)
    self.transClient.setServer('ProductionManagement/ProductionManager')
    return res
