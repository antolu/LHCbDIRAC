########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/ProductionManagementSystem/Agent/BookkeepingWatchAgent.py $
########################################################################

__RCSID__ = "$Id: TransformationAgent.py 19398 2009-12-09 14:14:50Z acsmith $"

from DIRAC.TransformationSystem.Agent.TransformationAgent         import TransformationAgent as DIRACTransformationAgent
from DIRAC                                                        import gLogger, S_OK, S_ERROR
AGENT_NAME = 'ProductionManagement/TransformationAgent'

class TransformationAgent(DIRACTransformationAgent):

  def initialize(self):
    res = DIRACTransformationAgent.initialize(self)
    if not res['OK']:
      return res
    self.pluginLocation = 'LHCbDIRAC.TransformationSystem.Agent.TransformationPlugin'
    return S_OK()
    
