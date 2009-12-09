########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/ProductionManagementSystem/Agent/BookkeepingWatchAgent.py $
########################################################################

__RCSID__ = "$Id: TransformationAgent.py 19398 2009-12-09 14:14:50Z acsmith $"

from DIRAC                                                        import S_OK, S_ERROR, gConfig, gLogger, gMonitor
from DIRAC.TransformationSystem.Agent.TransformationAgent         import TransformationAgent as DIRACTransformationAgent

AGENT_NAME = 'ProductionManagement/TransformationAgent'

class TransformationAgent(DIRACTransformationAgent):
  pass
