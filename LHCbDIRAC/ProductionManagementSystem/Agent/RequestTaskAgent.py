########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/ProductionManagementSystem/Agent/RequestTaskAgent.py $
########################################################################
"""  This exists just to make sure we use the correct service for obtaining transformations and updating their status. """
__RCSID__ = "$Id: RequestTaskAgent.py 20001 2010-01-20 12:47:38Z acsmith $"

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gMonitor, gLogger, rootPath
from DIRAC.TransformationSystem.Agent.RequestTaskAgent              import RequestTaskAgent as DIRACRequestTaskAgent

AGENT_NAME = 'ProductionManagement/RequestTaskAgent'

class RequestTaskAgent(DIRACRequestTaskAgent):

  #############################################################################
  def initialize(self):
    """ Sets defaults """
    res =  DIRACRequestTaskAgent.initialize(self)
    self.transClient.setServer('ProductionManagement/ProductionManager')
    return res
