########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/WorkloadManagementSystem/Agent/BKInputDataAgent.py $
# File :   StorageSummaryAgent.py
########################################################################

"""   The Storage Summary Agent will create a summary of the 
      storage usage DB grouped by processing pass or other 
      interesting parameters.
      
      Initially this will dump the information to a file but eventually
      can be inserted in a new DB table and made visible via the web portal.
"""

__RCSID__ = "$Id: StorageSummaryAgent.py 31247 2010-12-04 10:32:34Z rgracian $"

from DIRAC.Core.Base.AgentModule                                import AgentModule

from DIRAC  import gConfig, S_OK, S_ERROR

class StorageSummaryAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
    self.pollingTime = self.am_setOption('PollingTime',10)
    return S_OK()

  #############################################################################
  def execute(self):
    """The StorageSummaryAgent execution method.
    """
    self.log.info('Hello world')
    return S_OK()
  
  