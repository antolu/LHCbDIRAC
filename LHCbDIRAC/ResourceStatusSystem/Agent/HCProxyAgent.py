"""
LHCbDIRAC/ResourceStatusSystem/Agent/HCProxyAgent.py
"""

__RCSID__ = "$Id:$"

# First, pythonic stuff
import os

# Second, DIRAC stuff
from DIRAC                                      import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                import AgentModule
from DIRAC.ResourceStatusSystem.Utilities.Utils import where

# Third, LHCbDIRAC stuff
# ...

AGENT_NAME = 'ResourceStatus/HCProxyAgent'

class HCProxyAgent( AgentModule ):
  '''
  Class HCProxyAgent. This agent is in charge of getting a fresh proxy from
  DIRAC. In this case, it sets the proxy as the one of the HammerCloudManager. 
  See CS/Operations/Shift/HammerCloudManager
  
  This is used by the HammerCloud framework, to get a new proxy every day,
  without having to use any hardcoded password ( which means, this agent will
  run on the same machine as HammerCloud ).
  
  The agent overwrites the parent methods:

  - initialize
  - execute
  '''
  
################################################################################

  def initialize(self):
    ''' 
    Method executed when the agent is launched.
    It sets the proxy as the one of the HammerCloudManager, which is stored under
      /opt/dirac/pro/work/ResourceStatus/HCProxyAgent
    '''
    
    try:

      self.am_setOption( 'shifterProxy', 'HammerCloudManager' )
      return S_OK() 

    except Exception:
      errorStr = "HCAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )

################################################################################

  def execute(self):
    """
    At every execution this method will try to print the environment variable
    X509_USER_PROXY, which should contain the path to the proxy. If is empty,
    something went wrong.
    
    This agent should get a new proxy whenever it is close to expire. 
    Appart from that, which is done internally, it is a pretty dummy loop. 
    """
    
    try:

      gLogger.info('Loop')
      gLogger.info(os.environ['X509_USER_PROXY'])
      return S_OK()
    
    except Exception, x:
      errorStr = where(self, self.execute)
      gLogger.exception(errorStr, lException = x)
      return S_ERROR(errorStr)
  
  
################################################################################

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF