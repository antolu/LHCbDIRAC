########################################################################
# $HeadURL:  $
########################################################################

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

from DIRAC.ResourceStatusSystem.Utilities.Utils import where

import os

__RCSID__ = "$Id: $"

AGENT_NAME = 'ResourceStatus/HCProxyAgent'

class HCProxyAgent(AgentModule):
  
#############################################################################

  def initialize(self):
    """ Standard constructor
    """
    
    try:

      self.am_setOption( 'shifterProxy', 'HammerCloudManager' )
      return S_OK() 

    except Exception:
      errorStr = "HCAgent initialization"
      gLogger.exception(errorStr)
      return S_ERROR(errorStr)

#############################################################################

  def execute(self):
    """
      The main HCAgent execution method.
    """
    
    try:

      gLogger.info('Loop')
      gLogger.info(os.environ['X509_USER_PROXY'])
      return S_OK()
    
    except Exception, x:
      errorStr = where(self, self.execute)
      gLogger.exception(errorStr, lException = x)
      return S_ERROR(errorStr)
  
  
#############################################################################