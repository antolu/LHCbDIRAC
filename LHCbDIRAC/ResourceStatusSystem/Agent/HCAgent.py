########################################################################
# $HeadURL:  $
########################################################################

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin

from DIRAC.ResourceStatusSystem.Utilities.CS import getSetup
from DIRAC.ResourceStatusSystem.Utilities.Utils import where

from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB

from LHCbDIRAC.ResourceStatusSystem.DB.ResourceStatusAgentDB import ResourceStatusAgentDB
from LHCbDIRAC.ResourceStatusSystem.Client.HCClient import HCClient

from datetime import datetime, timedelta

from LHCbDIRAC.ResourceStatusSystem.Agent.HCModes import HCMasterMode, HCSlaveMode

import time

__RCSID__ = "$Id: $"

AGENT_NAME = 'ResourceStatus/HCAgent'

class HCAgent(AgentModule):
  """ Class HCAgent looks for sites on Probing state,
      with tokenOwner = RS_SVC and sends a test. After,
      'locks' the site with tokenOwner == RS_HOLD
  """

  def initialize(self):
    """ Standard constructor
    """
    
    try:
      self.rsDB  = ResourceStatusDB()
      self.rsaDB = ResourceStatusAgentDB()
      self.hc    = HCClient()
      
      self.diracAdmin = DiracAdmin()
      self.setup = None

      # Duration is stored on minutes, but we transform it into minutes       
      self.TEST_DURATION     = self.am_getOption( 'testDurationMin', 120 ) / 60.  
      self.MAX_COUNTER_ALARM = self.am_getOption( 'maxCounterAlarm', 4 )
      self.POLLING_TIME      = self.am_getPollingTime()
      
      setup = getSetup()
      if setup['OK']:
        self.setup = setup['Value']         
        
      if self.setup == 'LHCb-Production':  
        self.mode = HCMasterMode.HCMasterMode  
      else:
        self.mode = HCSlaveMode.HCSlaveMode  
        
      self.mode = self.mode( setup = self.setup, rssDB = self.rsDB, 
                             rsaDB = self.rsaDB, hcClient = self.hc, 
                             diracAdmin = self.diracAdmin, maxCounterAlarm = self.MAX_COUNTER_ALARM, 
                             testDuration = self.TEST_DURATION, pollingTime = self.POLLING_TIME )  
        
      gLogger.info( "TEST_DURATION: %s minutes" % self.TEST_DURATION )  
      gLogger.info( "MAX_COUNTER_ALARM: %s jumps" % self.MAX_COUNTER_ALARM )
        
      return S_OK()

    except Exception:
      errorStr = "HCAgent initialization"
      gLogger.exception( errorStr )
      return S_ERROR( errorStr )
    

#############################################################################
              
  def execute( self ):
    """
      The main HCAgent execution method.
      The agent shows a completely different behavior depending on the setup.
      If it is LHCb-Production, we submit tests. If not, it just prints some
        information. 
    """
    
    try:
      
      return self.mode.run()
                
    except Exception, x:
      errorStr = where(self, self.execute)
      gLogger.exception(errorStr, lException = x)
      return S_ERROR()
            
 
#############################################################################