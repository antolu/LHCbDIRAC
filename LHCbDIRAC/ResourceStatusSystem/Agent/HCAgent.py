#"""
#LHCbDIRAC/ResourceStatusSystem/Agent/HCAgent.py
#"""
#
#__RCSID__ = "$Id$"
#
## First, pythonic stuff
## ...
#
## Second, DIRAC stuff
#from DIRAC                                          import S_OK, S_ERROR
#from DIRAC.Core.Base.AgentModule                    import AgentModule
#from DIRAC.Interfaces.API.DiracAdmin                import DiracAdmin
#from DIRAC.ResourceStatusSystem.Utilities.CS        import getSetup
#from DIRAC.ResourceStatusSystem.Utilities.Utils     import where
#from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import ResourceStatusDB
#
## Third, LHCbDIRAC stuff
#from LHCbDIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB
#from LHCbDIRAC.ResourceStatusSystem.Client.HCClient         import HCClient
#from LHCbDIRAC.ResourceStatusSystem.Agent.HCModes           import HCMasterMode, HCSlaveMode
#
#AGENT_NAME = 'ResourceStatus/HCAgent'
#
#class HCAgent( AgentModule ):
#  ''' 
#  Class HCAgent. This agent is in charge of submitting HammerCloud tests when
#  needed. It shows a different behavior depending on the setup, being
#  LHCb-Production the only setup where tests can actually be submitted. Other
#  setups just fake it.
# 
#  The agent submits test to a Site, or to an specific CE on a site. The conditions
#  for a Site or a CE to be eligible are:
#  
#    - `Probing` state on the RSS tables.
#    - `RS_SVC` tokenOwner on the RSS tables.
#  
#  Once the test is submitted, the Site or CE is `locked` changing the tokenOwner
#  to RS_Hold.
#  
#  The agent overwrites the parent methods:
#
#  - initialize
#  - execute
#  '''
#
#################################################################################
#
#  def initialize(self):
#    ''' 
#    Method executed when the agent is launched.
#    Initializes all modules needed to run any of the different modes of the agent.
#    Then, depending on the setup, starts a MasterMode or a SlaveMode. The first
#    one is the one used on production, the other one is only used to debug - it 
#    does not submit tests.
#    '''
#    
#    try:
#      
#      self.rsDB = ResourceStatusDB()
#      self.rmDB = ResourceManagementDB()
#      self.hc   = HCClient()
#      
#      self.diracAdmin = DiracAdmin()
#      self.setup = None
#
#      # Duration is stored on minutes, but we transform it into minutes       
#      self.TEST_DURATION     = self.am_getOption( 'testDurationMin', 120 ) / 60.  
#      self.MAX_COUNTER_ALARM = self.am_getOption( 'maxCounterAlarm', 4 )
#      self.POLLING_TIME      = self.am_getPollingTime()
#      
#      setup = getSetup()
#      if setup['OK']:
#        self.setup = setup['Value']         
#        
#      if self.setup == 'LHCb-Production':  
#        self.mode = HCMasterMode.HCMasterMode  
#      else:
#        self.mode = HCSlaveMode.HCSlaveMode  
#        
#      self.mode = self.mode( setup = self.setup, rssDB = self.rsDB, 
#                             rmDB = self.rmDB, hcClient = self.hc, 
#                             diracAdmin = self.diracAdmin, 
#                             maxCounterAlarm = self.MAX_COUNTER_ALARM, 
#                             testDuration = self.TEST_DURATION, 
#                             pollingTime = self.POLLING_TIME )  
#        
#      self.log.info( "TEST_DURATION: %s minutes"   % self.TEST_DURATION )  
#      self.log.info( "MAX_COUNTER_ALARM: %s jumps" % self.MAX_COUNTER_ALARM )
#        
#      return S_OK()
#
#    except Exception, x:
#      self.log.error( "HCAgent initialization crash" )
#      errorStr = where( self, self.initialize )
#      self.log.exception( errorStr, lException = x )
#      return S_ERROR( errorStr )
#    
#################################################################################
#              
#  def execute( self ):
#    '''
#    At every execution this method will run the selected mode, which comprises the
#    following steps:
#    
#    - updateScheduledTests     
#    - updateOngoingTests       
#    - updateLastFinishedTests  
#    - checkQuarantine          
#    - submitTests              
#    - monitorTests
#    
#    The steps may be or may be not implemented, depending on the mode. Check them
#    for details.             
#    '''
#    
#    try:
#      
#      return self.mode.run()
#                
#    except Exception, x:
#      self.log.error( "HCAgent execution crash" )
#      errorStr = where( self, self.execute )
#      self.log.exception( errorStr, lException = x )
#      return S_ERROR()
# 
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF