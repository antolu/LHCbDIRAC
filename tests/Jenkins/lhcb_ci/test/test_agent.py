""" lhcb_ci.test.test_agent

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci.basecase
import lhcb_ci.commons


class ConfigureTest( lhcb_ci.basecase.Agent_TestCase ):
  """ ConfigureTest
  
  This class contains dirty & sticky tests. The configuration steps have been
  transformed into simple unittests, which are run here. Dirty & sticky because
  the tests will alter the CS structure, adding the necessary configuration 
  parameters to be able to run the rest of the tests. 
  
  Disclaimer: do not change the name of the tests, as some of them need to
  run in order.
  
  """
  
  @lhcb_ci.basecase.timeDecorator
  def test_configure_agents( self ):
    """ test_configure_agents
    """
    
    self.logTestName()
    
    for systemName, agents in self.swAgents.iteritems():
            
      for agentName in agents:
      
        if self.isException( agentName ):
          continue
      
        agent = lhcb_ci.component.Component( systemName, 'Agent', agentName )
        res   = agent.configure()
        self.assertDIRACEquals( res[ 'OK' ], True, res )    


  #.............................................................................
  # Nosetests attrs
  
  test_configure_agents.configure = 1
  test_configure_agents.agent     = 1  
  

class InstallationTest( lhcb_ci.basecase.Agent_TestCase ):
  """ InstallationTest
  
  Tests performing operations related with the Agents installation.
  
  """

  @lhcb_ci.basecase.timeDecorator
  def test_agents_install_drop( self ):
    """ test_agents_install_drop
    
    Tests that we can install / drop directly agents using the DIRAC tools. It
    does not check whether the agents run with errors or not. It iterates over
    all agents found in self.swAgents, which are all python files *Agent.py
    
    """    
    
    self.logTestName()
            
    for system, agents in self.swAgents.iteritems():
      
      for agentName in agents:
        
        if self.isException( agentName ):
          continue
        
        self.log.debug( "%s %s" % ( system, agentName ) )
       
        # Repeat with me : we do not trust agents,
        #                  we do not like their structure,
        #                  we will kill their threads
        currentThreads, activeThreads = lhcb_ci.commons.trackThreads()       
       
        agent = lhcb_ci.component.Component( system, 'Agent', agentName )
        res   = agent.install()
       
        #res = lhcb_ci.agent.setupAgent( system, agentName )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        self.assertEquals( res[ 'Value' ][ 'RunitStatus' ], 'Run' )
        
        res = agent.uninstall()
        #res = lhcb_ci.agent.uninstallAgent( system, agentName )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )  

        # Clean leftovers         
        threadsAfterPurge = lhcb_ci.commons.killThreads( currentThreads )
        # We make sure that there are no leftovers on the threading
        self.assertEquals( activeThreads, threadsAfterPurge )

  @lhcb_ci.basecase.timeDecorator
  def test_agents_voimport( self ):
    """ test_agents_voimport
    
    """

    self.logTestName()
    
    for diracSystem, agents in self.swAgents.iteritems():
      
      for agent in agents:
        
        agentName = "%s/%s" % ( diracSystem, agent )
        
        if self.isException( agent ):
          continue
        
        self.log.debug( agentName )

        # Import DIRAC module and get object
        agentPath = 'DIRAC.%sSystem.Agent.%s' % ( diracSystem, agent )
        self.log.debug( 'VO Importing %s' % agentPath )
        
        # This also includes the _limbo threads..
        # Protection measure against out-of-control __init__ methods on Agents
        currentThreads, activeThreads = lhcb_ci.commons.trackThreads()
        
        agentMod = lhcb_ci.extensions.import_( agentPath )
        self.assertEquals( hasattr( agentMod, agent ), True )
        
        agentClass = getattr( agentMod, agent )
        
        agentInstance = agentClass( agentName, agentName )
        del agentInstance 

        # Clean leftovers         
        threadsAfterPurge = lhcb_ci.commons.killThreads( currentThreads )
        # We make sure that there are no leftovers on the threading
        self.assertEquals( activeThreads, threadsAfterPurge )
    

  #.............................................................................
  # Nosetests attrs
  
  
  test_agents_install_drop.install = 0
  test_agents_install_drop.agent   = 0
  
  test_agents_voimport.install = 0
  test_agents_voimport.agent   = 0


class SmokeTest( lhcb_ci.basecase.Agent_TestCase ):
  """ SmokeTest
  
  Tests performing basic common operations on the agents.
  
  """ 

  pass

#...............................................................................
#EOF