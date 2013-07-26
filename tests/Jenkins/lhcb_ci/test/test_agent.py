""" lhcb_ci.test.test_agent

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci.agent
import lhcb_ci.basecase


class ConfigureTest( lhcb_ci.basecase.Agent_TestCase ):
  """ ConfigureTest
  
  This class contains dirty & sticky tests. The configuration steps have been
  transformed into simple unittests, which are run here. Dirty & sticky because
  the tests will alter the CS structure, adding the necessary configuration 
  parameters to be able to run the rest of the tests. 
  
  Disclaimer: do not change the name of the tests, as some of them need to
  run in order.
  
  """
  
  def test_configure_agent( self ):
    """ test_configure_agent
    """
    
    self.logTestName()
    
    for systemName, agents in self.swAgents.iteritems():
            
      for agentName in agents:
      
        if self.isException( agentName ):
          continue
      
        res = lhcb_ci.agent.configureAgent( systemName, agentName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )    


  #.............................................................................
  # Nosetests attrs
  
  test_configure_agent.configure = 1
  test_configure_agent.agent     = 1  
  

class InstallationTest( lhcb_ci.basecase.Agent_TestCase ):
  """ InstallationTest
  
  Tests performing operations related with the Agents installation.
  
  """

  def test_agents_install_drop( self ):
    """ test_agents_install_drop
    
    Tests that we can install / drop directly agents using the DIRAC tools. It
    does not check whether the agents run with errors or not. It iterates over
    all agents found in self.swAgents, which are all python files *Agent.py
    
    """    
    
    self.logTestName()
            
    for system, agents in self.swAgents.iteritems():
      
      for agent in agents:
        self.log.debug( "%s %s" % ( system, agent ) )
       
        res = lhcb_ci.agent.setupAgent( system, agent )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        self.assertEquals( res[ 'Value' ][ 'RunitStatus' ], 'Run' )
        
        res = lhcb_ci.agent.uninstallAgent( system, agent )      
        self.assertDIRACEquals( res[ 'OK' ], True, res )


  def test_agents_import( self ):
    """ test_agents_import
    
    """

    self.logTestName()
    
    for diracSystem, agents in self.swAgents.iteritems():
      
      for agentName in agents:
        self.log.debug( "%s %s" % ( diracSystem, agentName ) )

        # Import DIRAC module and get object
        agentPath = 'DIRAC.%sSystem.Agent.%s' % ( diracSystem, agentName )
        self.log.debug( 'VO Importing %s' % agentPath )
        
        agentMod = lhcb_ci.extensions.import_( agentPath )
        self.assertEquals( hasattr( agentMod, agentName ), True )
        
        agentClass = getattr( agentMod, agentName )
        
        try:
          agentInstance = agentClass()
          del agentInstance
          self.fail( 'Created instance of %s should have failed' % agentPath )
        except RuntimeError, e:          
          self.assertEquals( str( e ).startswith( 'Can not connect to Agent' ), True )
    
    
    

  #.............................................................................
  # Nosetests attrs
  
  
  #test_agents_install_drop.install         = 1
  test_agents_install_drop.agent   = 1
  
  test_agents_import.install = 1
  test_agents_import.agent   = 1


#...............................................................................
#EOF