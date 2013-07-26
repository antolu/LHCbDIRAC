""" lhcb_ci.test.test_agent

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci.agent
import lhcb_ci.basecase


class Configure_Test( lhcb_ci.basecase.Agent_TestCase ):
  """ Configure_Test
  
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
  

class Installation_Test:#( lhcb_ci.basecase.DB_TestCase ):
  """ Installation_Test
  
  Tests performing operations related with the Agents installation.
  """
  pass


#...............................................................................
#EOF