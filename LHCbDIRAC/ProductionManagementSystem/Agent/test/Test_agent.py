import unittest
from LHCbDIRAC.ProductionManagementSystem.Agent.ProductionStatusAgent import ProductionStatusAgent

class AgentTestCase( unittest.TestCase ):
  """ Base class for the Agent test cases
  """
  def setUp( self ):

    self.psa = ProductionStatusAgent()

#############################################################################
# ProductionStatusAgent.py
#############################################################################

class ProductionStatusSuccess( AgentTestCase ):

  def test__evaluateProgress( self ):
    pass

#############################################################################
# Test Suite run 
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( AgentTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ProductionStatusSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
