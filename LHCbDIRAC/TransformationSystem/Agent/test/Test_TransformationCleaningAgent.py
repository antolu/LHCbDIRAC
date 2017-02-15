import datetime
import copy
import importlib
import unittest

from mock import MagicMock
from DIRAC import S_OK, S_ERROR, gLogger

from LHCbDIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent

class TransformationCleaningAgentTestCase( unittest.TestCase ):

  def setUp( self ):
    self.mockAM = MagicMock()
    self.mockAM.am_getOption.return_value = S_OK()
    self.agent = importlib.import_module( 'LHCbDIRAC.TransformationSystem.Agent.TransformationCleaningAgent' )
    self.agent.AgentModule = self.mockAM
    self.agent.DIRACMCExtensionAgent = self.mockAM
    self.agent = TransformationCleaningAgent()
    self.agent.log = gLogger

  def test_getTransformationDirectories(self):
    res = self.agent.getTransformationDirectories(1)
    print res


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TransformationCleaningAgentTestCase )
  testResult = unittest.TextTestResult(verbosity = 2).run(suite)
