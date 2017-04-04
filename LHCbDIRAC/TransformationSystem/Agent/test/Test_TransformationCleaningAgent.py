""" Minimal test for TransformationCleaningAgent
"""

import importlib
import unittest

from mock import MagicMock
from DIRAC import S_OK, gLogger

from LHCbDIRAC.TransformationSystem.Agent.TransformationCleaningAgent import TransformationCleaningAgent

class TransformationCleaningAgentTestCase( unittest.TestCase ):

  def setUp( self ):
    self.mockAM = MagicMock()
    self.mockAM.am_getOption.return_value = S_OK()
    self.agent = importlib.import_module( 'LHCbDIRAC.TransformationSystem.Agent.TransformationCleaningAgent' )
    self.agent.AgentModule = self.mockAM
    self.mockDTCAObj = MagicMock()
    self.mockDTCAObj.getTransformationDirectories.return_value = {'OK':True, 'Value':['/this/is/SIM/dir',
                                                                                      '/this/is/FOO/dir', '/this/is/BAR/dir']}
    self.mockDTCA = MagicMock()
    self.mockDTCA.return_value = self.mockDTCAObj
    self.agent.DiracTCAgent = self.mockDTCA
    self.agent = TransformationCleaningAgent('a', 'b')
    self.agent.log = gLogger
    self.mockSUC = MagicMock()
    self.mockSUC.getStorageDirectories.return_value = {'OK':True, 'Value':[]}
    self.agent.storageUsageClient = self.mockSUC

  def test_getTransformationDirectories(self):
    res = self.agent.getTransformationDirectories(1)
    self.assertTrue(res['OK'])


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TransformationCleaningAgentTestCase )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )