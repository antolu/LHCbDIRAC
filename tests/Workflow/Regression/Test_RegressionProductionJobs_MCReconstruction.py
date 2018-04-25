#!/usr/bin/env python

""" Regression production jobs are "real" XMLs of production jobs that ran in production
"""

# pylint: disable=missing-docstring,invalid-name,wrong-import-position

import os
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all

from tests.Utilities.IntegrationTest import IntegrationTest

from LHCbDIRAC import rootPath
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


class RegressionTestCase(IntegrationTest):
  """ Base class for the Regression test cases
  """

  def setUp(self):
    super(RegressionTestCase, self).setUp()

    self.diracLHCb = DiracLHCb()
    self.bkkClient = BookkeepingClient()
    os.chdir(os.environ['PILOTINSTALLDIR'])

#   def tearDown( self ):
#     pass


class MCReconstructionSuccess(RegressionTestCase):
  def test_Regression_Production(self):

    try:
      location40652 = find_all('40652.xml', os.environ['WORKSPACE'], '/LHCbDIRAC/tests/Workflow/Regression')[0]
    except (IndexError, KeyError):
      location40652 = find_all('40652.xml', rootPath, '/LHCbDIRAC/tests/Workflow/Regression')[0]
    j_mc_40652 = LHCbJob(location40652)
    j_mc_40652.setConfigArgs('pilot.cfg')

    res = j_mc_40652.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(RegressionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCReconstructionSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
