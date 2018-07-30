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


class RecoSuccess(RegressionTestCase):
  def test_Regression_Production(self):
    # Reco from Reco17 #  FIXME: the input file is not there.
    try:
      location63284 = find_all('63284.xml', os.environ['WORKSPACE'], '/LHCbDIRAC/tests/Workflow/Regression')[0]
    except (IndexError, KeyError):
      location63284 = find_all('63284.xml', rootPath, '/LHCbDIRAC/tests/Workflow/Regression')[0]

    j_reco_63284 = LHCbJob(location63284)
    j_reco_63284.setConfigArgs('pilot.cfg')

    res = j_reco_63284.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])


class StrippSuccess(RegressionTestCase):
  def test_Regression_Production(self):
    # Turbo Stripping Collision15em
    try:
      location46403 = find_all('46403.xml', os.environ['WORKSPACE'], '/LHCbDIRAC/tests/Workflow/Regression')[0]
    except (IndexError, KeyError):
      location46403 = find_all('46403.xml', rootPath, '/LHCbDIRAC/tests/Workflow/Regression')[0]

    j_stripp_46403 = LHCbJob(location46403)
    j_stripp_46403.setConfigArgs('pilot.cfg')

    res = j_stripp_46403.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(RegressionTestCase)
  # suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RecoSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(StrippSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
