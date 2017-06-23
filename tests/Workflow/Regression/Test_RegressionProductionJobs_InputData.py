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
    location40652 = find_all('40652.xml', '..', '/LHCbDIRAC/tests/Workflow/Regression')[0]
    j_mc_40652 = LHCbJob(location40652)
    j_mc_40652.setConfigArgs('pilot.cfg')

    res = j_mc_40652.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])


class RecoSuccess(RegressionTestCase):
  def test_Regression_Production(self):
    # Reco from Reco17
    location63284 = find_all('63284.xml', '..', '/LHCbDIRAC/tests/Workflow/Regression')[0]
    j_reco_63284 = LHCbJob(location63284)
    j_reco_63284.setConfigArgs('pilot.cfg')

    res = j_reco_63284.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])


class StrippSuccess(RegressionTestCase):
  def test_Regression_Production(self):
    # Turbo Stripping Collision15em
    location46403 = find_all('46403.xml', '..', '/LHCbDIRAC/tests/Workflow/Regression')[0]
    j_stripp_46403 = LHCbJob(location46403)
    j_stripp_46403.setConfigArgs('pilot.cfg')

    res = j_stripp_46403.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])


class MCMergeSuccess(RegressionTestCase):
  def test_Regression_Production(self):
    location51753 = find_all('51753.xml', '..', '/LHCbDIRAC/tests/Workflow/Regression')[0]
    j_MCmerge_51753 = LHCbJob(location51753)
    j_MCmerge_51753.setConfigArgs('pilot.cfg')

    res = j_MCmerge_51753.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])


class MergeMDFSuccess(RegressionTestCase):
  def test_Regression_Production(self):
    location20657 = find_all('20657.xml', '..', '/LHCbDIRAC/tests/Workflow/Regression')[0]
    j_mergeMDF_20657 = LHCbJob(location20657)
    j_mergeMDF_20657.setConfigArgs('pilot.cfg')

    res = j_mergeMDF_20657.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])


class MergeHISTOSuccess(RegressionTestCase):
  def test_Regression_Production(self):
    location66219 = find_all('66219.xml', '..', '/LHCbDIRAC/tests/Workflow/Regression')[0]
    j_mergeHISTO_66219 = LHCbJob(location66219)
    j_mergeHISTO_66219.setConfigArgs('pilot.cfg')

    res = j_mergeHISTO_66219.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(RegressionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCReconstructionSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(RecoSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(StrippSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCMergeSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MergeMDFSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MergeHISTOSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
