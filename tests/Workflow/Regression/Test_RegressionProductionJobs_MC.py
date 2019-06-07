#!/usr/bin/env python
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################

""" Regression production jobs are "real" XMLs of production jobs that ran in production
"""

# pylint: disable=missing-docstring,invalid-name,wrong-import-position

import sys
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all

from tests.Utilities.IntegrationTest import IntegrationTest

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb


class RegressionTestCase(IntegrationTest):
  """ Base class for the Regression test cases
  """

  def setUp(self):
    super(RegressionTestCase, self).setUp()

    self.diracLHCb = DiracLHCb()

#   def tearDown( self ):
#     pass


class MCSuccess(RegressionTestCase):
  def test_Regression_Production(self):

    location40651 = find_all('40651.xml', '..', 'Regression')[0]
    j_mc_40651 = LHCbJob(location40651)
    j_mc_40651.setConfigArgs('pilot.cfg')

    res = j_mc_40651.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(RegressionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(MCSuccess))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
