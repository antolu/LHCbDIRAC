#!/usr/bin/env python

""" Regression production jobs are "real" XMLs of production jobs that ran in production
"""

#pylint: disable=missing-docstring,invalid-name,wrong-import-position

import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Utilities.utils import find_all

from tests.Utilities.IntegrationTest import IntegrationTest

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


class RegressionTestCase( IntegrationTest ):
  """ Base class for the Regression test cases
  """
  def setUp( self ):
    super( RegressionTestCase, self ).setUp()

    self.diracLHCb = DiracLHCb()
    self.bkkClient = BookkeepingClient()

#   def tearDown( self ):
#     pass

class MCSuccess( RegressionTestCase ):
  def test_Regression_Production( self ):

    location40651 = find_all( '40651.xml', '..', 'Regression' )[0]
    j_mc_40651 = LHCbJob( location40651 )
    j_mc_40651.setConfigArgs( 'pilot.cfg' )

    res = j_mc_40651.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RegressionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( MCSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
