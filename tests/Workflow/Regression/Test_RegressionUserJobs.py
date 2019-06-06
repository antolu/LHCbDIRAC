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

""" Testings XMLs of user jobs that ran before
"""

import os
import sys
import shutil
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()


from DIRAC.tests.Utilities.utils import find_all

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

from tests.Utilities.IntegrationTest import IntegrationTest


class RegressionTestCase(IntegrationTest):
  """ Base class for the Regression test cases
  """

  def setUp(self):
    super(RegressionTestCase, self).setUp()

    self.diracLHCb = DiracLHCb()
    self.bkkClient = BookkeepingClient()

    exeScriptLoc = find_all('exe-script.py', '..', '/LHCbDIRAC/tests/Workflow/Regression')[0]
    helloWorldLoc = find_all('helloWorld.py', '..', '/LHCbDIRAC/tests/Workflow/Regression')[0]

    shutil.copyfile(exeScriptLoc, './exe-script.py')
    shutil.copyfile(helloWorldLoc, './helloWorld.py')

    helloWorldXMLLocation = find_all('helloWorld.xml', '..', '/LHCbDIRAC/tests/Workflow/Regression')[0]
    self.j_u_hello = LHCbJob(helloWorldXMLLocation)
    self.j_u_hello.setConfigArgs('pilot.cfg')

    helloWorldXMLFewMoreLocation = find_all('helloWorld.xml', '..', '/LHCbDIRAC/tests/Workflow/Regression')[0]
    self.j_u_helloPlus = LHCbJob(helloWorldXMLFewMoreLocation)
    self.j_u_helloPlus.setConfigArgs('pilot.cfg')
#    self.j_u_collision12 = LHCbJob( 'collision12.xml' )
#    self.j_u_rootMerger = LHCbJob( 'rootMerger.xml' )

  def tearDown(self):
    os.remove('exe-script.py')
    os.remove('helloWorld.py')


class HelloWorldSuccess(RegressionTestCase):
  def test_Regression_User(self):
    res = self.j_u_hello.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])


class HelloWorldPlusSuccess(RegressionTestCase):
  def test_Regression_User(self):
    res = self.j_u_helloPlus.runLocal(self.diracLHCb)
    self.assertTrue(res['OK'])

# class Collision12Success( RegressionTestCase ):
#  def test_Regression_User( self ):
#    res = self.j_u_collision12.runLocal( self.diracLHCb, self.bkkClient )
#    self.assertTrue( res['OK'] )

# class RootMergerSuccess( RegressionTestCase ):
#  def test_Regression_User( self ):
#    res = self.j_u_rootMerger.runLocal( self.diracLHCb, self.bkkClient )
#    self.assertTrue( res['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(RegressionTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(HelloWorldSuccess))
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(HelloWorldPlusSuccess))
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Collision12Success ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RootMergerSuccess ) )
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
  sys.exit(not testResult.wasSuccessful())
