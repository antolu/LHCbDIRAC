#!/usr/bin/env python
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

import unittest, os, shutil

from TestDIRAC.Utilities.utils import find_all

from LHCbTestDirac.Utilities.IntegrationTest import IntegrationTest

from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient


class RegressionTestCase( IntegrationTest ):
  """ Base class for the Regression test cases
  """
  def setUp( self ):
    super( IntegrationTest, self ).setUp()

    self.diracLHCb = DiracLHCb()
    self.bkkClient = BookkeepingClient()

    exeScriptLoc = find_all( 'exe-script.py', '.', 'Regression' )[0]
    shutil.copyfile( exeScriptLoc, './exe-script.py' )
    helloWorldLoc = find_all( 'helloWorld.py', '.', 'Regression' )[0]
    shutil.copyfile( helloWorldLoc, './helloWorld.py' )

    helloWorldXMLLocation = find_all( 'helloWorld.xml', '.', 'Regression' )[0]
    self.j_u_hello = LHCbJob( helloWorldXMLLocation )
    helloWorldXMLFewMoreLocation = find_all( 'helloWorld.xml', '.', 'Regression' )[0]
    self.j_u_helloPlus = LHCbJob( helloWorldXMLFewMoreLocation )
#    self.j_u_collision12 = LHCbJob( 'collision12.xml' )
#    self.j_u_rootMerger = LHCbJob( 'rootMerger.xml' )

  def tearDown( self ):
    os.remove( 'exe-script.py' )
    os.remove( 'helloWorld.py' )

class HelloWorldSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_u_hello.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

class HelloWorldPlusSuccess( RegressionTestCase ):
  def test_execute( self ):
    res = self.j_u_helloPlus.runLocal( self.diracLHCb, self.bkkClient )
    self.assertTrue( res['OK'] )

# class Collision12Success( RegressionTestCase ):
#  def test_execute( self ):
#    res = self.j_u_collision12.runLocal( self.diracLHCb, self.bkkClient )
#    self.assertTrue( res['OK'] )

# class RootMergerSuccess( RegressionTestCase ):
#  def test_execute( self ):
#    res = self.j_u_rootMerger.runLocal( self.diracLHCb, self.bkkClient )
#    self.assertTrue( res['OK'] )


#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( RegressionTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( HelloWorldPlusSuccess ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Collision12Success ) )
#  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RootMergerSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
