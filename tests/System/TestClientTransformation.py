""" This is a test of the chain
    TransformationClient -> TransformationManagerHandler -> TransformationDB

    It supposed that the DB is present, and that the service is running
    
    This is LHCb counterpart of the DIRAC one
"""

import unittest

from LHCbDIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient

class TestClientTransformationTestCase( unittest.TestCase ):

  def setUp( self ):
    self.transClient = TransformationClient()

  def tearDown( self ):
    pass


class TransformationClientChain( TestClientTransformationTestCase ):

  def test_runsTable( self ):
    res = self.transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    transID = res['Value']
    lfns = ['/aa/lfn.1.txt', '/aa/lfn.2.txt', '/aa/lfn.3.txt', '/aa/lfn.4.txt']
    res = self.transClient.addTransformationRunFiles( transID, 123, lfns )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationRuns( {'TransformationID':transID} )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationRunsSummaryWeb( {'TransformationID':transID}, [], 0, 50 )
    self.assert_( res['OK'] )

    # delete it in the end
    self.transClient.cleanTransformation( transID )
    self.transClient.deleteTransformation( transID )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientTransformationTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( TransformationClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
