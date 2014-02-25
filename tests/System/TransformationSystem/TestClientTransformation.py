""" This is a test of the chain
    TransformationClient -> TransformationManagerHandler -> TransformationDB

    It supposes that the DB is present, and that the service is running

    It extends the DIRAC one
"""
import unittest

from TestDIRAC.System.TransformationSystem.TestClientTransformation import TransformationClientChain as DIRACTransformationClientChain

from LHCbDIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient

class TestClientTransformationTestCase( unittest.TestCase ):

  def setUp( self ):
    self.transClient = TransformationClient()

  def tearDown( self ):
    pass


class LHCbTransformationClientChain( TestClientTransformationTestCase, DIRACTransformationClientChain ):

  def test_LHCbOnly( self ):
    # add
    res = self.transClient.addTransformation( 'transNameLHCb', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '', bkQuery = {'DataTakingConditions':'DataTakingConditions',
                                                                       'EventType': 12345} )
    self.assert_( res['OK'] )
    transID = res['Value']

    res = self.transClient.getBookkeepingQuery( transID )
    self.assert_( res['OK'] )

    res = self.transClient.setBookkeepingQueryStartRun( transID, 1 )
    self.assert_( res['OK'] )
    res = self.transClient.setBookkeepingQueryEndRun( transID, 10 )
    self.assert_( res['OK'] )
    res = self.transClient.getBookkeepingQuery( transID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {'TCK': 'All', 'StartRun': 1L, 'ConfigName': 'All', 'EndRun': 10L,
                                     'EventType': 12345L, 'FileType': 'All', 'ProcessingPass': 'All',
                                     'ProductionID': 'All', 'Visible': 'All', 'DataQualityFlag': 'All',
                                     'RunNumbers': 'All', 'ConfigVersion': 'All',
                                     'DataTakingConditions': 'DataTakingConditions', 'SimulationConditions': 'All'} )

    res = self.transClient.addBookkeepingQueryRunList( transID, [2, 3, 4, 5] )
    self.assert_( res['OK'] )
    res = self.transClient.getBookkeepingQuery( transID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {'TCK': 'All', 'StartRun': 1L, 'ConfigName': 'All', 'EndRun': 10L,
                                     'EventType': 12345L, 'FileType': 'All', 'ProcessingPass': 'All',
                                     'ProductionID': 'All', 'Visible': 'All', 'DataQualityFlag': 'All',
                                     'RunNumbers': ['2', '3', '4', '5'], 'ConfigVersion': 'All',
                                     'DataTakingConditions': 'DataTakingConditions', 'SimulationConditions': 'All'} )


    # FIXME: first, I should add some...
    res = self.transClient.getTransformationRuns()
    self.assert_( res['OK'] )

    res = self.transClient.getTransformationRunStats( transID )
    self.assert_( res['OK'] )

    # clean
    res = self.transClient.cleanTransformation( transID )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationParameters( transID, 'Status' )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'TransformationCleaned' )

    # really delete
    res = self.transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientTransformationTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( LHCbTransformationClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
