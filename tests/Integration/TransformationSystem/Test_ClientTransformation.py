""" This is a test of the chain
    TransformationClient -> TransformationManagerHandler -> TransformationDB

    It supposes that the DB is present, and that the service is running

    It extends the DIRAC one
"""
import unittest

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.tests.Integration.TransformationSystem.Test_Client_Transformation import TransformationClientChain as DIRACTransformationClientChain

from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

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
    self.assertEqual( res['Value'], {'StartRun': 1L, 'EndRun': 10L,
                                     'EventType': 12345L, 'DataTakingConditions':'DataTakingConditions'} )
    res = self.transClient.addBookkeepingQueryRunList( transID, [2, 3, 4, 5] )
    self.assert_( res['OK'] )
    res = self.transClient.getBookkeepingQuery( transID )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {'StartRun': 1L, 'EndRun': 10L, 'EventType': 12345L,
                                     'RunNumbers': ['2', '3', '4', '5'],
                                     'DataTakingConditions': 'DataTakingConditions'} )
    res = self.transClient.getTransformationsWithBkQueries( [] )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [transID] )
    res = self.transClient.getTransformationsWithBkQueries( [transID] )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [transID] )
    res = self.transClient.getTransformationsWithBkQueries( [transID - 10] )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], [] )
    res = self.transClient.setHotFlag( transID, True )
    self.assert_( res['OK'] )
    '''ideally should be {'Hot': True} but a bug in DIRAC/../mysql.py causes line 68 to fail unless
       it's {'Hot': 1}'''
    res = self.transClient.getTransformations( {'Hot': 1} )
    self.assert_( res['OK'] )

    self.assertEqual( res['Value'][0]['TransformationID'], transID )

     # test managing RunDestination table
    res = self.transClient.setDestinationForRun( 1, 'CERN-RAW' )
    self.assert_( res )
    res = self.transClient.getDestinationForRun( [1] )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], { 1: 'CERN-RAW' } )
    res = self.transClient.getDestinationForRun( [ 111, 113, 116 ] )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {} )
    # now, let's insert them...
    self.transClient.setDestinationForRun( 111, 'CERN' )
    self.transClient.setDestinationForRun( 113, 'italy' )
    self.transClient.setDestinationForRun( 116, 'germany' )
    res = self.transClient.getDestinationForRun( [ 111, 113, 116 ] )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {111: 'CERN', 113: 'italy', 116: 'germany'} )
    res = self.transClient.getDestinationForRun( [111, 360] )
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], {111:'CERN'} )

    # test managing TransformationRuns table
    res = self.transClient.insertTransformationRun( transID, 767, 'PIPPO_SE' )
    self.assert_( res['OK'] )
    res = self.transClient.insertTransformationRun( transID, 768, 'PIPPO_SE' )
    self.assert_( res['OK'] )
    res = self.transClient.getTransformationRuns()
    runs = [element['RunNumber'] for element in res ['Value']]
    self.assert_( res['OK'] )

    res = self.transClient.getTransformationRunStats( transID )
    self.assert_( res['OK'] )

    # testStoredJobDescription
    # add
    res = self.transClient.addStoredJobDescription( transID, 'jobdescription' )
    self.assert_( res['OK'] )

    # list Ids
    res = self.transClient.getStoredJobDescriptionIDs()
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'][0][0], transID )

    # testing get
    res = self.transClient.getStoredJobDescription( transID )
    self.assert_( res ['OK'] )
    self.assertEqual( res['Value'][0][0], transID )

    # testing remove
    res = self.transClient.removeStoredJobDescription( transID )
    self.assert_( res ['OK'] )

    lfn = ['/aa/lfn.1.txt', '/aa/lfn.2.txt', '/aa/lfn.3.txt']
    res = self.transClient.addTransformationRunFiles( transID, 22222, lfn )
    self.assert_( res['OK'] )

    # test setParameterToTransformationFiles
    lfnsDict = {'/aa/lfn.1.txt': {'Size': 276386386}, '/aa/lfn.2.txt': {'FileType': 'Pippo'}, '/aa/lfn.3.txt':{'RAWAncestors': 3}}
    res = self.transClient.setParameterToTransformationFiles( transID, lfnsDict )
    self.assert_( res['OK'] )

    # clean
    res = self.transClient.cleanTransformation( transID )
    self.assert_( res['OK'] )
    print res
    res = self.transClient.getTransformationParameters( transID, 'Status' )
    print res
    self.assert_( res['OK'] )
    self.assertEqual( res['Value'], 'TransformationCleaned' )

    # really delete
    res = self.transClient.deleteTransformation( transID )
    self.assert_( res['OK'] )

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientTransformationTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( LHCbTransformationClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
