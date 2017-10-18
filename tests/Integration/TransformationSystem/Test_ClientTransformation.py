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
    res = self.transClient.addTransformation( 'transNameLHCb', 'description', 'longDescription', 
                                              'MCSimulation', 'Standard',
                                              'Manual', '', bkQuery = {'DataTakingConditions':'DataTakingConditions',
                                                                       'EventType': 12345} )
    self.assertTrue(res['OK'])
    transID = res['Value']
    res = self.transClient.getBookkeepingQuery( transID )
    self.assertTrue(res['OK'])
    res = self.transClient.setBookkeepingQueryStartRun( transID, 1 )
    self.assertTrue(res['OK'])
    res = self.transClient.setBookkeepingQueryEndRun( transID, 10 )
    self.assertTrue(res['OK'])
    res = self.transClient.getBookkeepingQuery( transID )
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], {'StartRun': 1L, 'EndRun': 10L,
                                     'EventType': 12345L, 'DataTakingConditions':'DataTakingConditions'} )
    res = self.transClient.addBookkeepingQueryRunList( transID, [2, 3, 4, 5] )
    self.assertTrue(res['OK'])
    res = self.transClient.getBookkeepingQuery( transID )
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], {'StartRun': 1L, 'EndRun': 10L, 'EventType': 12345L,
                                     'RunNumbers': ['2', '3', '4', '5'],
                                     'DataTakingConditions': 'DataTakingConditions'} )
    res = self.transClient.getTransformationsWithBkQueries( [] )
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], [transID] )
    res = self.transClient.getTransformationsWithBkQueries( [transID] )
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], [transID] )
    res = self.transClient.getTransformationsWithBkQueries( [transID - 10] )
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], [] )
    res = self.transClient.setHotFlag( transID, True )
    self.assertTrue(res['OK'])
    '''ideally should be {'Hot': True} but a bug in DIRAC/../mysql.py causes line 68 to fail unless
       it's {'Hot': 1}'''
    res = self.transClient.getTransformations( {'Hot': 1} )
    self.assertTrue(res['OK'])

    self.assertEqual( res['Value'][0]['TransformationID'], transID )

     # test managing RunDestination table
    res = self.transClient.setDestinationForRun( 1, 'CERN-RAW' )
    self.assertTrue( res )
    res = self.transClient.getDestinationForRun( [1] )
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], { 1: 'CERN-RAW' } )
    res = self.transClient.getDestinationForRun( [ 111, 113, 116 ] )
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], {} )
    # now, let's insert them...
    self.transClient.setDestinationForRun( 111, 'CERN' )
    self.transClient.setDestinationForRun( 113, 'italy' )
    self.transClient.setDestinationForRun( 116, 'germany' )
    res = self.transClient.getDestinationForRun( [ 111, 113, 116 ] )
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], {111: 'CERN', 113: 'italy', 116: 'germany'} )
    res = self.transClient.getDestinationForRun( [111, 360] )
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], {111:'CERN'} )

    # test managing TransformationRuns table
    res = self.transClient.insertTransformationRun( transID, 767, 'PIPPO_SE' )
    self.assertTrue(res['OK'])
    res = self.transClient.insertTransformationRun( transID, 768, 'PIPPO_SE' )
    self.assertTrue(res['OK'])
    res = self.transClient.getTransformationRuns()
    _runs = [element['RunNumber'] for element in res ['Value']]
    self.assertTrue(res['OK'])

    res = self.transClient.getTransformationRunStats( transID )
    self.assertTrue(res['OK'])

    # testStoredJobDescription
    # add
    res = self.transClient.addStoredJobDescription( transID, 'jobdescription' )
    self.assertTrue(res['OK'])

    # list Ids
    res = self.transClient.getStoredJobDescriptionIDs()
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'][0][0], transID )

    # testing get
    res = self.transClient.getStoredJobDescription( transID )
    self.assertTrue( res ['OK'] )
    self.assertEqual( res['Value'][0][0], transID )

    # testing remove
    res = self.transClient.removeStoredJobDescription( transID )
    self.assertTrue( res ['OK'] )

    lfn = ['/aa/lfn.1.txt', '/aa/lfn.2.txt', '/aa/lfn.3.txt']
    res = self.transClient.addTransformationRunFiles( transID, 22222, lfn )
    self.assertTrue(res['OK'])

    # test setParameterToTransformationFiles
    lfnsDict = {'/aa/lfn.1.txt': {'Size': 276386386},
                '/aa/lfn.2.txt': {'FileType': 'Pippo'},
                '/aa/lfn.3.txt':{'RAWAncestors': 3}}
    res = self.transClient.setParameterToTransformationFiles( transID, lfnsDict )
    self.assertTrue(res['OK'])

    # clean
    res = self.transClient.cleanTransformation( transID )
    self.assertTrue(res['OK'])
    print res
    res = self.transClient.getTransformationParameters( transID, 'Status' )
    print res
    self.assertTrue(res['OK'])
    self.assertEqual( res['Value'], 'TransformationCleaned' )

    # really delete
    res = self.transClient.deleteTransformation( transID )
    self.assertTrue(res['OK'])


  def test_mix( self ):
    """ Here because the state machine of LHCbDIRAC is different
    """

    res = self.transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard',
                                              'Manual', '' )
    transID = res['Value']

    # parameters
    res = self.transClient.setTransformationParameter( transID, 'aParamName', 'aParamValue' )
    self.assertTrue( res['OK'] )
    res1 = self.transClient.getTransformationParameters( transID, 'aParamName' )
    self.assertTrue( res1['OK'] )
    res2 = self.transClient.getTransformationParameters( transID, ( 'aParamName', ) )
    self.assertTrue( res2['OK'] )
    res3 = self.transClient.getTransformationParameters( transID, ['aParamName'] )
    self.assertTrue( res3['OK'] )
    self.assertTrue( res1['Value'] == res2['Value'] == res3['Value'] )

    # file status
    lfns = ['/aa/lfn.1.txt', '/aa/lfn.2.txt', '/aa/lfn.3.txt', '/aa/lfn.4.txt']
    res = self.transClient.addFilesToTransformation( transID, lfns )
    self.assertTrue( res['OK'] )
    res = self.transClient.getTransformationFiles( {'TransformationID':transID, 'LFN': lfns} )
    self.assertTrue( res['OK'] )
    for ft in res['Value']:
      self.assertEqual( ft['Status'], 'Unused' )
      self.assertEqual( ft['ErrorCount'], 0 )
    res = self.transClient.setFileStatusForTransformation( transID, 'Assigned', lfns )
    self.assertTrue( res['OK'] )
    res = self.transClient.getTransformationFiles( {'TransformationID':transID, 'LFN': lfns} )
    for ft in res['Value']:
      self.assertEqual( ft['Status'], 'Assigned' )
      self.assertEqual( ft['ErrorCount'], 0 )
    res = self.transClient.getTransformationStats( transID )
    self.assertTrue( res['OK'] )
    self.assertEqual( res['Value'], {'Assigned': 4L, 'Total': 4L} )
    # Setting files MaxReset from Assigned should increment ErrorCount
    res = self.transClient.setFileStatusForTransformation( transID, 'MaxReset', lfns )
    res = self.transClient.getTransformationFiles( {'TransformationID':transID, 'LFN': lfns} )
    self.assertTrue( res['OK'] )
    for ft in res['Value']:
      self.assertEqual( ft['Status'], 'MaxReset' )
      self.assertEqual( ft['ErrorCount'], 1 )
    # Cycle through Unused -> Assigned This should not increment ErrorCount
    res = self.transClient.setFileStatusForTransformation( transID, 'Unused', lfns )
    self.assertTrue( res['OK'] )
    res = self.transClient.setFileStatusForTransformation( transID, 'Assigned', lfns )
    self.assertTrue( res['OK'] )
    res = self.transClient.getTransformationFiles( {'TransformationID':transID, 'LFN': lfns} )
    self.assertTrue( res['OK'] )
    for ft in res['Value']:
      self.assertEqual( ft['Status'], 'MaxReset' ) # maxReset is final state
      self.assertEqual( ft['ErrorCount'], 1 )

    # tasks
    res = self.transClient.addTaskForTransformation( transID, lfns )
    self.assertTrue( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assertTrue( res['OK'] )
    taskIDs = []
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Created' )
      taskIDs.append( task['TaskID'] )
    self.transClient.setTaskStatus( transID, taskIDs, 'Running' )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    for task in res['Value']:
      self.assertEqual( task['ExternalStatus'], 'Running' )
    res = self.transClient.extendTransformation( transID, 5 )
    self.assertTrue( res['OK'] )
    res = self.transClient.getTransformationTasks( {'TransformationID': transID} )
    self.assertEqual( len( res['Value'] ), 6 )
    res = self.transClient.getTasksToSubmit( transID, 5 )
    self.assertTrue( res['OK'] )

    # logging
    res = self.transClient.setTransformationParameter( transID, 'Status', 'Active' )
    self.assertTrue( res['OK'] )
    res = self.transClient.getTransformationLogging( transID )
    self.assertTrue( res['OK'] )
    self.assertAlmostEqual( len( res['Value'] ), 4 )

    # delete it in the end
    self.transClient.cleanTransformation( transID )
    self.transClient.deleteTransformation( transID )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( TestClientTransformationTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( LHCbTransformationClientChain ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
