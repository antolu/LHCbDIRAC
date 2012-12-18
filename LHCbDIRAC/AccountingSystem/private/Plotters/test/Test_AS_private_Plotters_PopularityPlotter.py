'''
  Unittest for:
    LHCbDIRAC.AccountingSystem.private.Plotters.PopularityPlotter
    
  PopularityPlotter.__bases__:
    DIRAC.AccountingSystem.private.Plotters.BaseReporter  
  
  We are assuming there is a solid test of __bases__, we are not testing them
  here and assuming they work fine.
  
  IMPORTANT: the test MUST be pylint compliant !  
'''

import mock
import unittest

from decimal import Decimal

import math
import operator
import Image
def compare( file1Path, file2Path ):
  '''
    Function used to compare two plots
    
    returns 0.0 if both are identical
  '''
  #Crops image to remote the "Generated on xxxx UTC" string
  image1 = Image.open( file1Path ).crop( ( 0, 0, 800, 570 ) )
  image2 = Image.open( file2Path ).crop( ( 0, 0, 800, 570 ) )
  h1 = image1.histogram()
  h2 = image2.histogram()
  rms = math.sqrt( reduce( operator.add,
                           map(lambda a,b: (a-b)**2, h1, h2))/len(h1) )
  return rms

#...............................................................................

class PopularityPlotterTestCase( unittest.TestCase ):
  '''
    PopularityPlotterTestCase
  '''

  moduleTested = None
  classsTested = None
  
  def mockModuleTested( self, moduleTested ):
    '''
      Used to not redo the mocking done on the parent class ( if any )
    '''
    
    # Tries to get the mocks of the parent TestCases ( if any )
    for baseClass in self.__class__.__bases__:
      try:
        #pylint: disable=E1101
        moduleTested = baseClass.mockModuleTested( moduleTested )
      except TypeError:
        continue  
    
    # And then makes its own mock
    class MockPopularity:
      #pylint: disable=C0111,R0903,W0232
      definitionKeyFields = ( 'DataType', 'Activity', 'FileType', 'Production',
                              'ProcessingPass', 'Conditions', 'EventType', 'StorageElement' )
        
    moduleTested.Popularity = mock.Mock( return_value = MockPopularity() )
    
    return moduleTested
    
  def setUp( self ):
    '''
      Setup the test case
    '''
    
    import LHCbDIRAC.AccountingSystem.private.Plotters.PopularityPlotter as moduleTested
    
    self.moduleTested = self.mockModuleTested( moduleTested )
    self.classsTested = self.moduleTested.PopularityPlotter
    
  def tearDown( self ):
    '''
      Tear down the test case
    '''
    
    del self.moduleTested
    del self.classsTested  
  
#...............................................................................  

class PopularityPlotterUnitTest( PopularityPlotterTestCase ):
  '''
    PopularityPlotterUnitTest
    <constructor>
     - test_instantiate
    <class variables>
     - test_typeName
     - test_typeKeyFields
    <methods>
     - test_reportDataUsage
     - test_reportNormalizedDataUsage
     - test_plotDataUsage
     - test_plotNormalizedDataUsage
  '''

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''     
    obj = self.classsTested( None, None )
    self.assertEqual( 'PopularityPlotter', obj.__class__.__name__ )

  def test_typeName( self ):
    ''' test the class variable "_typeName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._typeName, "Popularity" )

  def test_typeKeyFields( self ):
    ''' test the class variable "_typeKeyFields" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._typeKeyFields, [ 'DataType', 'Activity', 'FileType', 
                                            'Production', 'ProcessingPass', 
                                            'Conditions', 'EventType', 'StorageElement'
                                          ] )

  def test_reportDataUsage( self ):
    ''' test the method "_reportDataUsage"
    '''
    
    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    res = obj._reportDataUsage( { 'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                  'startTime'      : 'startTime',
                                  'endTime'        : 'endTime',
                                  'condDict'       : {} 
                                 } )
    self.assertEqual( res[ 'OK' ], False )
    self.assertEqual( res[ 'Message' ], 'No connection' )
    
    #Changed mocked to run over different lines of code
    mockAccountingDB._getConnection.return_value               = { 'OK' : True, 'Value' : [] }
    res = obj._reportDataUsage( { 'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                  'startTime'      : 'startTime',
                                  'endTime'        : 'endTime',
                                  'condDict'       : {} 
                                 } )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'graphDataDict': {}, 
                                        'data'         : {}, 
                                        'unit'         : 'files', 
                                        'granularity'  : 'BucketLength'
                                       })
    
    mockedData = ( ( '90000000', 1355616000L, 86400, Decimal( '123456.789' ) ), 
                   ( '90000000', 1355702400L, 86400, Decimal( '78901.2345' ) ) ) 
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : mockedData }
    mockAccountingDB.calculateBucketLengthForTime.return_value = 86400
    
    res = obj._reportDataUsage( { 'groupingFields' : ( '%s', [ 'EventType' ] ),
                                  'startTime'      : 1355663249.0,
                                  'endTime'        : 1355749690.0,
                                  'condDict'       : { 'EventType' : '90000000' } 
                                 } )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'graphDataDict' : { '90000000' : { 1355616000L : 123.456789, 
                                                                           1355702400L : 78.901234500000001 }
                                                           }, 
                                        'data'          : { '90000000' : { 1355616000L : 123456.789, 
                                                                           1355702400L : 78901.234500000006 } 
                                                           }, 
                                        'unit'          : 'kfiles', 
                                        'granularity'   : 86400
                                       } )

  def test_reportNormalizedDataUsage( self ):
    ''' test the method "_reportNormalizedDataUsage"
    '''
    
    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    res = obj._reportNormalizedDataUsage( { 'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                            'startTime'      : 'startTime',
                                            'endTime'        : 'endTime',
                                            'condDict'       : {} 
                                           } )
    self.assertEqual( res[ 'OK' ], False )
    self.assertEqual( res[ 'Message' ], 'No connection' )
    
    #Changed mocked to run over different lines of code
    mockAccountingDB._getConnection.return_value               = { 'OK' : True, 'Value' : [] }
    res = obj._reportNormalizedDataUsage( { 'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                            'startTime'      : 'startTime',
                                            'endTime'        : 'endTime',
                                            'condDict'       : {} 
                                           } )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'graphDataDict': {}, 
                                        'data'         : {}, 
                                        'unit'         : 'files', 
                                        'granularity'  : 'BucketLength'
                                       })
    
    mockedData = ( ( '90000000', 1355616000L, 86400, Decimal( '123456.789' ) ), 
                   ( '90000000', 1355702400L, 86400, Decimal( '78901.2345' ) ),
                   ( '90000001', 1355616000L, 86400, Decimal( '223456.789' ) ), 
                   ( '90000001', 1355702400L, 86400, Decimal( '148901.2345' ) ) ) 
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : mockedData }
    mockAccountingDB.calculateBucketLengthForTime.return_value = 86400
    
    res = obj._reportNormalizedDataUsage( { 'groupingFields' : ( '%s', [ 'EventType' ] ),
                                            'startTime'      : 1355663249.0,
                                            'endTime'        : 1355749690.0,
                                            'condDict'       : { 'StorageElement' : 'CERN' } 
                                           } )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'graphDataDict' : { '90000001' : { 1355616000L : 223.45678899999999, 
                                                                           1355702400L : 148.90123449999999 }, 
                                                            '90000000' : { 1355616000L : 123.456789, 
                                                                           1355702400L : 78.901234500000001 }
                                                           }, 
                                        'data'          : { '90000001' : { 1355616000L : 223456.78899999999, 
                                                                           1355702400L : 148901.23449999999 }, 
                                                            '90000000' : { 1355616000L : 123456.789, 
                                                                           1355702400L : 78901.234500000006 } 
                                                           }, 
                                        'unit'          : 'kfiles', 
                                        'granularity': 86400 
                                       } )

  def test_plotDataUsage( self ):
    ''' test the method "_plotDataUsage"
    '''    
    
    obj = self.classsTested( None, None )
    
    reportRequest = { 'grouping'       : 'EventType',
                      'groupingFields' : ( '%s', [ 'EventType' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'StorageElement' : 'CERN' } 
                    } 
    plotInfo = { 'graphDataDict' : { '90000001' : { 1355616000L : 223.45678899999999, 
                                                    1355702400L : 148.90123449999999 }, 
                                     '90000000' : { 1355616000L : 123.456789, 
                                                    1355702400L : 78.901234500000001 }
                                    }, 
                 'data'          : { '90000001' : { 1355616000L : 223456.78899999999, 
                                                    1355702400L : 148901.23449999999 }, 
                                     '90000000' : { 1355616000L : 123456.789, 
                                                    1355702400L : 78901.234500000006 } 
                                    }, 
                 'unit'          : 'kfiles', 
                 'granularity': 86400 
                }
    res = obj._plotDataUsage( reportRequest, plotInfo, 'PopularityPlotter_plotDataUsage' )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )
    
    res = compare( 'PopularityPlotter_plotDataUsage.png', 
                   'LHCbDIRAC/AccountingSystem/private/Plotters/test/png/PopularityPlotter_plotDataUsage.png' )
    self.assertEquals( 0.0, res )   

  def test_plotNormalizedDataUsage( self ):
    ''' test the method "_plotNormalizedDataUsage"
    '''    
    
    obj = self.classsTested( None, None )
    
    reportRequest = { 'grouping'       : 'EventType',
                      'groupingFields' : ( '%s', [ 'EventType' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'StorageElement' : 'CERN' } 
                    } 
    plotInfo = { 'graphDataDict' : { '90000001' : { 1355616000L : 223.45678899999999, 
                                                    1355702400L : 148.90123449999999 }, 
                                     '90000000' : { 1355616000L : 123.456789, 
                                                    1355702400L : 78.901234500000001 }
                                    }, 
                 'data'          : { '90000001' : { 1355616000L : 223456.78899999999, 
                                                    1355702400L : 148901.23449999999 }, 
                                     '90000000' : { 1355616000L : 123456.789, 
                                                    1355702400L : 78901.234500000006 } 
                                    }, 
                 'unit'          : 'kfiles', 
                 'granularity': 86400 
                }
    res = obj._plotNormalizedDataUsage( reportRequest, plotInfo, 'PopularityPlotter_plotNormalizedDataUsage' )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )
    
    res = compare( 'PopularityPlotter_plotNormalizedDataUsage.png', 
                   'LHCbDIRAC/AccountingSystem/private/Plotters/test/png/PopularityPlotter_plotNormalizedDataUsage.png' )
    self.assertEquals( 0.0, res )   

#...............................................................................

class PopularityPlotterUnitTestCrashes( PopularityPlotterTestCase ):
  '''
    PopularityPlotterUnitTestCrashes
    <constructor>
     - test_instantiate
    <class variables>
    <methods>
    - test_reportDataUsage
    - test_reportNormalizedDataUsage
    - test_plotDataUsage
    - test_plotNormalizedDataUsage
  '''
    
  def test_instantiate( self ):
    ''' test the constructor
    '''
  
    self.assertRaises( TypeError, self.classsTested )
    self.assertRaises( TypeError, self.classsTested, None )
    self.assertRaises( TypeError, self.classsTested, None, None, None, None )
  
    self.assertRaises( TypeError, self.classsTested, extraArgs = None )
    self.assertRaises( TypeError, self.classsTested, None, extraArgs = None )
    self.assertRaises( TypeError, self.classsTested, None, None, None, extraArgs = None )
  
  def test_reportDataUsage( self ):
    ''' test the method "_reportDataUsage"
    '''
    
    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    self.assertRaises( KeyError, obj._reportDataUsage, {} )
    self.assertRaises( IndexError, obj._reportDataUsage, { 'groupingFields' : [] } )
    self.assertRaises( TypeError, obj._reportDataUsage,  { 'groupingFields' : [1,2] } )
    self.assertRaises( TypeError, obj._reportDataUsage,  { 'groupingFields' : [1,[ 2 ] ] } )
    self.assertRaises( TypeError, obj._reportDataUsage,  { 'groupingFields' : ['1', '2' ] } )
    self.assertRaises( KeyError, obj._reportDataUsage,   { 'groupingFields' : ['1',[ 2 ] ] } )
    self.assertRaises( KeyError, obj._reportDataUsage,   { 'groupingFields' : ['1', [2,2] ],
                                                           'startTime'      : None } )
    self.assertRaises( KeyError, obj._reportDataUsage,   { 'groupingFields' : ['1', [2,2] ],
                                                           'startTime'      : None,
                                                           'endTime'        : None } )
    self.assertRaises( TypeError, obj._reportDataUsage,  { 'groupingFields' : ['1', [2,2] ],
                                                           'startTime'      : None,
                                                           'endTime'        : None,
                                                           'condDict'       : None } )
    
  def test_reportNormalizedDataUsage( self ):
    ''' test the method "_reportNormalizedDataUsage"
    '''
    
    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    self.assertRaises( KeyError, obj._reportNormalizedDataUsage, {} )
    self.assertRaises( IndexError, obj._reportNormalizedDataUsage, { 'groupingFields' : [] } )
    self.assertRaises( TypeError, obj._reportNormalizedDataUsage,  { 'groupingFields' : [1,2] } )
    self.assertRaises( TypeError, obj._reportNormalizedDataUsage,  { 'groupingFields' : [1,[ 2 ] ] } )
    self.assertRaises( TypeError, obj._reportNormalizedDataUsage,  { 'groupingFields' : ['1', '2' ] } )
    self.assertRaises( KeyError, obj._reportNormalizedDataUsage,   { 'groupingFields' : ['1',[ 2 ] ] } )
    self.assertRaises( KeyError, obj._reportNormalizedDataUsage,   { 'groupingFields' : ['1', [2,2] ],
                                                                     'startTime'      : None } )
    self.assertRaises( KeyError, obj._reportNormalizedDataUsage,   { 'groupingFields' : ['1', [2,2] ],
                                                                     'startTime'      : None,
                                                                     'endTime'        : None } )
    self.assertRaises( TypeError, obj._reportNormalizedDataUsage,  { 'groupingFields' : ['1', [2,2] ],
                                                                     'startTime'      : None,
                                                                     'endTime'        : None,
                                                                     'condDict'       : None } )            

  def test_plotDataUsage( self ):
    ''' test the method "_plotDataUsage"
    '''
    
    obj = self.classsTested( None, None )
    self.assertRaises( TypeError, obj._plotDataUsage, None, None, None )
    self.assertRaises( KeyError, obj._plotDataUsage, {}, None, None )
    self.assertRaises( KeyError, obj._plotDataUsage, { 'startTime' : 'startTime' }, 
                                                        None, None )
    self.assertRaises( TypeError, obj._plotDataUsage, { 'startTime' : 'startTime',
                                                        'endTime'   : 'endTime' }, 
                                                         None, None )
    self.assertRaises( KeyError, obj._plotDataUsage, { 'startTime' : 'startTime',
                                                       'endTime'   : 'endTime' }, 
                                                        {}, None )
    self.assertRaises( KeyError, obj._plotDataUsage, { 'startTime' : 'startTime',
                                                       'endTime'   : 'endTime' }, 
                                                     { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotDataUsage, { 'startTime' : 'startTime',
                                                       'endTime'   : 'endTime',
                                                       'grouping'  : 'grouping' }, 
                                                     { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotDataUsage, { 'startTime' : 'startTime',
                                                       'endTime'   : 'endTime',
                                                       'grouping'  : 'grouping' }, 
                                                     { 'granularity'   : 'granularity',
                                                       'graphDataDict' : 'graphDataDict' }, None )

  def test_plotNormalizedDataUsage( self ):
    ''' test the method "_plotNormalizedDataUsage"
    '''
    
    obj = self.classsTested( None, None )
    self.assertRaises( TypeError, obj._plotNormalizedDataUsage, None, None, None )
    self.assertRaises( KeyError, obj._plotNormalizedDataUsage, {}, None, None )
    self.assertRaises( KeyError, obj._plotNormalizedDataUsage, { 'startTime' : 'startTime' }, 
                                                               None, None )
    self.assertRaises( TypeError, obj._plotNormalizedDataUsage, { 'startTime' : 'startTime',
                                                                  'endTime'   : 'endTime' }, 
                                                                None, None )
    self.assertRaises( KeyError, obj._plotNormalizedDataUsage, { 'startTime' : 'startTime',
                                                                 'endTime'   : 'endTime' }, 
                                                               {}, None )
    self.assertRaises( KeyError, obj._plotNormalizedDataUsage, { 'startTime' : 'startTime',
                                                                 'endTime'   : 'endTime' }, 
                                                               { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotNormalizedDataUsage, { 'startTime' : 'startTime',
                                                                 'endTime'   : 'endTime',
                                                                 'grouping'  : 'grouping' }, 
                                                               { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotNormalizedDataUsage, { 'startTime' : 'startTime',
                                                                 'endTime'   : 'endTime',
                                                                 'grouping'  : 'grouping' }, 
                                                               { 'granularity'   : 'granularity',
                                                                 'graphDataDict' : 'graphDataDict' }, None )
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF