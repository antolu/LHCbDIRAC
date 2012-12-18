'''
  Unittest for:
    LHCbDIRAC.AccountingSystem.private.Plotters.StoragePlotter
    
  StoragePlotter.__bases__:
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
  #Crops image to remove the "Generated on xxxx UTC" string
  image1 = Image.open( file1Path ).crop( ( 0, 0, 800, 570 ) )
  image2 = Image.open( file2Path ).crop( ( 0, 0, 800, 570 ) )
  h1 = image1.histogram()
  h2 = image2.histogram()
  rms = math.sqrt( reduce( operator.add,
                           map(lambda a,b: (a-b)**2, h1, h2))/len(h1) )
  return rms

#...............................................................................

class StoragePlotterTestCase( unittest.TestCase ):
  '''
    StoragePlotterTestCase
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
    class MockStorage:
      #pylint: disable=C0111,R0903,W0232
      definitionKeyFields = ( 'StorageElement', 'Directory' )
    
    moduleTested.Storage = mock.Mock( return_value = MockStorage() )
    
    return moduleTested
    
  def setUp( self ):
    '''
      Setup the test case
    '''
    
    import LHCbDIRAC.AccountingSystem.private.Plotters.StoragePlotter as moduleTested
    
    self.moduleTested = self.mockModuleTested( moduleTested )
    self.classsTested = self.moduleTested.StoragePlotter
    
  def tearDown( self ):
    '''
      Tear down the test case
    '''
    
    del self.moduleTested
    del self.classsTested
    
#...............................................................................  

class StoragePlotterUnitTest( StoragePlotterTestCase ):
  '''
    DataStoragePlotterUnitTest
    <constructor>
     - test_instantiate
    <class variables>
     - test_typeName
     - test_typeKeyFields
     - test_reportCatalogSpaceName
     - test_reportCatalogFilesName
     - test_reportPhysicalSpaceName
     - test_reportPhysicalFilesName
     - test_reportPFNvsLFNFileMultiplicityName
     - test_reportPFNvsLFNSizeMultiplicityName 
    <methods>
     - test_reportCatalogSpace
     - test_plotCatalogSpace
     - test_reportCatalogFiles
     - test_plotCatalogFiles
  ''' 
  #FIXME: missing test_reportPhysicalSpace
  #FIXME: missing test_plotPhysicalSpace
  #FIXME: missing test_reportPhysicalFiles
  #FIXME: missing test_plotPhysicalFiles
  #FIXME: missing test_reportPFNvsLFNFileMultiplicity
  #FIXME: missing test_plotPFNvsLFNFileMultiplicity
  #FIXME: missing test_reportPFNvsLFNSizeMultiplicity
  #FIXME: missing test_plotPFNvsLFNSizeMultiplicity
  #FIXME: missing test_multiplicityReport
    
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''     
    obj = self.classsTested( None, None )
    self.assertEqual( 'StoragePlotter', obj.__class__.__name__ )

  def test_typeName( self ):
    ''' test the class variable "_typeName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._typeName, "Storage" )    
  
  def test_typeKeyFields( self ):
    ''' test the class variable "_typeKeyFields" 
    '''      
    obj = self.classsTested( None, None )
    self.assertEqual( obj._typeKeyFields, [ 'StorageElement', 'Directory' ] )

  def test_reportCatalogSpaceName( self ):
    ''' test the class variable "_reportCatalogSpaceName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._reportCatalogSpaceName, "LFN size" )

  def test_reportCatalogFilesName( self ):
    ''' test the class variable "_reportCatalogFilesName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._reportCatalogFilesName, "LFN files" )
    
  def test_reportPhysicalSpaceName( self ):
    ''' test the class variable "_reportPhysicalSpaceName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._reportPhysicalSpaceName, "PFN size" )

  def test_reportPhysicalFilesName( self ):
    ''' test the class variable "_reportPhysicalFilesName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._reportPhysicalFilesName, "PFN files" )

  def test_reportPFNvsLFNFileMultiplicityName( self ):
    ''' test the class variable "_reportPFNvsLFNFileMultiplicityName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._reportPFNvsLFNFileMultiplicityName, "PFN/LFN file ratio" )

  def test_reportPFNvsLFNSizeMultiplicityName( self ):
    ''' test the class variable "_reportPFNvsLFNSizeMultiplicityName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._reportPFNvsLFNSizeMultiplicityName, "PFN/LFN size ratio" )

  def test_reportCatalogSpace( self ):
    ''' test the method "_reportCatalogSpace"
    '''
    
    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    res = obj._reportCatalogSpace( { 'grouping' : 'StorageElement' } )
    self.assertEqual( res[ 'OK' ], False, msg = 'Rejected StorageElement grouping' )
    self.assertEqual( res[ 'Message' ], 'Grouping by storage element when requesting lfn info makes no sense' )
    
    res = obj._reportCatalogSpace( { 'grouping'       : 'NextToABeer',
                                     'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                     'startTime'      : 'startTime',
                                     'endTime'        : 'endTime',
                                     'condDict'       : {} 
                                    } )
    self.assertEqual( res[ 'OK' ], False )
    self.assertEqual( res[ 'Message' ], 'No connection' )
    
    #Changed mocked to run over different lines of code
    mockAccountingDB._getConnection.return_value               = { 'OK' : True, 'Value' : [] }
    res = obj._reportCatalogSpace( { 'grouping'       : 'NextToABeer',
                                     'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                     'startTime'      : 'startTime',
                                     'endTime'        : 'endTime',
                                     'condDict'       : {} 
                                    } )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'graphDataDict': {}, 
                                        'data'         : {}, 
                                        'unit'         : 'MB', 
                                        'granularity'  : 'BucketLength'
                                       })
    
    mockedData = ( ( '/lhcb/data', 1355616000L, 86400, Decimal( '4935388524246.91' ) ), 
                   ( '/lhcb/data', 1355702400L, 86400, Decimal( '4843844487074.82' ) ),
                   ( '/lhcb/LHCb', 1355616000L, 86400, Decimal( '3935388524246.91' ) ), 
                   ( '/lhcb/LHCb', 1355702400L, 86400, Decimal( '3843844487074.82' ) ) )
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : mockedData }
    mockAccountingDB.calculateBucketLengthForTime.return_value = 86400
    
    res = obj._reportCatalogSpace( { 'grouping'       : 'Directory',
                                     'groupingFields' : ( '%s', [ 'Directory' ] ),
                                     'startTime'      : 1355663249.0,
                                     'endTime'        : 1355749690.0,
                                     'condDict'       : { 'Directory' : [ '/lhcb/data', '/lhcb/LHCb' ] } 
                                    } )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'graphDataDict' : { '/lhcb/data' : { 1355616000L : 4.9353885242469104, 
                                                                             1355702400L : 4.8438444870748203 }, 
                                                            '/lhcb/LHCb' : { 1355616000L : 3.93538852424691, 
                                                                             1355702400L : 3.8438444870748198 }
                                                           }, 
                                        'data'          : { '/lhcb/data' : { 1355616000L : 4935388.5242469106, 
                                                                             1355702400L : 4843844.4870748203 }, 
                                                            '/lhcb/LHCb' : { 1355616000L : 3935388.5242469101, 
                                                                             1355702400L : 3843844.4870748199 }
                                                           }, 
                                        'unit'          : 'TB', 
                                        'granularity'   : 86400 } )

  def test_plotCatalogSpace( self ):
    ''' test the method "_plotCatalogSpace"
    '''    
    
    plotName      = 'StoragePlotter_plotCatalogSpace'
    reportRequest = { 'grouping'       : 'Directory',
                      'groupingFields' : ( '%s', [ 'Directory' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'Directory' : [ '/lhcb/data', '/lhcb/LHCb' ] }
                    } 
    plotInfo =  { 'graphDataDict' : { '/lhcb/data' : { 1355616000L : 4.9353885242469104, 
                                                       1355702400L : 4.8438444870748203 }, 
                                      '/lhcb/LHCb' : { 1355616000L : 3.93538852424691, 
                                                       1355702400L : 3.8438444870748198 }
                                     }, 
                  'data'          : { '/lhcb/data' : { 1355616000L : 4935388.5242469106, 
                                                       1355702400L : 4843844.4870748203 }, 
                                      '/lhcb/LHCb' : { 1355616000L : 3935388.5242469101, 
                                                       1355702400L : 3843844.4870748199 }
                                     }, 
                  'unit'          : 'TB', 
                  'granularity'   : 86400
                 }
    
    obj = self.classsTested( None, None )
    res = obj._plotCatalogSpace( reportRequest, plotInfo, plotName )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )
    
    res = compare( '%s.png' % plotName, 'LHCbDIRAC/AccountingSystem/private/Plotters/test/png/%s.png' % plotName )
    self.assertEquals( 0.0, res )

  def test_reportCatalogFiles( self ):
    ''' test the method "_reportCatalogFiles"
    '''

    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    res = obj._reportCatalogFiles( { 'grouping' : 'StorageElement' } )
    self.assertEqual( res[ 'OK' ], False )
    self.assertEqual( res[ 'Message' ], 'Grouping by storage element when requesting lfn info makes no sense' )
    
    res = obj._reportCatalogFiles( { 'grouping'       : 'NextToABeer',
                                     'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                     'startTime'      : 'startTime',
                                     'endTime'        : 'endTime',
                                     'condDict'       : {} 
                                    } )
    self.assertEqual( res[ 'OK' ], False )
    self.assertEqual( res[ 'Message' ], 'No connection' )
    
    #Changed mocked to run over different lines of code
    mockAccountingDB._getConnection.return_value               = { 'OK' : True, 'Value' : [] }
    res = obj._reportCatalogFiles( { 'grouping'       : 'NextToABeer',
                                     'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                     'startTime'      : 'startTime',
                                     'endTime'        : 'endTime',
                                     'condDict'       : {} 
                                    } )
    self.assertEqual( res[ 'OK' ], True, msg = 'Expected S_OK' )
    self.assertEqual( res[ 'Value' ], { 'graphDataDict': {}, 
                                        'data'         : {}, 
                                        'unit'         : 'files', 
                                        'granularity'  : 'BucketLength'
                                       } )
    
    mockedData = ( ( '/lhcb/data', 1355616000L, 86400, Decimal( '4935388524246.91' ) ), 
                   ( '/lhcb/data', 1355702400L, 86400, Decimal( '4843844487074.82' ) ),
                   ( '/lhcb/LHCb', 1355616000L, 86400, Decimal( '3935388524246.91' ) ), 
                   ( '/lhcb/LHCb', 1355702400L, 86400, Decimal( '3843844487074.82' ) ) )
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : mockedData }
    mockAccountingDB.calculateBucketLengthForTime.return_value = 86400
    
    res = obj._reportCatalogFiles( { 'grouping'       : 'Directory',
                                     'groupingFields' : ( '%s', [ 'Directory' ] ),
                                     'startTime'      : 1355663249.0,
                                     'endTime'        : 1355749690.0,
                                     'condDict'       : { 'Directory' : [ '/lhcb/data', '/lhcb/LHCb' ] } 
                                    } )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'graphDataDict' : { '/lhcb/data' : { 1355616000L : 4.5242469100000005, 
                                                                             1355702400L : 4.4870748200000001 }, 
                                                            '/lhcb/LHCb' : { 1355616000L : 3.52424691, 
                                                                             1355702400L : 0.38707481999999999} 
                                                           }, 
                                        'data'           : { '/lhcb/data' : { 1355616000L : 4524246.9100000001, 
                                                                              1355702400L : 4487074.8200000003 }, 
                                                             '/lhcb/LHCb' : { 1355616000L : 3524246.9100000001, 
                                                                              1355702400L : 387074.82000000001 }
                                                            }, 
                                        'unit'           : 'Mfiles', 
                                        'granularity'    : 86400 } )

  def test_plotCatalogFiles( self ):
    ''' test the method "_plotCatalogFiles"
    '''    
    
    plotName = 'StoragePlotter_plotCatalogFiles'
    reportRequest = { 'grouping'       : 'Directory',
                      'groupingFields' : ( '%s', [ 'Directory' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'Directory' : [ '/lhcb/data', '/lhcb/LHCb' ] } 
                     }
    plotInfo = { 'graphDataDict' : { '/lhcb/data' : { 1355616000L : 4.5242469100000005, 
                                                      1355702400L : 4.4870748200000001 }, 
                                     '/lhcb/LHCb' : { 1355616000L : 3.52424691, 
                                                      1355702400L : 0.38707481999999999} 
                                    }, 
                 'data'           : { '/lhcb/data' : { 1355616000L : 4524246.9100000001, 
                                                       1355702400L : 4487074.8200000003 }, 
                                      '/lhcb/LHCb' : { 1355616000L : 3524246.9100000001, 
                                                       1355702400L : 387074.82000000001 }
                                     }, 
                 'unit'           : 'Mfiles', 
                 'granularity'    : 86400 
                }
    
    obj = self.classsTested( None, None )
    res = obj._plotCatalogFiles( reportRequest, plotInfo, plotName )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )
    
    res = compare( '%s.png' % plotName, 'LHCbDIRAC/AccountingSystem/private/Plotters/test/png/%s.png' % plotName )
    self.assertEquals( 0.0, res )

  def test_reportPhysicalSpace( self ):
    ''' test the method "_reportPhysicalSpace"
    '''

    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    res = obj._reportPhysicalSpace( { 'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                      'startTime'      : 'startTime',
                                      'endTime'        : 'endTime',
                                      'condDict'       : {} 
                                     } )
    self.assertEqual( res[ 'OK' ], False )
    self.assertEqual( res[ 'Message' ], 'No connection' )
    
    #Changed mocked to run over different lines of code
    mockAccountingDB._getConnection.return_value               = { 'OK' : True, 'Value' : [] }
    res = obj._reportPhysicalSpace( { 'grouping'       : 'NextToABeer',
                                      'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                      'startTime'      : 'startTime',
                                      'endTime'        : 'endTime',
                                      'condDict'       : {} 
                                     } )
    self.assertEqual( res[ 'OK' ], True, msg = 'Expected S_OK' )
    self.assertEqual( res[ 'Value' ], { 'graphDataDict': {}, 
                                        'data'         : {}, 
                                        'unit'         : 'MB', 
                                        'granularity'  : 'BucketLength'
                                       } )
    
    mockedData = ( ( 'CERN-ARCHIVE', 1355616000L, 86400, Decimal( '2344556767812.91' ) ), 
                   ( 'CERN-ARCHIVE', 1355702400L, 86400, Decimal( '2544556767812.91' ) ),
                   ( 'CERN-DST',     1355616000L, 86400, Decimal( '344556767812.91' ) ), 
                   ( 'CERN-DST',     1355702400L, 86400, Decimal( '544556767812.91' ) ) )
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : mockedData }
    mockAccountingDB.calculateBucketLengthForTime.return_value = 86400
    
    res = obj._reportPhysicalSpace( { 'grouping'       : 'StorageElement',
                                      'groupingFields' : ( '%s', [ 'StorageElement' ] ),
                                      'startTime'      : 1355663249.0,
                                      'endTime'        : 1355749690.0,
                                      'condDict'       : { 'StorageElement' : [ 'CERN-ARCHIVE', 'CERN-DST' ] } 
                                     } )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'graphDataDict' : { 'Full stream' : { 1355616000L : 14.754501202, 
                                                                              1355702400L : 15.237810842 }
                                                           }, 
                                        'data'          : { 'Full stream' : { 1355616000L : 14.754501202, 
                                                                              1355702400L : 15.237810842 }
                                                           }, 
                                        'unit'          : 'MB', 
                                        'granularity'   : 86400 
                                        } )

  def test_plotPhysicalSpace( self ):
    ''' test the method "_plotPhysicalSpace"
    '''    
    
    plotName = 'StoragePlotter_plotPhysicalSpace'
    reportRequest = { 'grouping'       : 'StorageElement',
                      'groupingFields' : ( '%s', [ 'StorageElement' ] ),
                      'startTime'      : 1355663249.0,
                      'endTime'        : 1355749690.0,
                      'condDict'       : { 'StorageElement' : [ 'CERN-ARCHIVE', 'CERN-DST' ] } 
                    }
    plotInfo = { 'graphDataDict' : { '/lhcb/data' : { 1355616000L : 4.5242469100000005, 
                                                      1355702400L : 4.4870748200000001 }, 
                                     '/lhcb/LHCb' : { 1355616000L : 3.52424691, 
                                                      1355702400L : 0.38707481999999999} 
                                    }, 
                 'data'           : { '/lhcb/data' : { 1355616000L : 4524246.9100000001, 
                                                       1355702400L : 4487074.8200000003 }, 
                                      '/lhcb/LHCb' : { 1355616000L : 3524246.9100000001, 
                                                       1355702400L : 387074.82000000001 }
                                     }, 
                 'unit'           : 'Mfiles', 
                 'granularity'    : 86400 
                }
    
    obj = self.classsTested( None, None )
    res = obj._plotPhysicalSpace( reportRequest, plotInfo, plotName )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'plot': True, 'thumbnail': False } )
    
    res = compare( '%s.png' % plotName, 'LHCbDIRAC/AccountingSystem/private/Plotters/test/png/%s.png' % plotName )
    self.assertEquals( 0.0, res )

#...............................................................................

class StoragePlotterUnitTestCrashes( StoragePlotterTestCase ):
  '''
    StoragePlotterUnitTestCrashes
    <constructor>
     - test_instantiate
    <class variables>
    <methods>
     - test_reportCatalogSpace
     - test_plotCatalogSpace
     - test_reportCatalogFiles
     - test_plotCatalogFiles
     - test_reportPhysicalSpace
     - test_plotPhysicalSpace
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

  def test_reportCatalogSpace( self ):
    ''' test the method "_reportCatalogSpace"
    '''
    
    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    self.assertRaises( KeyError, obj._reportCatalogSpace, {} )
    self.assertRaises( KeyError, obj._reportCatalogSpace, { 'grouping' : 1 } )
    self.assertRaises( IndexError, obj._reportCatalogSpace, { 'grouping'       : 1,
                                                              'groupingFields' : [] } )
    self.assertRaises( TypeError, obj._reportCatalogSpace, { 'grouping'       : 1,
                                                             'groupingFields' : [1,2] } )
    self.assertRaises( TypeError, obj._reportCatalogSpace, { 'grouping'       : 1,
                                                             'groupingFields' : [1,[ 2 ] ] } )
    self.assertRaises( TypeError, obj._reportCatalogSpace, { 'grouping'       : 1,
                                                             'groupingFields' : ['1', '2' ] } )
    self.assertRaises( KeyError, obj._reportCatalogSpace, { 'grouping'       : 1,
                                                            'groupingFields' : ['1',[ 2 ] ] } )
    self.assertRaises( KeyError, obj._reportCatalogSpace, { 'grouping'       : 1,
                                                            'groupingFields' : ['1', [2,2] ],
                                                            'startTime'      : None } )
    self.assertRaises( KeyError, obj._reportCatalogSpace, { 'grouping'       : 1,
                                                            'groupingFields' : ['1', [2,2] ],
                                                            'startTime'      : None,
                                                            'endTime'        : None } )
    self.assertRaises( TypeError, obj._reportCatalogSpace, { 'grouping'       : 1,
                                                             'groupingFields' : ['1', [2,2] ],
                                                             'startTime'      : None,
                                                             'endTime'        : None,
                                                             'condDict'       : None } )
  
  def test_plotCatalogSpace( self ):
    ''' test the method "_plotCatalogSpace"
    '''
    
    obj = self.classsTested( None, None )
    self.assertRaises( TypeError, obj._plotCatalogSpace, None, None, None )
    self.assertRaises( KeyError, obj._plotCatalogSpace, {}, None, None )
    self.assertRaises( KeyError, obj._plotCatalogSpace, { 'startTime' : 'startTime' }, 
                                                        None, None )
    self.assertRaises( TypeError, obj._plotCatalogSpace, { 'startTime' : 'startTime',
                                                           'endTime'   : 'endTime' }, 
                                                         None, None )
    self.assertRaises( KeyError, obj._plotCatalogSpace, { 'startTime' : 'startTime',
                                                          'endTime'   : 'endTime' }, 
                                                        {}, None )
    self.assertRaises( KeyError, obj._plotCatalogSpace, { 'startTime' : 'startTime',
                                                          'endTime'   : 'endTime' }, 
                                                        { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotCatalogSpace, { 'startTime' : 'startTime',
                                                          'endTime'   : 'endTime',
                                                          'grouping'  : 'grouping' }, 
                                                        { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotCatalogSpace, { 'startTime' : 'startTime',
                                                          'endTime'   : 'endTime',
                                                          'grouping'  : 'grouping' }, 
                                                        { 'granularity' : 'granularity',
                                                          'graphDataDict' : 'graphDataDict' }, None )    

  def test_reportCatalogFiles( self ):
    ''' test the method "_reportCatalogFiles"
    '''
    
    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    self.assertRaises( KeyError, obj._reportCatalogFiles, {} )
    self.assertRaises( KeyError, obj._reportCatalogFiles, { 'grouping' : 1 } )
    self.assertRaises( IndexError, obj._reportCatalogFiles, { 'grouping'       : 1,
                                                              'groupingFields' : [] } )
    self.assertRaises( TypeError, obj._reportCatalogFiles, { 'grouping'       : 1,
                                                             'groupingFields' : [1,2] } )
    self.assertRaises( TypeError, obj._reportCatalogFiles, { 'grouping'       : 1,
                                                             'groupingFields' : [1,[ 2 ] ] } )
    self.assertRaises( TypeError, obj._reportCatalogFiles, { 'grouping'       : 1,
                                                             'groupingFields' : ['1', '2' ] } )
    self.assertRaises( KeyError, obj._reportCatalogFiles, { 'grouping'       : 1,
                                                            'groupingFields' : ['1',[ 2 ] ] } )
    self.assertRaises( KeyError, obj._reportCatalogFiles, { 'grouping'       : 1,
                                                            'groupingFields' : ['1', [2,2] ],
                                                            'startTime'      : None } )
    self.assertRaises( KeyError, obj._reportCatalogFiles, { 'grouping'       : 1,
                                                            'groupingFields' : ['1', [2,2] ],
                                                            'startTime'      : None,
                                                            'endTime'        : None } )
    self.assertRaises( TypeError, obj._reportCatalogFiles, { 'grouping'       : 1,
                                                             'groupingFields' : ['1', [2,2] ],
                                                             'startTime'      : None,
                                                             'endTime'        : None,
                                                             'condDict'       : None } )

  def test_plotCatalogFiles( self ):
    ''' test the method "_plotCatalogFiles"
    '''
    
    obj = self.classsTested( None, None )
    self.assertRaises( TypeError, obj._plotCatalogFiles, None, None, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, {}, None, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, { 'startTime' : 'startTime' }, 
                                                        None, None )
    self.assertRaises( TypeError, obj._plotCatalogFiles, { 'startTime' : 'startTime',
                                                           'endTime'   : 'endTime' }, 
                                                         None, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, { 'startTime' : 'startTime',
                                                          'endTime'   : 'endTime' }, 
                                                        {}, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, { 'startTime' : 'startTime',
                                                          'endTime'   : 'endTime' }, 
                                                        { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, { 'startTime' : 'startTime',
                                                          'endTime'   : 'endTime',
                                                          'grouping'  : 'grouping' }, 
                                                        { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, { 'startTime' : 'startTime',
                                                          'endTime'   : 'endTime',
                                                          'grouping'  : 'grouping' }, 
                                                        { 'granularity' : 'granularity',
                                                          'graphDataDict' : 'graphDataDict' }, None )

  def test_reportPhysicalSpace( self ):
    ''' test the method "_reportPhysicalSpace"
    '''
    
    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    self.assertRaises( KeyError, obj._reportPhysicalSpace, {} )
    self.assertRaises( IndexError, obj._reportPhysicalSpace, { 'groupingFields' : [] } )
    self.assertRaises( TypeError, obj._reportPhysicalSpace, { 'groupingFields' : [1,2] } )
    self.assertRaises( TypeError, obj._reportPhysicalSpace, { 'groupingFields' : [1,[ 2 ] ] } )
    self.assertRaises( TypeError, obj._reportPhysicalSpace, { 'groupingFields' : ['1', '2' ] } )
    self.assertRaises( KeyError, obj._reportPhysicalSpace, { 'groupingFields' : ['1',[ 2 ] ] } )
    self.assertRaises( KeyError, obj._reportPhysicalSpace, { 'groupingFields' : ['1', [2,2] ],
                                                             'startTime'      : None } )
    self.assertRaises( KeyError, obj._reportPhysicalSpace, { 'groupingFields' : ['1', [2,2] ],
                                                             'startTime'      : None,
                                                             'endTime'        : None } )
    self.assertRaises( TypeError, obj._reportPhysicalSpace, { 'groupingFields' : ['1', [2,2] ],
                                                              'startTime'      : None,
                                                              'endTime'        : None,
                                                              'condDict'       : None } )
    
  def test_plotPhysicalSpace( self ):
    ''' test the method "_plotPhysicalSpace"
    '''
    
    obj = self.classsTested( None, None )
    self.assertRaises( TypeError, obj._plotPhysicalSpace, None, None, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, {}, None, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, { 'startTime' : 'startTime' }, 
                                                         None, None )
    self.assertRaises( TypeError, obj._plotPhysicalSpace, { 'startTime' : 'startTime',
                                                            'endTime'   : 'endTime' }, 
                                                          None, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, { 'startTime' : 'startTime',
                                                           'endTime'   : 'endTime' }, 
                                                         {}, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, { 'startTime' : 'startTime',
                                                           'endTime'   : 'endTime' }, 
                                                         { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, { 'startTime' : 'startTime',
                                                           'endTime'   : 'endTime',
                                                           'grouping'  : 'grouping' }, 
                                                         { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, { 'startTime' : 'startTime',
                                                           'endTime'   : 'endTime',
                                                           'grouping'  : 'grouping' }, 
                                                         { 'granularity' : 'granularity',
                                                           'graphDataDict' : 'graphDataDict' }, None )
          

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF