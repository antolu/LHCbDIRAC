'''
  Unittest for:
    LHCbDIRAC.AccountingSystem.private.Plotters.DataStoragePlotter
    
  DataStoragePlotter.__bases__:
    DIRAC.AccountingSystem.private.Plotters.BaseReporter  
  
  We are assuming there is a solid test of __bases__, we are not testing them
  here and assuming they work fine.
  
  IMPORTANT: the test MUST be pylint compliant !
'''

import mock
import unittest

class DataStoragePlotterTestCase( unittest.TestCase ):
  '''
    DataStoragePlotterTestCase
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
    class MockDataStorage:
      #pylint: disable=C0111,R0903,W0232
      definitionKeyFields = ( 'DataType', 'Activity', 'FileType', 'Production',    
                              'ProcessingPass', 'Conditions', 'EventType',      
                              'StorageElement' )
    
    moduleTested.DataStorage = mock.Mock( return_value = MockDataStorage() )
    
    return moduleTested
    
  def setUp( self ):
    '''
      Setup the test case
    '''
    
    import LHCbDIRAC.AccountingSystem.private.Plotters.DataStoragePlotter as moduleTested
    
    self.moduleTested = self.mockModuleTested( moduleTested )
    self.classsTested = self.moduleTested.DataStoragePlotter
    
  def tearDown( self ):
    '''
      Tear down the test case
    '''
    
    del self.moduleTested
    del self.classsTested
    
#...............................................................................    

class DataStoragePlotterUnitTest( DataStoragePlotterTestCase ):
  '''
    DataStoragePlotterUnitTest
    - test_instantiate
    - test_typeName
    - test_typeKeyFields
    - test_noSEtypeKeyFields
    - test_noSEGrouping
    - test_reportCatalogSpaceName
    - test_reportCatalogFilesName
    - test_reportPhysicalSpaceName
    - test_reportPhysicalFilesName
    - test_reportCatalogSpace
    - test_reportCatalogFiles
    - test_reportPhysicalSpace
    - test_reportPhysicalFiles
  '''
  #FIXME: missing test_plotCatalogSpace
  #FIXME: missing test_plotCatalogFiles
  #FIXME: missing test_plotPhysicalSpace
  #FIXME: missing test_plotPhysicalFiles

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''     
    obj = self.classsTested( None, None )
    self.assertEqual( 'DataStoragePlotter', obj.__class__.__name__,
                      msg = 'Expected DataStoragePlotter object' )
  
  def test_typeName( self ):
    ''' test the class variable "_typeName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._typeName, "DataStorage", 
                      msg = 'Expected DataStorage as value' )    
  
  def test_typeKeyFields( self ):
    ''' test the class variable "_typeKeyFields" 
    '''      
    obj = self.classsTested( None, None )
    self.assertEqual( obj._typeKeyFields, [ 'DataType', 'Activity', 'FileType', 
                                            'Production', 'ProcessingPass', 
                                            'Conditions', 'EventType', 'StorageElement' ],
                      msg =  'Expected keys from MockDataStorage' )
        
  def test_noSEtypeKeyFields( self ):
    ''' test the class variable "_noSEtypeKeyFields" 
    ''' 
    obj = self.classsTested( None, None )
    self.assertEqual( obj._noSEtypeKeyFields, [ 'DataType', 'Activity', 'FileType', 
                                                'Production', 'ProcessingPass', 
                                                'Conditions', 'EventType' ],
                      msg =  'Expected keys from MockDataStorage, without StorageElement' )

  def test_noSEGrouping( self ):
    ''' test the class variable "_noSEGrouping" 
    '''
    mockValue = ( '%s, %s, %s, %s, %s, %s, %s', [ 'DataType', 'Activity', 'FileType', 
                                                  'Production', 'ProcessingPass', 
                                                  'Conditions', 'EventType'
                                                ]
                  ) 
    obj = self.classsTested( None, None )
    self.assertEqual( obj._noSEGrouping, ( mockValue ),
                      msg = 'Expected tuple with string and MockDataStorage keys without StorageElement key' )
    
  def test_reportCatalogSpaceName( self ):
    ''' test the class variable "_reportCatalogSpaceName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._reportCatalogSpaceName, "LFN size",
                      msg = 'Expected LFN size as value' )
  
  def test_reportCatalogFilesName( self ):
    ''' test the class variable "_reportCatalogFilesName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._reportCatalogFilesName, "LFN files",
                      msg = 'Expected LFN files as value' )
    
  def test_reportPhysicalSpaceName( self ):
    ''' test the class variable "_reportPhysicalSpaceName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._reportPhysicalSpaceName, "PFN size",
                      msg = 'Expected PFN size as value' )
  
  def test_reportPhysicalFilesName( self ):
    ''' test the class variable "_reportPhysicalFilesName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._reportPhysicalFilesName, "PFN files",
                      msg = 'Expected PFN files as value' )
    
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
    self.assertEqual( res[ 'Message' ], 'Grouping by storage element when requesting lfn info makes no sense',
                      msg = 'Rejected StorageElement grouping' )
    
    res = obj._reportCatalogSpace( { 'grouping'       : 'NextToABeer',
                                     'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                     'startTime'      : 'startTime',
                                     'endTime'        : 'endTime',
                                     'condDict'       : {} 
                                    } )
    self.assertEqual( res[ 'OK' ], False, msg = 'Correct input parameters, but no DB connection' )
    self.assertEqual( res[ 'Message' ], 'No connection',
                      msg = 'Correct input parameters, but no DB connection' )
    
    #Changed mocked to run over different lines of code
    mockAccountingDB._getConnection.return_value               = { 'OK' : True, 'Value' : [] }
    res = obj._reportCatalogSpace( { 'grouping'       : 'NextToABeer',
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
                                       },
                      msg = 'Expected dictionary with keys graphDataDict, data, unit & granularity' )
    
    #FIXME: continue test...

  def test_reportCatalogFiles( self ):
    ''' test the method "_reportCatalogFiles"
    '''

    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    res = obj._reportCatalogFiles( { 'grouping' : 'StorageElement' } )
    self.assertEqual( res[ 'OK' ], False, msg = 'Rejected StorageElement grouping' )
    self.assertEqual( res[ 'Message' ], 'Grouping by storage element when requesting lfn info makes no sense',
                      msg = 'Rejected StorageElement grouping' )
    
    res = obj._reportCatalogFiles( { 'grouping'       : 'NextToABeer',
                                     'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                     'startTime'      : 'startTime',
                                     'endTime'        : 'endTime',
                                     'condDict'       : {} 
                                    } )
    self.assertEqual( res[ 'OK' ], False, msg = 'Correct input parameters, but no DB connection' )
    self.assertEqual( res[ 'Message' ], 'No connection',
                      msg = 'Correct input parameters, but no DB connection' )
    
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
                                       },
                      msg = 'Expected dictionary with keys graphDataDict, data, unit & granularity' )
    
    #FIXME: continue test...
    
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
    self.assertEqual( res[ 'OK' ], False, msg = 'Correct input parameters, but no DB connection' )
    self.assertEqual( res[ 'Message' ], 'No connection',
                      msg = 'Correct input parameters, but no DB connection' )
    
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
                                       },
                      msg = 'Expected dictionary with keys graphDataDict, data, unit & granularity' )
    
    #FIXME: continue test...  

  def test_reportPhysicalFiles( self ):
    ''' test the method "_reportPhysicalFiles"
    '''

    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    res = obj._reportPhysicalFiles( { 'groupingFields' : [ 0, [ 'mehh' ], 'blah' ],
                                      'startTime'      : 'startTime',
                                      'endTime'        : 'endTime',
                                      'condDict'       : {} 
                                     } )
    self.assertEqual( res[ 'OK' ], False, msg = 'Correct input parameters, but no DB connection' )
    self.assertEqual( res[ 'Message' ], 'No connection',
                      msg = 'Correct input parameters, but no DB connection' )
    
    #Changed mocked to run over different lines of code
    mockAccountingDB._getConnection.return_value               = { 'OK' : True, 'Value' : [] }
    res = obj._reportPhysicalFiles( { 'grouping'       : 'NextToABeer',
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
                                       },
                      msg = 'Expected dictionary with keys graphDataDict, data, unit & granularity' )
    
    #FIXME: continue test...    

class DataStoragePlotterUnitTestCrashes( DataStoragePlotterTestCase ):
  '''
    DataStoragePlotterUnitTest
    - test_reportCatalogSpace
    - test_reportCatalogFiles
    - test_reportPhysicalSpace
    - test_reportPhysicalFiles
    - test_plotCatalogSpace
    - test_plotCatalogFiles
    - test_plotPhysicalSpace
    - test_plotPhysicalFiles
  '''
  
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

  def test_reportPhysicalFiles( self ):
    ''' test the method "_reportPhysicalFiles"
    '''
    
    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    self.assertRaises( KeyError, obj._reportPhysicalFiles, {} )
    self.assertRaises( IndexError, obj._reportPhysicalFiles, { 'groupingFields' : [] } )
    self.assertRaises( TypeError, obj._reportPhysicalFiles, { 'groupingFields' : [1,2] } )
    self.assertRaises( TypeError, obj._reportPhysicalFiles, { 'groupingFields' : [1,[ 2 ] ] } )
    self.assertRaises( TypeError, obj._reportPhysicalFiles, { 'groupingFields' : ['1', '2' ] } )
    self.assertRaises( KeyError, obj._reportPhysicalFiles, { 'groupingFields' : ['1',[ 2 ] ] } )
    self.assertRaises( KeyError, obj._reportPhysicalFiles, { 'groupingFields' : ['1', [2,2] ],
                                                             'startTime'      : None } )
    self.assertRaises( KeyError, obj._reportPhysicalFiles, { 'groupingFields' : ['1', [2,2] ],
                                                             'startTime'      : None,
                                                             'endTime'        : None } )
    self.assertRaises( TypeError, obj._reportPhysicalFiles, { 'groupingFields' : ['1', [2,2] ],
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
                                                           'endtime'   : 'endTime' }, 
                                                         None, None )
    self.assertRaises( KeyError, obj._plotCatalogSpace, { 'startTime' : 'startTime',
                                                          'endtime'   : 'endTime' }, 
                                                        {}, None )
    self.assertRaises( KeyError, obj._plotCatalogSpace, { 'startTime' : 'startTime',
                                                          'endtime'   : 'endTime' }, 
                                                        { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotCatalogSpace, { 'startTime' : 'startTime',
                                                          'endtime'   : 'endTime',
                                                          'grouping'  : 'grouping' }, 
                                                        { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotCatalogSpace, { 'startTime' : 'startTime',
                                                          'endtime'   : 'endTime',
                                                          'grouping'  : 'grouping' }, 
                                                        { 'granularity' : 'granularity',
                                                          'unit'        : 'unit' }, None )

  def test_plotCatalogFiles( self ):
    ''' test the method "_plotCatalogFiles"
    '''
    
    obj = self.classsTested( None, None )
    self.assertRaises( TypeError, obj._plotCatalogFiles, None, None, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, {}, None, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, { 'startTime' : 'startTime' }, 
                                                        None, None )
    self.assertRaises( TypeError, obj._plotCatalogFiles, { 'startTime' : 'startTime',
                                                           'endtime'   : 'endTime' }, 
                                                         None, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, { 'startTime' : 'startTime',
                                                          'endtime'   : 'endTime' }, 
                                                        {}, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, { 'startTime' : 'startTime',
                                                          'endtime'   : 'endTime' }, 
                                                        { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, { 'startTime' : 'startTime',
                                                          'endtime'   : 'endTime',
                                                          'grouping'  : 'grouping' }, 
                                                        { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotCatalogFiles, { 'startTime' : 'startTime',
                                                          'endtime'   : 'endTime',
                                                          'grouping'  : 'grouping' }, 
                                                        { 'granularity' : 'granularity',
                                                          'unit'        : 'unit' }, None )

  def test_plotPhysicalSpace( self ):
    ''' test the method "_plotPhysicalSpace"
    '''
    
    obj = self.classsTested( None, None )
    self.assertRaises( TypeError, obj._plotPhysicalSpace, None, None, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, {}, None, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, { 'startTime' : 'startTime' }, 
                                                         None, None )
    self.assertRaises( TypeError, obj._plotPhysicalSpace, { 'startTime' : 'startTime',
                                                            'endtime'   : 'endTime' }, 
                                                          None, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, { 'startTime' : 'startTime',
                                                           'endtime'   : 'endTime' }, 
                                                         {}, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, { 'startTime' : 'startTime',
                                                           'endtime'   : 'endTime' }, 
                                                         { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, { 'startTime' : 'startTime',
                                                           'endtime'   : 'endTime',
                                                           'grouping'  : 'grouping' }, 
                                                         { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotPhysicalSpace, { 'startTime' : 'startTime',
                                                           'endtime'   : 'endTime',
                                                           'grouping'  : 'grouping' }, 
                                                         { 'granularity' : 'granularity',
                                                           'unit'        : 'unit' }, None )

  def test_plotPhysicalFiles( self ):
    ''' test the method "_plotPhysicalFiles"
    '''
    
    obj = self.classsTested( None, None )
    self.assertRaises( TypeError, obj._plotPhysicalFiles, None, None, None )
    self.assertRaises( KeyError, obj._plotPhysicalFiles, {}, None, None )
    self.assertRaises( KeyError, obj._plotPhysicalFiles, { 'startTime' : 'startTime' }, 
                                                         None, None )
    self.assertRaises( TypeError, obj._plotPhysicalFiles, { 'startTime' : 'startTime',
                                                            'endtime'   : 'endTime' }, 
                                                          None, None )
    self.assertRaises( KeyError, obj._plotPhysicalFiles, { 'startTime' : 'startTime',
                                                           'endtime'   : 'endTime' }, 
                                                         {}, None )
    self.assertRaises( KeyError, obj._plotPhysicalFiles, { 'startTime' : 'startTime',
                                                           'endtime'   : 'endTime' }, 
                                                         { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotPhysicalFiles, { 'startTime' : 'startTime',
                                                           'endtime'   : 'endTime',
                                                           'grouping'  : 'grouping' }, 
                                                         { 'granularity' : 'granularity' }, None )
    self.assertRaises( KeyError, obj._plotPhysicalFiles, { 'startTime' : 'startTime',
                                                           'endtime'   : 'endTime',
                                                           'grouping'  : 'grouping' }, 
                                                         { 'granularity' : 'granularity',
                                                           'unit'        : 'unit' }, None )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF