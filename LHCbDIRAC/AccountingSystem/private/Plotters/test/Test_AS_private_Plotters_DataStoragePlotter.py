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
  '''

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''     
    obj = self.classsTested( None, None )
    self.assertEqual( 'DataStoragePlotter', obj.__class__.__name__ )
  
  def test_typeName( self ):
    ''' test the class variable "_typeName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._typeName, "DataStorage" )    
  
  def test_typeKeyFields( self ):
    ''' test the class variable "_typeKeyFields" 
    '''      
    obj = self.classsTested( None, None )
    self.assertEqual( obj._typeKeyFields, [ 'DataType', 'Activity', 'FileType', 
                                            'Production', 'ProcessingPass', 
                                            'Conditions', 'EventType', 'StorageElement' ] )
        
  def test_noSEtypeKeyFields( self ):
    ''' test the class variable "_noSEtypeKeyFields" 
    ''' 
    obj = self.classsTested( None, None )
    self.assertEqual( obj._noSEtypeKeyFields, [ 'DataType', 'Activity', 'FileType', 
                                                'Production', 'ProcessingPass', 
                                                'Conditions', 'EventType' ])

  def test_noSEGrouping( self ):
    ''' test the class variable "_noSEGrouping" 
    '''
    mockValue = ( '%s, %s, %s, %s, %s, %s, %s', [ 'DataType', 'Activity', 'FileType', 
                                                  'Production', 'ProcessingPass', 
                                                  'Conditions', 'EventType'
                                                ]
                  ) 
    obj = self.classsTested( None, None )
    self.assertEqual( obj._noSEGrouping, ( mockValue ) )
    
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
    
  def test_reportCatalogSpace( self ):
    ''' test the method "_reportCatalogSpace"
    '''
    
    mockAccountingDB = mock.Mock()
    mockAccountingDB._getConnection.return_value               = { 'OK' : False, 'Message' : 'No connection' }
    mockAccountingDB.retrieveBucketedData.return_value         = { 'OK' : True, 'Value' : [] } 
    mockAccountingDB.calculateBucketLengthForTime.return_value = 'BucketLength'
    obj = self.classsTested( mockAccountingDB, None )
    
    res = obj._reportCatalogSpace( { 'grouping' : 'StorageElement' } )
    self.assertEqual( res[ 'OK' ], False )
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
                                       } )
    
    
    
    #FIXME: continue test...

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF