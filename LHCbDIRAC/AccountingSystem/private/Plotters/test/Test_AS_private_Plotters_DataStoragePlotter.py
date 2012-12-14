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
    mockValue = 'DataType, Activity, FileType, Production, ProcessingPass, Conditions, EventType' 
    obj = self.classsTested( None, None )
    self.assertEqual( obj._noSEGrouping, mockValue )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF