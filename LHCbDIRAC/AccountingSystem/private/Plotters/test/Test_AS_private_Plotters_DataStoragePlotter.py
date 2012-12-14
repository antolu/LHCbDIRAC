'''
  Unittest for:
    LHCbDIRAC.AccountingSystem.private.Plotters.DataStoragePlotter
    
  DataStoragePlotter.__bases__:
    DIRAC.AccountingSystem.private.Plotters.BaseReporter  
  
  We are assuming there is a solid test of __bases__, we are not testing them
  here and assuming they work fine.
'''

import mock
import unittest

class DataStoragePlotterTestCase( unittest.TestCase ):
  '''
  ''' 

  moduleTested = None
  classsTested = None
  
  def mockModuleTested( self, moduleTested ):
    '''
      Used to not redo the mocking done on the parent class ( if any )
    '''
    
    # Tries to get the mocks of the parent TestCases ( if any )
    for baseClass in DataStoragePlotterTestCase.__bases__:
      try:
        moduleTested = baseClass.mockModuleTested( moduleTested )
      except AttributeError:
        continue  
    
    # And then makes its own mock
    class MockDataStorage:
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
    self.classsTested = self.moduleTested.DataStoratePlotter 
    
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
    -test_instantiate
  '''

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''     

    obj = self.classsTested( None, None )
    self.assertEqual( 'DataStoragePlotter', obj.__class__.__name__ )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF