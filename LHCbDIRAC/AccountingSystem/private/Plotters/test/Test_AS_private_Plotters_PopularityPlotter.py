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
  '''

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''     
    obj = self.classsTested( None, None )
    self.assertEqual( 'PopularityPlotter', obj.__class__.__name__,
                      msg = 'Expected PopularityPlotter object' )

  def test_typeName( self ):
    ''' test the class variable "_typeName" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._typeName, "Popularity", 
                      msg = 'Expected Popularity as value' )

  def test_typeKeyFields( self ):
    ''' test the class variable "_typeKeyFields" 
    '''
    obj = self.classsTested( None, None )
    self.assertEqual( obj._typeKeyFields, [ 'DataType', 'Activity', 'FileType', 
                                            'Production', 'ProcessingPass', 
                                            'Conditions', 'EventType', 'StorageElement'
                                          ],   
                      msg = 'Expected keys from MockPopularity' )
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF