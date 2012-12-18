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
    <methods>
  '''
  #FIXME: missing test_reportCatalogSpaceName
  #FIXME: missing test_reportCatalogFilesName
  #FIXME: missing test_reportPhysicalSpaceName
  #FIXME: missing test_reportPhysicalFilesName
  #FIXME: missing test_reportPFNvsLFNFileMultiplicityName
  #FIXME: missing test_reportPFNvsLFNSizeMultiplicityName
  #FIXME: missing test_reportCatalogSpace
  #FIXME: missing test_plotCatalogSpace
  #FIXME: missing test_reportCatalogFiles
  #FIXME: missing test_plotCatalogFiles
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

#...............................................................................

class StoragePlotterUnitTestCrashes( StoragePlotterTestCase ):
  '''
    StoragePlotterUnitTestCrashes
    <constructor>
     - test_instantiate
    <class variables>
    <methods>
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

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF