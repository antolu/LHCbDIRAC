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
  ''' 
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
                                     'condDict'       : { 'Directory' : [ '/lhcb/data', '/lhcb/LHCb' ]} 
                                    } )
    self.assertEqual( res[ 'OK' ], True )
    self.assertEqual( res[ 'Value' ], { 'graphDataDict' : { 'Full stream': { 1355616000L: 935.38852424691629, 
                                                                             1355702400L: 843.84448707482204
                                                                            }
                                                           }, 
                                        'data'          : { 'Full stream': { 1355616000L: 935388.52424691629, 
                                                                             1355702400L: 843844.48707482207
                                                                            }
                                                           }, 
                                        'unit'          : 'PB', 
                                        'granularity'   : 86400
                                       } )

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