''' Test_AS_Client_Type_DataStorage

'''

import unittest

__RCSID__ = '$Id: $'

dummyResults = {}
class DummyReturn():
  
  def __init__( self, *args, **kwargs ):
    pass
  def __getattr__( self, name ):
    return self.dummyMethod
  def dummyMethod( self, *args, **kwargs ):
    return dummyResults[ self.__class__.__name__ ]
  
class dBaseAccountingType( DummyReturn )  : pass

################################################################################

class DataStorage_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.AccountingSystem.Client.Types.DataStorage as moduleTested
    moduleTested.BaseAccountingType = dBaseAccountingType   
    moduleTested.DataStorage.__bases__ = ( dBaseAccountingType, ) 
    
    self.accountingType = moduleTested.DataStorage
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.accountingType
    
class DataStorage_Success( DataStorage_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''     
    global dummyResults
    dummyResults[ 'DataStorage' ] = None
    
    accountingType = self.accountingType()
    self.assertEqual( 'DataStorage', accountingType.__class__.__name__ )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    