# $HeadURL: $
''' Test_Resources_Storage_LHCbOnlineStorage

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

def dummyCallable( whatever ):
  return whatever
  
class dxmlrpclib( DummyReturn )  : pass
class dStorageBase( DummyReturn ): pass

################################################################################

class LHCbOnlineStorage_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.Resources.Storage.LHCbOnlineStorage as moduleTested
    moduleTested.xmlrpclib   = dxmlrpclib()
    moduleTested.StorageBase = dStorageBase   
    moduleTested.LHCbOnlineStorage.__bases__ = ( dStorageBase, ) 
    
    self.resource = moduleTested.LHCbOnlineStorage
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.resource
      
################################################################################
# Tests

class LHCbOnlineStorage_Success( LHCbOnlineStorage_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    global dummyResults
    dummyResults[ 'dxmlrpclib'   ] = None
    dummyResults[ 'dStorageBase' ] = None 
    
    resource = self.resource( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    self.assertEqual( 'LHCbOnlineStorage', resource.__class__.__name__ )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF