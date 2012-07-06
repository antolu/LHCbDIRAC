# $HeadURL: $
''' Test_Resources_Storage_LHCbOnlineStorage

'''

import mock
import unittest

import LHCbDIRAC.Resources.Storage.LHCbOnlineStorage as moduleTested 

__RCSID__ = '$Id: $'

################################################################################

class LHCbOnlineStorage_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
    
    # Mock external libraries / modules not interesting for the unit test
    mock_xmlrpclib = mock.Mock()
    mock_xmlrpclib.Server.return_value( '' ) 
    
    # Add mocks to moduleTested
    moduleTested.xmlrpclib = mock_xmlrpclib
    
    self.testClass = moduleTested.LHCbOnlineStorage
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass
      
################################################################################
# Tests

class LHCbOnlineStorage_Success( LHCbOnlineStorage_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    self.assertEqual( 'LHCbOnlineStorage', resource.__class__.__name__ )
  
  def test_init( self ):
    ''' test that the init method does what it should do
    '''
    
    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )  
      
    self.assertEqual( 'storageName', resource.name )
    self.assertEqual( 'LHCbOnline' , resource.protocolName )
    self.assertEqual( 'path'       , resource.path )
    self.assertEqual( 'host'       , resource.host )
    self.assertEqual( 'port'       , resource.port )
    self.assertEqual( 'spaceToken' , resource.spaceToken )
    self.assertEqual( 'wspath'     , resource.wspath )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF