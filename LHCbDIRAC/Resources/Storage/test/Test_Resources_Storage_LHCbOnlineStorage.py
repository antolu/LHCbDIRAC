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
    ''' tests that the init method does what it should do
    '''
    
    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )  
      
    self.assertEqual( 'storageName', resource.name )
    self.assertEqual( 'LHCbOnline' , resource.protocolName )
    self.assertEqual( 'protocol'   , resource.protocol )
    self.assertEqual( 'path'       , resource.path )
    self.assertEqual( 'host'       , resource.host )
    self.assertEqual( 'port'       , resource.port )
    self.assertEqual( 'spaceToken' , resource.spaceToken )
    self.assertEqual( 'wspath'     , resource.wspath )

  def test_getParameters( self ):
    ''' tests the output of getParameters method
    '''

    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )  

    res = resource.getParameters()
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
      
    self.assertEqual( 'storageName', res['StorageName'] )
    self.assertEqual( 'LHCbOnline' , res['ProtocolName'] )
    self.assertEqual( 'protocol'   , res['Protocol'] )
    self.assertEqual( 'path'       , res['Path'] )
    self.assertEqual( 'host'       , res['Host'] )
    self.assertEqual( 'port'       , res['Port'] )
    self.assertEqual( 'spaceToken' , res['SpaceToken'] )
    self.assertEqual( 'wspath'     , res['WSUrl'] )

  def test_getProtocolPfn( self ):
    ''' tests the output of getProtocolPfn
    '''
    
    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    
    res = resource.getProtocolPfn( { 'FileName' : 1 }, None )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( 1, res )
    
    res = resource.getProtocolPfn( { 'FileName' : 2, 'A' : 1 }, 123 )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( 2, res )

  def test___checkArgumentFormat( self ):
    ''' tests the outout of __checkArgumentFormat
    '''
    
    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    
    res = resource.__checkArgumentFormat( 'path' )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( [ 'path' ], res )
    
    res = resource.__checkArgumentFormat( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( [ 'path' ], res )
    
    res = resource.__checkArgumentFormat( [ 'path', 'path2' ] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( [ 'path', 'path2' ], res )
    
    res = resource.__checkArgumentFormat( {} )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( [], res )
    
    res = resource.__checkArgumentFormat( { 'A' : 1, 'B' : 2 } )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( [ 'A', 'B' ], res )
    
    res = resource.__checkArgumentFormat( 1 )
    self.assertEqual( False, res['OK'] )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF