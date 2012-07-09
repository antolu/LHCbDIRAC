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
    ''' tests the output of __checkArgumentFormat
    '''
    
    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    
    res = resource._LHCbOnlineStorage__checkArgumentFormat( 'path' )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( [ 'path' ], res )
    
    res = resource._LHCbOnlineStorage__checkArgumentFormat( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( [ 'path' ], res )
    
    res = resource._LHCbOnlineStorage__checkArgumentFormat( [ 'path', 'path2' ] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( [ 'path', 'path2' ], res )
    
    res = resource._LHCbOnlineStorage__checkArgumentFormat( {} )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( [], res )
    
    res = resource._LHCbOnlineStorage__checkArgumentFormat( { 'A' : 1, 'B' : 2 } )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( [ 'A', 'B' ], res )
    
    res = resource._LHCbOnlineStorage__checkArgumentFormat( 1 )
    self.assertEqual( False, res['OK'] )

  def test_getFileSize( self ):
    ''' tests the output of getFileSize 
    '''
    
    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    
    res = resource.getFileSize( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = resource.getFileSize( {} )
    self.assertEqual( False, res['OK'] )
    
    res = resource.getFileSize( [] )
    self.assertEqual( False, res['OK'] )
    
    res = resource.getFileSize( [ 'A', 'B' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {'A':0,'B':0}, res['Value']['Successful'] )    
    self.assertEqual( {}, res['Value']['Failed'] )
    
    res = resource.getFileSize( { 'A' : 1, 'B' : {}} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {'A':0,'B':0}, res['Value']['Successful'] )
    self.assertEqual( {}, res['Value']['Failed'] )
    
  def test_removeFile( self ):
    ''' tests the output of removeFile
    '''    
    
    resource = self.testClass( 'storageName', 'protocol', 'path', 'host', 'port', 'spaceToken', 'wspath' )
    resource.server = mock.Mock()
    resource.server.endMigratingFile.return_value = [ ( 1, 0 ) ] 
    
    res = resource.removeFile( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = resource.removeFile( {} )
    self.assertEqual( False, res['OK'] )
    
    res = resource.removeFile( [] )
    self.assertEqual( False, res['OK'] )

    res = resource.removeFile( [ 'A', 'B' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {'A':True,'B':True}, res['Value']['Successful'] )    
    self.assertEqual( {}, res['Value']['Failed'] )

    resource.server.endMigratingFile.side_effect = [ (1, 0), (0, 1) ]

    res = resource.removeFile( [ 'A', 'B' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {'A':True}, res['Value']['Successful'] )    
    self.assertEqual( ['B'], res['Value']['Failed'].keys() )

    resource.server.endMigratingFile.side_effect = Exception('Boom!')
    res = resource.removeFile( [ 'A', 'B' ] )
    #FIXME: This should return S_ERROR !!    
    self.assertEqual( True, res['OK'] )
    self.assertEqual( {}, res['Value']['Successful'] )    
    self.assertEqual( {}, res['Value']['Failed'] )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF