# $HeadURL: $
''' Test_Resources_Catalog_BookkeepingDBClient
'''

import mock
import unittest

import LHCbDIRAC.Resources.Catalog.BookkeepingDBClient as moduleTested 

__RCSID__ = '$Id: $'

################################################################################

class BookkeepingDBClientt_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
    
#    # Mock external libraries / modules not interesting for the unit test
#    mock_pathFinder = mock.Mock()
#    mock_pathFinder.getServiceURL.return_value = 'cookiesURL' 
#    self.mock_pathFinder = mock_pathFinder
#    
    mock_RPC = mock.Mock()
    mock_RPC.addFiles.return_value    = { 'OK' : True, 'Value' : { 'A' : 1 , 'B' : 2} }
    mock_RPC.removeFiles.return_value = { 'OK' : True, 'Value' : { 'A' : 1 , 'B' : 2} }
#    mock_RPC.removeMigratingFiles.return_value    = { 'OK' : True }
#    mock_RPC.removeMigratingReplicas.return_value = { 'OK' : True }
    
    mock_RPCClient              = mock.Mock()
    mock_RPCClient.return_value = mock_RPC
    self.mock_RPCClient         = mock_RPCClient
#    
#    # Add mocks to moduleTested
#    moduleTested.PathFinder = self.mock_pathFinder
    moduleTested.RPCClient  = self.mock_RPCClient
    
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.BookkeepingDBClient
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass

################################################################################

class BookkeepingDBClient_Success( BookkeepingDBClientt_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    catalog = self.testClass()
    self.assertEqual( 'BookkeepingDBClient', catalog.__class__.__name__ )

  def test_init(self):
    ''' tests that the init method does what it should do
    '''
    
    catalog = self.testClass()
    
    self.assertEqual( 1000, catalog.splitSize )
    self.assertEqual( 'BookkeepingDB', catalog.name )
    self.assertEqual( True, catalog.valid )
    self.assertEqual( 'Bookkeeping/BookkeepingManager', catalog.url )
    
    catalog = self.testClass( url = 'URLTest' )
    self.assertEqual( 1000, catalog.splitSize )
    self.assertEqual( 'BookkeepingDB', catalog.name )
    self.assertEqual( True, catalog.valid )
    self.assertEqual( 'URLTest', catalog.url )
    
  def test_isOK(self):
    ''' tests output of isOK method
    '''  
    catalog = self.testClass()
    self.assertEqual( True, catalog.valid )
    
    res = catalog.isOK()
    self.assertEqual( True, res )
    
    catalog.valid = 'Banzai !'
    res = catalog.isOK()
    self.assertEqual( 'Banzai !', res )    

  def test___checkArgumentFormat( self ):
    ''' tests the output of __checkArgumentFormat
    '''
    
    catalog = self.testClass()
    
    res = catalog._BookkeepingDBClient__checkArgumentFormat( 'path' )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'path' : False }, res )

    res = catalog._BookkeepingDBClient__checkArgumentFormat( [] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( {}, res )
    
    res = catalog._BookkeepingDBClient__checkArgumentFormat( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'path' : False }, res )
    
    res = catalog._BookkeepingDBClient__checkArgumentFormat( [ 'path', 'path2' ] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'path' : False, 'path2' : False }, res )
    
    res = catalog._BookkeepingDBClient__checkArgumentFormat( {} )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( {}, res )
    
    res = catalog._BookkeepingDBClient__checkArgumentFormat( { 'A' : 1, 'B' : 2 } )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'A' : 1, 'B' : 2 }, res )
    
    res = catalog._BookkeepingDBClient__checkArgumentFormat( 1 )
    self.assertEqual( False, res['OK'] )   

  def test__setHasReplicaFlag(self):
    ''' test the output of __setHasReplicaFlag
    '''
    
    catalog = self.testClass()    
    
    res = catalog._BookkeepingDBClient__setHasReplicaFlag( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__setHasReplicaFlag( ['A'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 1 } }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__setHasReplicaFlag( ['A', 'B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 1, 'B' : 2 } }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__setHasReplicaFlag( ['C'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'C' : True }, 'Failed' : {} }, res[ 'Value' ] )
    
    mock_RPC = mock.Mock()
    mock_RPC.addFiles.return_value = { 'OK' : False, 'Message' : 'Bo!' }

    self.moduleTested.RPCClient.return_value = mock_RPC
    
    catalog = self.testClass()
    
    res = catalog._BookkeepingDBClient__setHasReplicaFlag( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__setHasReplicaFlag( ['A'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 'Bo!' } }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__setHasReplicaFlag( ['A', 'B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 'Bo!', 'B' : 'Bo!' } }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__setHasReplicaFlag( ['C'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'C' : 'Bo!' } }, res[ 'Value' ] )
    
    mock_RPC = mock.Mock()
    mock_RPC.addFiles.side_effect = [ { 'OK' : True, 'Value' : { 'A' : 1 , 'B' : 2 } }, 
                                      { 'OK' : False, 'Message' : 'Bo!' } ]

    self.moduleTested.RPCClient.return_value = mock_RPC
    
    catalog = self.testClass()
    catalog.splitSize = 2
    
    res = catalog._BookkeepingDBClient__setHasReplicaFlag( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__setHasReplicaFlag( ['A','C','B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'C' : True }, 'Failed' : { 'A' : 1, 'B' : 'Bo!' } }, res[ 'Value' ] )
    
    # Restore the module
    self.moduleTested.RPCClient.return_value = self.mock_RPCClient
    reload( self.moduleTested )    

  def test__unsetHasReplicaFlag(self):
    ''' test the output of __unsetHasReplicaFlag
    '''
    
    catalog = self.testClass()

    res = catalog._BookkeepingDBClient__unsetHasReplicaFlag( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__unsetHasReplicaFlag( ['A'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 1 } }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__unsetHasReplicaFlag( ['A', 'B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 1, 'B' : 2 } }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__unsetHasReplicaFlag( ['C'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'C' : True }, 'Failed' : {} }, res[ 'Value' ] )
    
    mock_RPC = mock.Mock()
    mock_RPC.removeFiles.return_value = { 'OK' : False, 'Message' : 'Bo!' }

    self.moduleTested.RPCClient.return_value = mock_RPC
    
    catalog = self.testClass()
    
    res = catalog._BookkeepingDBClient__unsetHasReplicaFlag( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__unsetHasReplicaFlag( ['A'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 'Bo!' } }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__unsetHasReplicaFlag( ['A', 'B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 'Bo!', 'B' : 'Bo!' } }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__unsetHasReplicaFlag( ['C'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'C' : 'Bo!' } }, res[ 'Value' ] )

    mock_RPC = mock.Mock()
    mock_RPC.removeFiles.side_effect = [ { 'OK' : True, 'Value' : { 'A' : 1 , 'B' : 2 } }, 
                                      { 'OK' : False, 'Message' : 'Bo!' } ]

    self.moduleTested.RPCClient.return_value = mock_RPC
    
    catalog = self.testClass()
    catalog.splitSize = 2
    
    res = catalog._BookkeepingDBClient__unsetHasReplicaFlag( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__unsetHasReplicaFlag( ['A','C','B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'C' : True }, 'Failed' : { 'A' : 1, 'B' : 'Bo!' } }, res[ 'Value' ] )
    
    # Restore the module
    self.moduleTested.RPCClient.return_value = self.mock_RPCClient
    reload( self.moduleTested )    
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF