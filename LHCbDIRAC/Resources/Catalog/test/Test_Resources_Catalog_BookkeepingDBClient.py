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
    mock_RPC.addFiles.return_value        = { 'OK' : True, 'Value' : { 'A' : 1 , 'B' : 2} }
    mock_RPC.removeFiles.return_value     = { 'OK' : True, 'Value' : { 'A' : 1 , 'B' : 2} }
    mock_RPC.exists.return_value          = { 'OK' : True, 'Value' : { 'A' : 1 , 'B' : 2} }
    mock_RPC.getFileMetadata.return_value = { 'OK' : True, 'Value' : { 'A' : 1 , 'B' : 2} }
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

  def test__exists(self):
    ''' tests the output of __exists
    '''    
    
    catalog = self.testClass()
    
    res = catalog._BookkeepingDBClient__exists( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__exists( ['A', 'B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {'A': 1, 'B': 2}, 'Failed' : {} }, res[ 'Value' ] )
        
    res = catalog._BookkeepingDBClient__exists( ['C'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {'A': 1, 'B': 2}, 'Failed' : {} }, res[ 'Value' ] )
    
    mock_RPC = mock.Mock()
    mock_RPC.exists.return_value = { 'OK' : False, 'Message' : 'Bo!' }
    
    self.moduleTested.RPCClient.return_value = mock_RPC
    
    catalog = self.testClass()

    res = catalog._BookkeepingDBClient__exists( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__exists( ['A'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__exists( ['A', 'B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__exists( ['C'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )

    mock_RPC = mock.Mock()
    mock_RPC.exists.side_effect = [ { 'OK' : True, 'Value' : { 'A' : 1 , 'B' : 2 } }, 
                                      { 'OK' : False, 'Message' : 'Bo!' } ]

    self.moduleTested.RPCClient.return_value = mock_RPC
    
    catalog = self.testClass()
    catalog.splitSize = 2
    
    res = catalog._BookkeepingDBClient__exists( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__exists( ['A','C','B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'A' : 1, 'B' : 2 }, 'Failed' : {} }, res[ 'Value' ] )
    
    # Restore the module
    self.moduleTested.RPCClient.return_value = self.mock_RPCClient
    reload( self.moduleTested )        
 
  def test__getFileMetadata(self):
    ''' tests the output of __getFileMetadata
    ''' 
    
    catalog = self.testClass()

    res = catalog._BookkeepingDBClient__getFileMetadata( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__getFileMetadata( ['A', 'B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {'A': 1, 'B': 2}, 'Failed' : {} }, res[ 'Value' ] )
        
    res = catalog._BookkeepingDBClient__getFileMetadata( ['C'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'C' : 'File does not exist'} }, res[ 'Value' ] )    
    
    mock_RPC = mock.Mock()
    mock_RPC.getFileMetadata.return_value = { 'OK' : True, 'Value' : { 'A' : '1' , 'B' : '2' } }
    
    self.moduleTested.RPCClient.return_value = mock_RPC
    
    catalog = self.testClass()    

    res = catalog._BookkeepingDBClient__getFileMetadata( ['A', 'B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {'A': '1', 'B': '2'}, 'Failed' : {} }, res[ 'Value' ] )
        
    res = catalog._BookkeepingDBClient__getFileMetadata( ['C'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'C' : 'File does not exist'} }, res[ 'Value' ] )    

    mock_RPC = mock.Mock()
    mock_RPC.getFileMetadata.return_value = { 'OK' : False, 'Message' : 'Bo!' }
    
    self.moduleTested.RPCClient.return_value = mock_RPC
    
    catalog = self.testClass()
    
    res = catalog._BookkeepingDBClient__getFileMetadata( ['A'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 'Bo!' } }, res[ 'Value' ] )
    
    res = catalog._BookkeepingDBClient__getFileMetadata( ['A', 'B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 'Bo!', 'B' : 'Bo!'} }, res[ 'Value' ] )
    
    mock_RPC = mock.Mock()
    mock_RPC.getFileMetadata.side_effect = [ { 'OK' : True, 'Value' : { 'A' : 1 , 'B' : 2 } }, 
                                             { 'OK' : False, 'Message' : 'Bo!' } ]

    self.moduleTested.RPCClient.return_value = mock_RPC
    
    catalog = self.testClass()
    catalog.splitSize = 2
    
    res = catalog._BookkeepingDBClient__getFileMetadata( ['A','C','B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'A' : 1 }, 'Failed' : { 'C' : 'File does not exist',
                                                                 'B' : 'Bo!'} }, res[ 'Value' ] )

    mock_RPC = mock.Mock()
    mock_RPC.getFileMetadata.return_value = { 'OK' : True, 'Value' : { 'A' : str , 'B' : '2' } }
    
    self.moduleTested.RPCClient.return_value = mock_RPC
    
    catalog = self.testClass()    

    res = catalog._BookkeepingDBClient__getFileMetadata( ['A', 'B'] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {'B': '2'}, 'Failed' : {'A':str} }, res[ 'Value' ] )
        
    # Restore the module
    self.moduleTested.RPCClient.return_value = self.mock_RPCClient
    reload( self.moduleTested )          

  def test_addFile(self):
    ''' tests the output of addFile
    '''
    
    catalog = self.testClass()
    
    res = catalog.addFile( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = catalog.addFile( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )

    res = catalog.addFile( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.addFile( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True }, 'Failed' : {} }, res['Value'] )

    res = catalog.addFile( [ 'A' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 1 } }, res['Value'] )    
    
    res = catalog.addFile( [ 'A', 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True }, 'Failed' : { 'A' : 1 } }, res['Value'] )    

  def test_addReplica(self):
    ''' tests the output of addReplica
    '''
    
    catalog = self.testClass()
    
    res = catalog.addReplica( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = catalog.addReplica( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )

    res = catalog.addReplica( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.addReplica( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True }, 'Failed' : {} }, res['Value'] )

    res = catalog.addReplica( [ 'A' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : { 'A' : 1 } }, res['Value'] )    
    
    res = catalog.addReplica( [ 'A', 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True }, 'Failed' : { 'A' : 1 } }, res['Value'] ) 

  def test_removeFile(self):
    ''' tests the output of removeFile
    '''
    
    catalog = self.testClass()
    
    res = catalog.removeFile( 1 )
    self.assertEqual( False, res['OK'] ) 
    
    res = catalog.removeFile( [] )
    self.assertEqual( True, res['OK'] )       
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.removeFile( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )

    res = catalog.removeFile( [ 'A', 'B', 'C' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {'A': 1, 'B': 2} }, res['Value'] )

    res = catalog.removeFile( {'A': 1, 'B': 2, 'C' : 3 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {'A': 1, 'B': 2} }, res['Value']  )

    #FIXME: to be continued, but the method is quite tangled to be tested 
    # on a rational way    

  def test_removeReplica(self):
    ''' tests the output of removeReplica
    '''
    
    catalog = self.testClass()

    res = catalog.removeReplica( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = catalog.removeReplica( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.removeReplica( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.removeReplica( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True }, 'Failed' : {} }, res['Value'] )

    res = catalog.removeReplica( [ 'A', 'B', 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True, 'A' :True, 'B' : True }, 'Failed' : {} }, res['Value'] ) 
    
    res = catalog.removeReplica( { 'A' : 2, 'C' : 3 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'A' :1, 'C' : True }, 'Failed' : {} }, res['Value'] )
    
  def test_setReplicaStatus(self):
    ''' tests the output of setReplicaStatus
    '''
    
    catalog = self.testClass()

    res = catalog.setReplicaStatus( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = catalog.setReplicaStatus( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.setReplicaStatus( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.setReplicaStatus( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True }, 'Failed' : {} }, res['Value'] )

    res = catalog.setReplicaStatus( [ 'A', 'B', 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True, 'A' :True, 'B' : True }, 'Failed' : {} }, res['Value'] ) 
    
    res = catalog.setReplicaStatus( { 'A' : 2, 'C' : 3 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'A' :1, 'C' : True }, 'Failed' : {} }, res['Value'] )
    
  def test_setReplicaHost(self):
    ''' tests the output of setReplicaHost
    '''
    
    catalog = self.testClass()

    res = catalog.setReplicaHost( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = catalog.setReplicaHost( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.setReplicaHost( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.setReplicaHost( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True }, 'Failed' : {} }, res['Value'] )

    res = catalog.setReplicaHost( [ 'A', 'B', 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True, 'A' :True, 'B' : True }, 'Failed' : {} }, res['Value'] ) 
    
    res = catalog.setReplicaHost( { 'A' : 2, 'C' : 3 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'A' :1, 'C' : True }, 'Failed' : {} }, res['Value'] )    

  def test_removeDirectory(self):
    ''' tests the output of removeDirectory
    '''
    
    catalog = self.testClass()

    res = catalog.removeDirectory( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = catalog.removeDirectory( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.removeDirectory( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.removeDirectory( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True }, 'Failed' : {} }, res['Value'] )

    res = catalog.removeDirectory( [ 'A', 'B', 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True, 'A' :True, 'B' : True }, 'Failed' : {} }, res['Value'] ) 
    
    res = catalog.removeDirectory( { 'A' : 2, 'C' : 3 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'A' :1, 'C' : True }, 'Failed' : {} }, res['Value'] )   

  def test_createDirectory(self):
    ''' tests the output of createDirectory
    '''
    
    catalog = self.testClass()

    res = catalog.createDirectory( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = catalog.createDirectory( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.createDirectory( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.createDirectory( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True }, 'Failed' : {} }, res['Value'] )

    res = catalog.createDirectory( [ 'A', 'B', 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True, 'A' :True, 'B' : True }, 'Failed' : {} }, res['Value'] ) 
    
    res = catalog.createDirectory( { 'A' : 2, 'C' : 3 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'A' :1, 'C' : True }, 'Failed' : {} }, res['Value'] )   

  def test_removeLink(self):
    ''' tests the output of createDirectory
    '''
    
    catalog = self.testClass()

    res = catalog.removeLink( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = catalog.removeLink( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.removeLink( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.removeLink( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True }, 'Failed' : {} }, res['Value'] )

    res = catalog.removeLink( [ 'A', 'B', 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True, 'A' :True, 'B' : True }, 'Failed' : {} }, res['Value'] ) 
    
    res = catalog.removeLink( { 'A' : 2, 'C' : 3 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'A' :1, 'C' : True }, 'Failed' : {} }, res['Value'] )   

  def test_createLink(self):
    ''' tests the output of createDirectory
    '''
    
    catalog = self.testClass()

    res = catalog.createLink( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = catalog.createLink( [] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.createLink( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {} }, res['Value'] )
    
    res = catalog.createLink( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True }, 'Failed' : {} }, res['Value'] )

    res = catalog.createLink( [ 'A', 'B', 'path' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'path' : True, 'A' :True, 'B' : True }, 'Failed' : {} }, res['Value'] ) 
    
    res = catalog.createLink( { 'A' : 2, 'C' : 3 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'A' :1, 'C' : True }, 'Failed' : {} }, res['Value'] )  
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF