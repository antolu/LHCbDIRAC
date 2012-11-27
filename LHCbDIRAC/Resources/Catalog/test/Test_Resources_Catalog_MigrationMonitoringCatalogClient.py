''' Test_Resources_Catalog_MigrationMonitoringCatalogClient
'''

import mock
import unittest

import LHCbDIRAC.Resources.Catalog.MigrationMonitoringCatalogClient as moduleTested 

__RCSID__ = '$Id: $'

################################################################################

class MigrationMonitoringCatalogClient_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
    
    # Mock external libraries / modules not interesting for the unit test
    mock_pathFinder = mock.Mock()
    mock_pathFinder.getServiceURL.return_value = 'cookiesURL' 
    self.mock_pathFinder = mock_pathFinder
    
    mock_RPC = mock.Mock()
    mock_RPC.addMigratingReplicas.return_value    = { 'OK' : True }
    mock_RPC.removeMigratingFiles.return_value    = { 'OK' : True }
    mock_RPC.removeMigratingReplicas.return_value = { 'OK' : True }
    
    mock_RPCClient              = mock.Mock()
    mock_RPCClient.return_value = mock_RPC
    self.mock_RPCClient         = mock_RPCClient
    
    # Add mocks to moduleTested
    moduleTested.PathFinder = self.mock_pathFinder
    moduleTested.RPCClient  = self.mock_RPCClient
    
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.MigrationMonitoringCatalogClient
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass
    del self.moduleTested
    del self.mock_pathFinder
    del self.mock_RPCClient

################################################################################
# Tests

class MigrationMonitoringCatalogClient_Success( MigrationMonitoringCatalogClient_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    catalog = self.testClass()
    self.assertEqual( 'MigrationMonitoringCatalogClient', catalog.__class__.__name__ )
  
  def test_init(self):
    ''' tests that the init method does what it should do
    '''

    catalog = self.testClass()
    self.assertEqual( True, catalog.valid )
    self.assertEqual( 'cookiesURL', catalog.url )

    # We are altering one of the module members, we have to reload the whole module..
    self.moduleTested.PathFinder.getServiceURL.return_value = Exception( 'Boom!' )
    reload( self.moduleTested )

    catalog = self.testClass()
    self.assertEqual( False, catalog.valid )

    # Restore the module
    self.moduleTested.PathFinder = self.mock_pathFinder
    reload( self.moduleTested )
    
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
    
    res = catalog._MigrationMonitoringCatalogClient__checkArgumentFormat( 'path' )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'path' : False }, res )

    res = catalog._MigrationMonitoringCatalogClient__checkArgumentFormat( [] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( {}, res )
    
    res = catalog._MigrationMonitoringCatalogClient__checkArgumentFormat( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'path' : False }, res )
    
    res = catalog._MigrationMonitoringCatalogClient__checkArgumentFormat( [ 'path', 'path2' ] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'path' : False, 'path2' : False }, res )
    
    res = catalog._MigrationMonitoringCatalogClient__checkArgumentFormat( {} )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( {}, res )
    
    res = catalog._MigrationMonitoringCatalogClient__checkArgumentFormat( { 'A' : 1, 'B' : 2 } )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'A' : 1, 'B' : 2 }, res )
    
    res = catalog._MigrationMonitoringCatalogClient__checkArgumentFormat( 1 )
    self.assertEqual( False, res['OK'] )    

  def test_exists(self):
    ''' tests the output of exists
    '''
    
    catalog = self.testClass()
    
    res = catalog.exists( 1 )
    self.assertEqual( False, res['OK'] )
    
    res = catalog.exists( {} )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Failed' : {}, 'Successful' : {} }, res['Value'] )
    
    res = catalog.exists( [ 'path1' ] )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Failed' : {}, 'Successful' : { 'path1' : False } }, res['Value'] )
    
    res = catalog.exists( { 'A' : 1, 'B' : 2 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Failed' : {}, 'Successful' : { 'A' : False, 'B' : False} }, res['Value'] )
    
  def test_addFile(self):
    ''' tests the output of addFile
    '''  
    
    catalog = self.testClass()
    
    res = catalog.addFile( 1 )
    self.assertEqual( False, res['OK'] )
    
    self.assertRaises( TypeError, catalog.addFile, '' )
    self.assertRaises( TypeError, catalog.addFile, [ '' ] )        

    fileDict = { 
                 'PFN'      : 'pfn',
                 'Size'     : '10',
                 'SE'       : 'se',
                 'GUID'     : 'guid',
                 'Checksum' : 'checksum'
               }
            
    self.assertRaises( TypeError, catalog.addFile, '' )            
            
    fileDict[ 'Size' ]  = '10' 
                     
    res = catalog.addFile( { 'lfn1' : fileDict } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'lfn1' : True }, 'Failed' : {} }, res['Value'] )
    
    res = catalog.addFile( { 'lfn1' : fileDict, 'lfn2' : fileDict } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'lfn1' : True, 'lfn2' : True }, 'Failed' : {} }, res['Value'] )
    
    mock_RPC = mock.Mock()
    mock_RPC.addMigratingReplicas.return_value = { 'OK' : False, 'Message' : 'Bo!' }

    self.moduleTested.RPCClient.return_value = mock_RPC
    catalog = self.testClass()
    
    res = catalog.addFile( { 'lfn1' : fileDict } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {'lfn1' : 'Bo!' } }, res['Value'] )
    
    res = catalog.addFile( { 'lfn1' : fileDict, 'lfn2' : fileDict } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {  'lfn1' : 'Bo!', 'lfn2' : 'Bo!' } }, res['Value'] )
 
    # Restore the module
    self.moduleTested.RPCClient.return_value = self.mock_RPCClient
    reload( self.moduleTested )

  def test_removeFile(self):
    ''' tests the output of removeFile
    '''  
    
    catalog = self.testClass()
    
    res = catalog.removeFile( 1 )
    self.assertEqual( False, res['OK'] )    
    
    res = catalog.removeFile( { 'lfn1' : 1 } )
    self.assertEqual( { 'Successful' : { 'lfn1' : True }, 'Failed' : {} }, res['Value'] )
    
    res = catalog.removeFile( { 'lfn1' : 1, 'lfn2' : 2 } )
    self.assertEqual( { 'Successful' : { 'lfn1' : True, 'lfn2' : True }, 'Failed' : {} }, res['Value'] )
    
    mock_RPC = mock.Mock()
    mock_RPC.removeMigratingFiles.return_value = { 'OK' : False, 'Message' : 'Bo!' }    

    self.moduleTested.RPCClient.return_value = mock_RPC
    catalog = self.testClass()
    
    res = catalog.removeFile( { 'lfn1' : 1 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {'lfn1' : 'Bo!' } }, res['Value'] )
    
    res = catalog.removeFile( { 'lfn1' : 1, 'lfn2' : 2 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {  'lfn1' : 'Bo!', 'lfn2' : 'Bo!' } }, res['Value'] )
 
    # Restore the module
    self.moduleTested.RPCClient.return_value = self.mock_RPCClient
    reload( self.moduleTested )

  def test_addReplica(self):
    ''' tests the output of addReplica
    '''
    
    catalog = self.testClass()
    
    res = catalog.addReplica( 1 )
    self.assertEqual( False, res['OK'] ) 

    self.assertRaises( TypeError, catalog.addReplica, '' )
    self.assertRaises( TypeError, catalog.addReplica, [ '' ] )   

    fileDict = { 
                 'PFN'      : 'pfn',
                 'SE'       : 'se',
               }
    
    res = catalog.addReplica( { 'lfn1' : fileDict } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'lfn1' : True }, 'Failed' : {} }, res['Value'] )
    
    res = catalog.addReplica( { 'lfn1' : fileDict, 'lfn2' : fileDict } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : { 'lfn1' : True, 'lfn2' : True }, 'Failed' : {} }, res['Value'] )
    
    mock_RPC = mock.Mock()
    mock_RPC.addMigratingReplicas.return_value = { 'OK' : False, 'Message' : 'Bo!' }

    self.moduleTested.RPCClient.return_value = mock_RPC
    catalog = self.testClass()
    
    res = catalog.addReplica( { 'lfn1' : fileDict } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {'lfn1' : 'Bo!' } }, res['Value'] )
    
    res = catalog.addReplica( { 'lfn1' : fileDict, 'lfn2' : fileDict } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Successful' : {}, 'Failed' : {  'lfn1' : 'Bo!', 'lfn2' : 'Bo!' } }, res['Value'] )

    # Restore the module
    self.moduleTested.RPCClient.return_value = self.mock_RPCClient
    reload( self.moduleTested )

  def test_removeReplica(self):
    ''' tests the output of removeReplica
    '''
    
    catalog = self.testClass()

    res = catalog.removeReplica( 1 )
    self.assertEqual( False, res['OK'] ) 

    fileDict = { 
                 'PFN'      : 'pfn',
                 'SE'       : 'se',
               }

    res = catalog.removeReplica( { 'lfn1' : fileDict } )
    self.assertEqual( True, res['OK'] )

    res = catalog.removeReplica( { 'lfn1' : fileDict, 'lfn2' : fileDict } )
    self.assertEqual( True, res['OK'] )
    
    mock_RPC = mock.Mock()
    mock_RPC.removeMigratingReplicas.return_value = { 'OK' : False, 'Message' : 'Bo!' }    

    self.moduleTested.RPCClient.return_value = mock_RPC
    catalog = self.testClass()

    res = catalog.removeReplica( { 'lfn1' : fileDict } )
    self.assertEqual( { 'OK' : False, 'Message' : 'Bo!' }, res )

    res = catalog.removeReplica( { 'lfn1' : fileDict, 'lfn2' : fileDict } )
    self.assertEqual( { 'OK' : False, 'Message' : 'Bo!' }, res )

    # Restore the module
    self.moduleTested.RPCClient.return_value = self.mock_RPCClient
    reload( self.moduleTested )

  def test___dummyMethod(self):
    ''' test the output of __dummyMethod
    '''
    
    catalog = self.testClass()
    
    res = catalog._MigrationMonitoringCatalogClient__dummyMethod( 1 )
    self.assertEqual( False, res[ 'OK' ] )

    res = catalog._MigrationMonitoringCatalogClient__dummyMethod( [] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog._MigrationMonitoringCatalogClient__dummyMethod( [ 'A', 'B' ] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': { 'A' : True, 'B' : True}, 'Failed' : {} }, res[ 'Value' ] )
    
  def test_setReplicaStatus(self):
    ''' test the output of setReplicaStatus
    '''

    catalog = self.testClass()
    
    res = catalog.setReplicaStatus( 1 )
    self.assertEqual( False, res[ 'OK' ] )

    res = catalog.setReplicaStatus( [] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog.setReplicaStatus( [ 'A', 'B' ] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': { 'A' : True, 'B' : True}, 'Failed' : {} }, res[ 'Value' ] )

  def test_setReplicaHost(self):
    ''' test the output of setReplicaHost
    '''

    catalog = self.testClass()
    
    res = catalog.setReplicaHost( 1 )
    self.assertEqual( False, res[ 'OK' ] )

    res = catalog.setReplicaHost( [] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog.setReplicaHost( [ 'A', 'B' ] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': { 'A' : True, 'B' : True}, 'Failed' : {} }, res[ 'Value' ] )

  def test_removeDirectory(self):
    ''' test the output of removeDirectory
    '''
    
    catalog = self.testClass()
    
    res = catalog.removeDirectory( 1 )
    self.assertEqual( False, res[ 'OK' ] )

    res = catalog.removeDirectory( [] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog.removeDirectory( [ 'A', 'B' ] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': { 'A' : True, 'B' : True}, 'Failed' : {} }, res[ 'Value' ] )

  def test_createDirectory(self):
    ''' test the output of createDirectory
    '''
    
    catalog = self.testClass()
    
    res = catalog.createDirectory( 1 )
    self.assertEqual( False, res[ 'OK' ] )

    res = catalog.createDirectory( [] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog.createDirectory( [ 'A', 'B' ] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': { 'A' : True, 'B' : True}, 'Failed' : {} }, res[ 'Value' ] )

  def test_removeLink(self):
    ''' test the output of removeLink
    '''
    
    catalog = self.testClass()
    
    res = catalog.removeLink( 1 )
    self.assertEqual( False, res[ 'OK' ] )

    res = catalog.removeLink( [] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog.removeLink( [ 'A', 'B' ] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': { 'A' : True, 'B' : True}, 'Failed' : {} }, res[ 'Value' ] )

  def test_createLink(self):
    ''' test the output of createLink
    '''
    
    catalog = self.testClass()
    
    res = catalog.createLink( 1 )
    self.assertEqual( False, res[ 'OK' ] )

    res = catalog.createLink( [] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': {}, 'Failed' : {} }, res[ 'Value' ] )
    
    res = catalog.createLink( [ 'A', 'B' ] )
    self.assertEqual( True, res[ 'OK' ] )
    self.assertEqual( { 'Successful': { 'A' : True, 'B' : True}, 'Failed' : {} }, res[ 'Value' ] )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF