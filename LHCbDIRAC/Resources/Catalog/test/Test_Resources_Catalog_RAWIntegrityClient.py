# $HeadURL: $
''' Test_Resources_Catalog_RAWIntegrityClient
'''

import mock
import unittest

import LHCbDIRAC.Resources.Catalog.RAWIntegrityClient as moduleTested 

__RCSID__ = '$Id: $'

################################################################################

class RAWIntegrityClient_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
    
    # Mock external libraries / modules not interesting for the unit test
    mock_pathFinder = mock.Mock()
    mock_pathFinder.getServiceURL.return_value = 'cookiesURL' 
    self.mock_pathFinder = mock_pathFinder
    
    mock_RPC = mock.Mock()
    mock_RPC.addFile.return_value    = { 'OK' : True }
#    mock_RPC.addMigratingReplicas.return_value    = { 'OK' : True }
#    mock_RPC.removeMigratingFiles.return_value    = { 'OK' : True }
#    mock_RPC.removeMigratingReplicas.return_value = { 'OK' : True }
#    
    mock_RPCClient              = mock.Mock()
    mock_RPCClient.return_value = mock_RPC
    self.mock_RPCClient         = mock_RPCClient
    
    # Add mocks to moduleTested
    moduleTested.PathFinder = self.mock_pathFinder
    moduleTested.RPCClient  = self.mock_RPCClient
    
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.RAWIntegrityClient
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass

################################################################################

class RAWIntegrityClient_Success( RAWIntegrityClient_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    catalog = self.testClass()
    self.assertEqual( 'RAWIntegrityClient', catalog.__class__.__name__ )

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
    
    res = catalog._RAWIntegrityClient__checkArgumentFormat( 'path' )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'path' : False }, res )

    res = catalog._RAWIntegrityClient__checkArgumentFormat( [] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( {}, res )
    
    res = catalog._RAWIntegrityClient__checkArgumentFormat( [ 'path' ] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'path' : False }, res )
    
    res = catalog._RAWIntegrityClient__checkArgumentFormat( [ 'path', 'path2' ] )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'path' : False, 'path2' : False }, res )
    
    res = catalog._RAWIntegrityClient__checkArgumentFormat( {} )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( {}, res )
    
    res = catalog._RAWIntegrityClient__checkArgumentFormat( { 'A' : 1, 'B' : 2 } )
    self.assertEqual( True, res['OK'] )
    res = res[ 'Value' ]
    self.assertEqual( { 'A' : 1, 'B' : 2 }, res )
    
    res = catalog._RAWIntegrityClient__checkArgumentFormat( 1 )
    self.assertEqual( False, res['OK'] )            

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
    mock_RPC.addFile.return_value = { 'OK' : False, 'Message' : 'Bo!' }

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

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF