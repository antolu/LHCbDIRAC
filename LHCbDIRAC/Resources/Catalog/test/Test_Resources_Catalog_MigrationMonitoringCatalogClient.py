# $HeadURL: $
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
    
    # Add mocks to moduleTested
    moduleTested.PathFinder = mock_pathFinder
    
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.MigrationMonitoringCatalogClient
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.testClass

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
    self.moduleTested.PathFinder.getServiceURL.return_value = 'cookiesURL'
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
    self.assertEqual( { 'Failed' : {}, 'Successful' : {} }, res['Value'] )
    
    res = catalog.exists( { 'A' : 1, 'B' : 2 } )
    self.assertEqual( True, res['OK'] )
    self.assertEqual( { 'Failed' : {}, 'Successful' : { 'A' : True, 'B' : True} }, res['Value'] )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF