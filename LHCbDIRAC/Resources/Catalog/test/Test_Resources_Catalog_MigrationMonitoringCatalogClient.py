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
    
    self.moduleTested.PathFinder.return_value = Exception( 'Boom!' )
    #catalog = self.testClass()
    self.assertEqual( False, catalog.valid )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF