''' Test_RSS_Client_ResourceManagementClient
'''

import mock
import unittest

import LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient as moduleTested

__RCSID__ = "$Id$"

################################################################################

class ResourceManagementClient_TestCase( unittest.TestCase ):
 
  def setUp( self ):
    '''
    Setup
    '''
                  
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.ResourceManagementClient

  def tearDown( self ):
    '''
    Tear down
    '''
   
    del self.moduleTested
    del self.testClass

################################################################################

class ResourceManagementClient_Success( ResourceManagementClient_TestCase ):
 
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
   
    module = self.testClass()
    self.assertEqual( 'ResourceManagementClient', module.__class__.__name__ )

  def test_init( self ):
    ''' test the __init__ method
    '''

    module = self.testClass( True )
    self.assertEqual( True, module.gate )

  def test_selectMonitoringTest( self ):
    ''' tests the method selectMonitorintTest
    '''
    
    gate                     = mock.Mock()
    gate.select.return_value = { 'OK' : True, 'Value' : 42 }
    
    module = self.testClass( gate )
    
    res = module.selectMonitoringTest()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 42, res[ 'Value' ] )
    
    res = module.selectMonitoringTest( metricName = 1, serviceURI = 2, 
                                       siteName = 3, serviceFlavour = 4,
                                       metricStatus = 5, summaryData = 6,
                                       timestamp = 7, lastCheckTime = 8, 
                                       meta = {} )    
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 42, res[ 'Value' ] )   
    
  def test_deleteMonitoringTest( self ):
    ''' tests the method deleteMonitoringTest
    '''
    
    gate                     = mock.Mock()
    gate.delete.return_value = { 'OK' : True, 'Value' : 43 }
    
    module = self.testClass( gate )
    
    res = module.deleteMonitoringTest()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 43, res[ 'Value' ] )
    
    res = module.deleteMonitoringTest( metricName = 1, serviceURI = 2, 
                                       siteName = 3, serviceFlavour = 4,
                                       metricStatus = 5, summaryData = 6,
                                       timestamp = 7, lastCheckTime = 8, 
                                       meta = {} )    
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 43, res[ 'Value' ] )  

  def test_addOrModifyMonitoringTest( self ):
    ''' tests the method addOrModifyMonitoringTest
    '''
    
    gate                          = mock.Mock()
    gate.addOrModify.return_value = { 'OK' : True, 'Value' : 44 }
    
    module = self.testClass( gate )
    self.assertRaises( TypeError, module.addOrModifyMonitoringTest )
    
    res = module.addOrModifyMonitoringTest( 1,2,3,4,5,6,7,8 )    
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 44, res[ 'Value' ] )  

  def test_selectJobAccountingCache( self ):
    ''' tests the method selectJobAccountingCache
    '''
    
    gate                     = mock.Mock()
    gate.select.return_value = { 'OK' : True, 'Value' : 42 }
    
    module = self.testClass( gate )
    
    res = module.selectJobAccountingCache()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 42, res[ 'Value' ] )
    
    res = module.selectJobAccountingCache( name = 1, checking = 2, completed = 3, 
                                           done = 4, failed = 5, killed = 6, matched = 7, 
                                           running = 8, stalled  = 9, lastCheckTime = 10, 
                                           meta = {} )    
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 42, res[ 'Value' ] )   

  def test_deleteJobAccountingCache( self ):
    ''' tests the method deleteJobAccountingCache
    '''
    
    gate                     = mock.Mock()
    gate.delete.return_value = { 'OK' : True, 'Value' : 43 }
    
    module = self.testClass( gate )
    
    res = module.deleteJobAccountingCache()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 43, res[ 'Value' ] )
    
    res = module.deleteJobAccountingCache( name = 1, checking = 2, completed = 3, 
                                           done = 4, failed = 5, killed = 6, matched = 7, 
                                           running = 8, stalled  = 9, lastCheckTime = 10, 
                                           meta = {} )    
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 43, res[ 'Value' ] )  

  def test_addOrModifyJobAccountingCache( self ):
    ''' tests the method addOrModifyJobAccountingCache
    '''
    
    gate                          = mock.Mock()
    gate.addOrModify.return_value = { 'OK' : True, 'Value' : 44 }
    
    module = self.testClass( gate )
#    self.assertRaises( TypeError, module.addOrModifyJobAccountingCache )
    
    res = module.addOrModifyJobAccountingCache( 1,2,3,4,5,6,7,8,9,10 )    
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 44, res[ 'Value' ] )  

  def test_selectPilotAccountingCache( self ):
    ''' tests the method selectPilotAccountingCache
    '''
    
    gate                     = mock.Mock()
    gate.select.return_value = { 'OK' : True, 'Value' : 42 }
    
    module = self.testClass( gate )
    
    res = module.selectPilotAccountingCache()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 42, res[ 'Value' ] )
    
    res = module.selectPilotAccountingCache( name = 1, aborted = 2, deleted = 3, 
                                             done = 4, failed = 5, lastCheckTime = 6, 
                                             meta = {} )    
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 42, res[ 'Value' ] )   

  def test_deletePilotAccountingCache( self ):
    ''' tests the method deletePilotAccountingCache
    '''
    
    gate                     = mock.Mock()
    gate.delete.return_value = { 'OK' : True, 'Value' : 43 }
    
    module = self.testClass( gate )
    
    res = module.deletePilotAccountingCache()
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 43, res[ 'Value' ] )
    
    res = module.deletePilotAccountingCache( name = 1, aborted = 2, deleted = 3, 
                                             done = 4, failed = 5, lastCheckTime = 6, 
                                             meta = {} )    
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 43, res[ 'Value' ] )  

  def test_addOrModifyPilotAccountingCache( self ):
    ''' tests the method addOrModifyPilotAccountingCache
    '''
    
    gate                          = mock.Mock()
    gate.addOrModify.return_value = { 'OK' : True, 'Value' : 44 }
    
    module = self.testClass( gate )
#    self.assertRaises( TypeError, module.addOrModifyJobAccountingCache )
    
    res = module.addOrModifyPilotAccountingCache( 1, 2, 3, 4, 5, 6 )    
    self.assertEquals( True, res[ 'OK' ] )
    self.assertEquals( 44, res[ 'Value' ] )  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF