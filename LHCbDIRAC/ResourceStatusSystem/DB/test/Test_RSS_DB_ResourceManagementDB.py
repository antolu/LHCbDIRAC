''' Test_RSS_DB_ResourceManagementDB
'''

import mock
import unittest

import LHCbDIRAC.ResourceStatusSystem.DB.ResourceManagementDB as moduleTested

__RCSID__ = '$Id: Test_RSS_DB_ResourceManagementDB.py 67124 2013-06-19 09:20:35Z ubeda $'

################################################################################

class ResourceManagementDB_TestCase( unittest.TestCase ):
 
  def setUp( self ):
    '''
    Setup
    '''
                  
    self.moduleTested = moduleTested
    self.testClass    = self.moduleTested.ResourceManagementDB

  def tearDown( self ):
    '''
    Tear down
    '''
   
    del self.moduleTested
    del self.testClass

################################################################################

class ResourceManagementDB_Success( ResourceManagementDB_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
   
    module = self.testClass( mySQL = 1 )
    self.assertEqual( 'ResourceManagementDB', module.__class__.__name__ )
    
  def test_tablesDB( self ):
    ''' tests the tables definition, making sure changes in the schema will not be
        unnoticed.
    '''
    
    module = self.testClass( mySQL = 1 )
    
    # ensure the tables are still there
    self.assertEquals( True, 'HammerCloudTest' in module._tablesDB )
    self.assertEquals( True, 'MonitoringTest' in module._tablesDB )
    self.assertEquals( True, 'JobAccountingCache' in module._tablesDB )  
    self.assertEquals( True, 'PilotAccountingCache' in module._tablesDB )
      
    self.assertEquals( True, 'SLST1Service' in module._tablesDB )
    self.assertEquals( True, 'SLSLogSE' in module._tablesDB )
    
  def test_tablesDB_HammerCloudTest( self ):
    ''' tests the table HammerCloudTest
    '''

    module = self.testClass( mySQL = 1 )
    table  = module._tablesDB[ 'HammerCloudTest' ]
    
    self.assertEquals( set( [ 'Fields', 'PrimaryKey' ] ), set( table.keys() ) )
    
    fields = [ 'TestID', 'SiteName', 'ResourceName', 'TestStatus', 'SubmissionTime',
               'StartTime', 'EndTime', 'CounterTime', 'AgentStatus', 'FormerAgentStatus',
               'Counter']
    self.assertEquals( set( fields ), set( table[ 'Fields' ].keys() ) )
    
    self.assertEquals( 'INT UNSIGNED', table[ 'Fields' ][ 'TestID' ] )
    self.assertEquals( 'VARCHAR(64) NOT NULL', table[ 'Fields' ][ 'SiteName' ] )
    self.assertEquals( 'VARCHAR(64) NOT NULL', table[ 'Fields' ][ 'ResourceName' ] )
    self.assertEquals( 'VARCHAR(16)', table[ 'Fields' ][ 'TestStatus' ] )
    self.assertEquals( 'DATETIME NOT NULL', table[ 'Fields' ][ 'SubmissionTime' ] )
    self.assertEquals( 'DATETIME', table[ 'Fields' ][ 'StartTime' ] )
    self.assertEquals( 'DATETIME', table[ 'Fields' ][ 'EndTime' ] )
    self.assertEquals( 'DATETIME', table[ 'Fields' ][ 'CounterTime' ] )
    self.assertEquals( 'VARCHAR(255) NOT NULL DEFAULT "Unspecified"', table[ 'Fields' ][ 'AgentStatus' ] )
    self.assertEquals( 'VARCHAR(255) NOT NULL DEFAULT "Unspecified"', table[ 'Fields' ][ 'FormerAgentStatus' ] )    
    self.assertEquals( 'INT NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Counter' ] )
    
    self.assertEquals( [ 'SubmissionTime' ], table[ 'PrimaryKey' ] )

  def test_tablesDB_MonitoringTest( self ):
    ''' test the table MonitoringTest
    '''
    
    module = self.testClass( mySQL = 1 )
    table  = module._tablesDB[ 'MonitoringTest' ]
    
    self.assertEquals( set( [ 'Fields', 'PrimaryKey' ] ), set( table.keys() ) )

    fields = [ 'MetricName', 'ServiceURI', 'SiteName', 'ServiceFlavour', 'MetricStatus',
               'SummaryData', 'Timestamp', 'LastCheckTime' ]
    self.assertEquals( set( fields ), set( table[ 'Fields' ].keys() ) )

    self.assertEquals( 'VARCHAR(128) NOT NULL', table[ 'Fields' ][ 'MetricName' ] )
    self.assertEquals( 'VARCHAR(128) NOT NULL', table[ 'Fields' ][ 'ServiceURI' ] )
    self.assertEquals( 'VARCHAR(64) NOT NULL', table[ 'Fields' ][ 'SiteName' ] )
    self.assertEquals( 'VARCHAR(64) NOT NULL', table[ 'Fields' ][ 'ServiceFlavour' ] )
    self.assertEquals( 'VARCHAR(512) NOT NULL', table[ 'Fields' ][ 'MetricStatus' ] )                       
    self.assertEquals( 'BLOB NOT NULL', table[ 'Fields' ][ 'SummaryData' ] )                           
    self.assertEquals( 'DATETIME NOT NULL', table[ 'Fields' ][ 'Timestamp' ] )
    self.assertEquals( 'DATETIME NOT NULL', table[ 'Fields' ][ 'LastCheckTime' ] )                           
    
    self.assertEquals( [ 'MetricName', 'ServiceURI' ], table[ 'PrimaryKey' ] )                  

  def test_tablesDB_JobAccountingCache( self ):
    ''' test the table JobAccountingCache
    '''
    
    module = self.testClass( mySQL = 1 )
    table  = module._tablesDB[ 'JobAccountingCache' ]
    
    self.assertEquals( set( [ 'Fields', 'PrimaryKey' ] ), set( table.keys() ) )
   
    fields = [ 'Name', 'Checking', 'Completed', 'Done', 'Failed', 'Killed',
               'Matched', 'Running', 'Stalled', 'LastCheckTime' ]
    self.assertEquals( set( fields ), set( table[ 'Fields' ].keys() ) )

    self.assertEquals( 'VARCHAR(64) NOT NULL', table[ 'Fields' ][ 'Name' ] )
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Checking' ] )
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Completed' ] )
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Done' ] )
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Failed' ] )
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Killed' ] )                           
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Matched' ] )
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Running' ] )
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Stalled' ] )
    self.assertEquals( 'DATETIME NOT NULL', table[ 'Fields' ][ 'LastCheckTime' ] )                           
    
    self.assertEquals( [ 'Name' ], table[ 'PrimaryKey' ] )  

  def test_tablesDB_PilotAccountingCache( self ):
    ''' test the table PilotAccountingCache
    '''
    
    module = self.testClass( mySQL = 1 )
    table  = module._tablesDB[ 'PilotAccountingCache' ]
    
    self.assertEquals( set( [ 'Fields', 'PrimaryKey' ] ), set( table.keys() ) )
   
    fields = [ 'Name', 'Aborted', 'Deleted', 'Done', 'Failed', 'LastCheckTime' ]
    self.assertEquals( set( fields ), set( table[ 'Fields' ].keys() ) )

    self.assertEquals( 'VARCHAR(64) NOT NULL', table[ 'Fields' ][ 'Name' ] )
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Aborted' ] )
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Deleted' ] )
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Done' ] )
    self.assertEquals( 'DOUBLE NOT NULL DEFAULT 0', table[ 'Fields' ][ 'Failed' ] )
    self.assertEquals( 'DATETIME NOT NULL', table[ 'Fields' ][ 'LastCheckTime' ] )                           
    
    self.assertEquals( [ 'Name' ], table[ 'PrimaryKey' ] )  
    
  def test_tablesDB_SLST1Service( self ):
    ''' test the table SLST1Service
    '''
    
    module = self.testClass( mySQL = 1 )
    table  = module._tablesDB[ 'SLST1Service' ]
    
    self.assertEquals( set( [ 'Fields', 'PrimaryKey' ] ), set( table.keys() ) )

    fields = [ 'Site', 'System', 'Availability', 'TimeStamp', 'Version', 'ServiceUptime',
               'HostUptime', 'Message' ]
    
    self.assertEquals( set( fields ), set( table[ 'Fields' ].keys() ) )

    self.assertEquals( 'VARCHAR(64) NOT NULL', table[ 'Fields' ][ 'Site' ] )
    self.assertEquals( 'VARCHAR(32) NOT NULL', table[ 'Fields' ][ 'System' ] )
    self.assertEquals( 'TINYINT UNSIGNED NOT NULL', table[ 'Fields' ][ 'Availability' ] )
    self.assertEquals( 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP', table[ 'Fields' ][ 'TimeStamp' ] )
    self.assertEquals( 'VARCHAR(32)', table[ 'Fields' ][ 'Version' ] )
    self.assertEquals( 'INT UNSIGNED', table[ 'Fields' ][ 'ServiceUptime' ] )
    self.assertEquals( 'INT UNSIGNED', table[ 'Fields' ][ 'HostUptime' ] )
    self.assertEquals( 'TEXT', table[ 'Fields' ][ 'Message' ] )

    self.assertEquals( [ 'Site', 'System' ], table[ 'PrimaryKey' ] )

  def test_tablesDB_SLSLogSE( self ):
    ''' test the table SLSLogSE
    '''
    
    module = self.testClass( mySQL = 1 )
    table  = module._tablesDB[ 'SLSLogSE' ]
    
    self.assertEquals( set( [ 'Fields', 'PrimaryKey' ] ), set( table.keys() ) )

    fields = [ 'Name', 'Availability', 'TimeStamp', 'ValidityDuration',
               'DataPartitionUsed', 'DataPartitionTotal' ]
    
    self.assertEquals( set( fields ), set( table[ 'Fields' ].keys() ) )

    self.assertEquals( 'VARCHAR(32)', table[ 'Fields' ][ 'Name' ] )
    self.assertEquals( 'TINYINT UNSIGNED NOT NULL', table[ 'Fields' ][ 'Availability' ] )
    self.assertEquals( 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP', table[ 'Fields' ][ 'TimeStamp' ] )
    self.assertEquals( 'VARCHAR(32) NOT NULL', table[ 'Fields' ][ 'ValidityDuration' ] )                       
    self.assertEquals( 'TINYINT UNSIGNED', table[ 'Fields' ][ 'DataPartitionUsed' ] )
    self.assertEquals( 'BIGINT UNSIGNED', table[ 'Fields' ][ 'DataPartitionTotal' ] )                                                

    self.assertEquals( [ 'Name' ], table[ 'PrimaryKey' ] )
            
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF