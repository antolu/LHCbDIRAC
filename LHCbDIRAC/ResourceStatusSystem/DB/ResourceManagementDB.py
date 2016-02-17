''' LHCbDIRAC.ResourceStatusSystem.DB.ResourceManagementDB

   ResourceManagementDB.__bases__:
     DIRAC.ResourceStatusSystem.DB.ResourceManagementDB.ResourceManagementDB

'''

from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import \
  ResourceManagementDB as DIRACResourceManagementDB
  
__RCSID__ = "$Id$"

class ResourceManagementDB( DIRACResourceManagementDB ):
  '''
   Module that extends basic methods to access the ResourceManagementDB.
  
    Extension of ResourceManagementDB, adding the following tables:
    - HammerCloudTest
    - MonitoringTest
    - SLST1Service
    - SLSLogSE
    - SLSStorage
  '''
  
  _tablesDB    = DIRACResourceManagementDB._tablesDB
  _tablesDB[ 'EnvironmentCache' ] = { 'Fields' : { 'HashKey'       : 'VARCHAR(64) NOT NULL',
                                                    'Environment'   : 'TEXT',
                                                    'SiteName'      : 'VARCHAR(64) NOT NULL',
                                                    'Arguments'     : 'VARCHAR(512) NOT NULL',
                                                    'DateEffective' : 'DATETIME NOT NULL',
                                                    'LastCheckTime' : 'DATETIME NOT NULL'},
                                       'PrimaryKey' : [ 'HashKey' ]}
  _tablesDB[ 'HammerCloudTest' ] = { 'Fields' : {'TestID'            : 'INT UNSIGNED',
                                                 'SiteName'          : 'VARCHAR(64) NOT NULL',
                                                 'ResourceName'      : 'VARCHAR(64) NOT NULL',
                                                 'TestStatus'        : 'VARCHAR(16)',
                                                 'SubmissionTime'    : 'DATETIME NOT NULL',
                                                 'StartTime'         : 'DATETIME',
                                                 'EndTime'           : 'DATETIME',
                                                 'CounterTime'       : 'DATETIME',
                                                 'AgentStatus'       : 'VARCHAR(255) NOT NULL DEFAULT "Unspecified"',
                                                 'FormerAgentStatus' : 'VARCHAR(255) NOT NULL DEFAULT "Unspecified"',
                                                 'Counter'           : 'INT NOT NULL DEFAULT 0' },
                                    'PrimaryKey' : [ 'SubmissionTime' ]}
  _tablesDB[ 'MonitoringTest' ] = { 'Fields' : { 'MetricName'     : 'VARCHAR(128) NOT NULL',
                                                'ServiceURI'     : 'VARCHAR(128) NOT NULL',
                                                'SiteName'       : 'VARCHAR(64) NOT NULL',
                                                'ServiceFlavour' : 'VARCHAR(64) NOT NULL',
                                                'MetricStatus'   : 'VARCHAR(512) NOT NULL',
                                                'SummaryData'    : 'BLOB NOT NULL',
                                                'Timestamp'      : 'DATETIME NOT NULL',
                                                'LastCheckTime'  : 'DATETIME NOT NULL'},
                                   'PrimaryKey' : [ 'MetricName', 'ServiceURI' ]}
  _tablesDB[ 'JobAccountingCache' ] = { 'Fields' : 
                     {'Name'          : 'VARCHAR(64) NOT NULL',
                       'Checking'      : 'DOUBLE NOT NULL DEFAULT 0',
                       'Completed'     : 'DOUBLE NOT NULL DEFAULT 0',
                       'Done'          : 'DOUBLE NOT NULL DEFAULT 0',
                       'Failed'        : 'DOUBLE NOT NULL DEFAULT 0',
                       'Killed'        : 'DOUBLE NOT NULL DEFAULT 0',
                       'Matched'       : 'DOUBLE NOT NULL DEFAULT 0',
                       'Running'       : 'DOUBLE NOT NULL DEFAULT 0',
                       'Stalled'       : 'DOUBLE NOT NULL DEFAULT 0',
                       'LastCheckTime' : 'DATETIME NOT NULL'},
                      'PrimaryKey' : [ 'Name' ]                                            
                                }
  
  _tablesDB[ 'PilotAccountingCache' ] = { 'Fields' : 
                     { 'Name'          : 'VARCHAR(64) NOT NULL',
                       'Aborted'       : 'DOUBLE NOT NULL DEFAULT 0',
                       'Deleted'       : 'DOUBLE NOT NULL DEFAULT 0',
                       'Done'          : 'DOUBLE NOT NULL DEFAULT 0',
                       'Failed'        : 'DOUBLE NOT NULL DEFAULT 0',
                       'LastCheckTime' : 'DATETIME NOT NULL'},
                      'PrimaryKey' : [ 'Name' ]                                            
                                }

  
  # TABLES THAT WILL EVENTUALLY BE DELETED
  
  _tablesDB[ 'SLST1Service' ] = { 'Fields' : 
                     { 'Site'          : 'VARCHAR(64) NOT NULL',
                       'System'        : 'VARCHAR(32) NOT NULL',
                       'Availability'  : 'TINYINT UNSIGNED NOT NULL',
                       'TimeStamp'     : 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                       'Version'       : 'VARCHAR(32)',
                       'ServiceUptime' : 'INT UNSIGNED',
                       'HostUptime'    : 'INT UNSIGNED',
                       'Message'       : 'TEXT' },
                      'PrimaryKey' : [ 'Site', 'System' ]                                            
                                }
  _tablesDB[ 'SLSLogSE' ] = { 'Fields' : 
                     { 'Name'               : 'VARCHAR(32)',
                       'Availability'       : 'TINYINT UNSIGNED NOT NULL',
                       'TimeStamp'          : 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                       'ValidityDuration'   : 'VARCHAR(32) NOT NULL',
                       'DataPartitionUsed'  : 'TINYINT UNSIGNED',
                       'DataPartitionTotal' : 'BIGINT UNSIGNED'},
                      'PrimaryKey' : [ 'Name' ]                                            
                                }

  
  #_tablesLike  = DIRACResourceManagementDB._tablesLike
  #_likeToTable = DIRACResourceManagementDB._likeToTable
  
#...............................................................................
#EOF
