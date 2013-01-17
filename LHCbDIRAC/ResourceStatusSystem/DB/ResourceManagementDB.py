# $HeadURL:  $
''' ResourceManagementDB module, extension of the DIRAC one.

  Module that extends basic methods to access the ResourceManagementDB.

'''

from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import \
  ResourceManagementDB as DIRACResourceManagementDB
  
__RCSID__ = '$Id:  $'

class ResourceManagementDB( DIRACResourceManagementDB ):
  '''
    Extension of ResourceManagementDB, adding the following tables:
    - HammerCloudTest
    - MonitoringTest
    - SLSTest
    - SLSService
    - SLSSRMStats
    - SLST1Service
    - SLSLogSE
    - SLSStorage
    - SLSCondDB
  '''
  
  _tablesDB    = DIRACResourceManagementDB._tablesDB
  _tablesDB[ 'HammerCloudTest' ] = { 'Fields' : 
                     {
                       'TestID'            : 'INT UNSIGNED',
                       'SiteName'          : 'VARCHAR(64) NOT NULL',
                       'ResourceName'      : 'VARCHAR(64) NOT NULL',
                       'TestStatus'        : 'VARCHAR(16)',
                       'SubmissionTime'    : 'DATETIME NOT NULL',
                       'StartTime'         : 'DATETIME',
                       'EndTime'           : 'DATETIME',
                       'CounterTime'       : 'DATETIME',
                       'AgentStatus'       : 'VARCHAR(255) NOT NULL DEFAULT "Unspecified"',
                       'FormerAgentStatus' : 'VARCHAR(255) NOT NULL DEFAULT "Unspecified"',
                       'Counter'           : 'INT NOT NULL DEFAULT 0' 
                      },
                      'PrimaryKey' : [ 'SubmissionTime' ]                                            
                                }
  _tablesDB[ 'MonitoringTest' ] = { 'Fields' : 
                     {
                       'MetricName'     : 'VARCHAR(128) NOT NULL',
                       'ServiceURI'     : 'VARCHAR(128) NOT NULL',
                       'SiteName'       : 'VARCHAR(64) NOT NULL',
                       'ServiceFlavour' : 'VARCHAR(64) NOT NULL',
                       'MetricStatus'   : 'VARCHAR(512) NOT NULL',
                       'SummaryData'    : 'BLOB NOT NULL',
                       'Timestamp'      : 'DATETIME NOT NULL',
                       'LastCheckTime'  : 'DATETIME NOT NULL'
                      },
                      'PrimaryKey' : [ 'MetricName', 'ServiceURI' ]                                            
                                }
  _tablesDB[ 'SLSTest' ] = { 'Fields' : 
                     {
                       'TestName'      : 'VARCHAR(64) NOT NULL',
                       'Target'        : 'VARCHAR(255) NOT NULL',
                       'Availability'  : 'INT UNSIGNED NOT NULL',
                       'Result'        : 'INT NOT NULL',
                       'Description'   : 'VARCHAR(511) NOT NULL',
                       'DateEffective' : 'DATETIME NOT NULL'
                      },
                      'PrimaryKey' : [ 'TestName', 'Target' ]                                            
                                }

  _tablesDB[ 'JobAccountingCache' ] = { 'Fields' : 
                     {
                       'Name'          : 'VARCHAR(64) NOT NULL',
                       'Checking'      : 'DOUBLE NOT NULL DEFAULT 0',
                       'Completed'     : 'DOUBLE NOT NULL DEFAULT 0',
                       'Done'          : 'DOUBLE NOT NULL DEFAULT 0',
                       'Failed'        : 'DOUBLE NOT NULL DEFAULT 0',
                       'Killed'        : 'DOUBLE NOT NULL DEFAULT 0',
                       'Matched'       : 'DOUBLE NOT NULL DEFAULT 0',
                       'Running'       : 'DOUBLE NOT NULL DEFAULT 0',
                       'Stalled'       : 'DOUBLE NOT NULL DEFAULT 0',
                       'LastCheckTime' : 'DATETIME NOT NULL'
                      },
                      'PrimaryKey' : [ 'Name' ]                                            
                                }
  
  _tablesDB[ 'PilotAccountingCache' ] = { 'Fields' : 
                     {
                       'Name'          : 'VARCHAR(64) NOT NULL',
                       'Aborted'       : 'DOUBLE NOT NULL DEFAULT 0',
                       'Deleted'       : 'DOUBLE NOT NULL DEFAULT 0',
                       'Done'          : 'DOUBLE NOT NULL DEFAULT 0',
                       'Failed'        : 'DOUBLE NOT NULL DEFAULT 0',
                       'LastCheckTime' : 'DATETIME NOT NULL'
                      },
                      'PrimaryKey' : [ 'Name' ]                                            
                                }

  
  # TABLES THAT WILL EVENTUALLY BE DELETED
  
  _tablesDB[ 'SLSService' ] = { 'Fields' : 
                     {
                       'System'        : 'VARCHAR(64) NOT NULL',
                       'Service'       : 'VARCHAR(32) NOT NULL',
                       'Availability'  : 'TINYINT UNSIGNED NOT NULL',
                       'TimeStamp'     : 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                       'Host'          : 'VARCHAR(64)',
                       'ServiceUptime' : 'INT UNSIGNED',
                       'HostUptime'    : 'INT UNSIGNED',
                       'InstantLoad'   : 'FLOAT UNSIGNED',
                       'Message'       : 'TEXT',
                      },
                      'PrimaryKey' : [ 'System', 'Service' ]                                            
                                }
  _tablesDB[ 'SLSRMStats' ] = { 'Fields' : 
                     {
                       'Site'     : 'VARCHAR(64) REFERENCES SLST1Service',
                       'System'   : 'VARCHAR(32) REFERENCES SLST1Service',
                       'Name'     : 'VARCHAR(32) NOT NULL',
                       'Assigned' : 'INT UNSIGNED',
                       'Waiting'  : 'INT UNSIGNED',
                       'Done'     : 'INT UNSIGNED'
                      },
                      'PrimaryKey' : [ 'Site', 'System', 'Name' ]                                            
                                }
  _tablesDB[ 'SLST1Service' ] = { 'Fields' : 
                     {
                       'Site'          : 'VARCHAR(64) NOT NULL',
                       'System'        : 'VARCHAR(32) NOT NULL',
                       'Availability'  : 'TINYINT UNSIGNED NOT NULL',
                       'TimeStamp'     : 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                       'Version'       : 'VARCHAR(32)',
                       'ServiceUptime' : 'INT UNSIGNED',
                       'HostUptime'    : 'INT UNSIGNED',
                       'Message'       : 'TEXT'
                      },
                      'PrimaryKey' : [ 'Site', 'System' ]                                            
                                }
  _tablesDB[ 'SLSLogSE' ] = { 'Fields' : 
                     {
                       'Name'               : 'VARCHAR(32)',
                       'Availability'       : 'TINYINT UNSIGNED NOT NULL',
                       'TimeStamp'          : 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                       'ValidityDuration'   : 'VARCHAR(32) NOT NULL',
                       'DataPartitionUsed'  : 'TINYINT UNSIGNED',
                       'DataPartitionTotal' : 'BIGINT UNSIGNED'
                      },
                      'PrimaryKey' : [ 'Name' ]                                            
                                }
  _tablesDB[ 'SLSStorage' ] = { 'Fields' : 
                     {
                       'Site'             : 'VARCHAR(64) NOT NULL',
                       'Token'            : 'VARCHAR(32) NOT NULL',
                       'Availability'     : 'TINYINT UNSIGNED',
                       'TimeStamp'        : 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                       'RefreshPeriod'    : 'VARCHAR(32) NOT NULL',
                       'ValidityDuration' : 'VARCHAR(32) NOT NULL',
                       'TotalSpace'       : 'BIGINT UNSIGNED',
                       'GuaranteedSpace'  : 'BIGINT UNSIGNED',
                       'FreeSpace'        : 'BIGINT UNSIGNED',
                      },
                      'PrimaryKey' : [ 'Site', 'Token' ]                                            
                                }
  _tablesDB[ 'SLSCondDB' ] = { 'Fields' : 
                     {
                       'Site'         : 'VARCHAR(64)',
                       'Availability' : 'TINYINT UNSIGNED',
                       'TimeStamp'    : 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
                       'AccessTime'   : 'FLOAT UNSIGNED'
                      },
                      'PrimaryKey' : [ 'Site' ]                                            
                                }
  
  #_tablesLike  = DIRACResourceManagementDB._tablesLike
  #_likeToTable = DIRACResourceManagementDB._likeToTable
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  