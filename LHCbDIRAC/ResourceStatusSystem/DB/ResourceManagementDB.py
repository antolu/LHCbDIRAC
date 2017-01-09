''' LHCbDIRAC.ResourceStatusSystem.DB.ResourceManagementDB

   ResourceManagementDB.__bases__:
     DIRAC.ResourceStatusSystem.DB.ResourceManagementDB.ResourceManagementDB

'''

from DIRAC                                              import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import \
  ResourceManagementDB as DIRACResourceManagementDB

from sqlalchemy.dialects.mysql                     import DOUBLE, INTEGER, TIMESTAMP, TINYINT, BIGINT
from sqlalchemy                                    import Table, Column, MetaData, String, \
                                                          DateTime, exc, Text, text, BLOB

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

  def createTables( self ):

    EnvironmentCache         = Table( 'EnvironmentCache', self.metadata,
                              Column( 'DateEffective', DateTime, nullable = False ),
                              Column( 'LastCheckTime', DateTime, nullable = False ),
                              Column( 'SiteName', String( 64 ), nullable = False ),
                              Column( 'Environment', Text ),
                              Column( 'HashKey', String( 64 ), nullable = False, primary_key = True ),
                              Column( 'Arguments', String( 512 ), nullable = False ),
                              mysql_engine = 'InnoDB' )

    HammerCloudTest         = Table( 'HammerCloudTest', self.metadata,
                              Column( 'Counter', INTEGER, nullable = False, server_default = '0' ),
                              Column( 'TestStatus', String( 16 ) ),
                              Column( 'TestID', INTEGER ),
                              Column( 'ResourceName', String( 64 ), nullable = False ),
                              Column( 'AgentStatus', String( 255 ), nullable = False, server_default = "Unspecified" ),
                              Column( 'EndTime', DateTime ),
                              Column( 'SiteName', String( 64 ), nullable = False ),
                              Column( 'FormerAgentStatus', String( 255 ), nullable = False, server_default = "Unspecified" ),
                              Column( 'StartTime', DateTime ),
                              Column( 'SubmissionTime', DateTime, nullable = False, primary_key = True ),
                              Column( 'CounterTime', DateTime ),
                              mysql_engine = 'InnoDB' )

    MonitoringTest         = Table( 'MonitoringTest', self.metadata,
                              Column( 'ServiceFlavour', String( 64 ), nullable = False ),
                              Column( 'ServiceURI', String( 128 ), nullable = False, primary_key = True ),
                              Column( 'LastCheckTime', DateTime, nullable = False ),
                              Column( 'MetricStatus', String( 512 ), nullable = False ),
                              Column( 'SiteName', String( 64 ), nullable = False ),
                              Column( 'Timestamp', DateTime, nullable = False ),
                              Column( 'SummaryData', BLOB, nullable = False ),
                              Column( 'MetricName', String( 128 ), nullable = False, primary_key = True ),
                              mysql_engine = 'InnoDB' )

    JobAccountingCache      = Table( 'JobAccountingCache', self.metadata,
                              Column( 'Failed', DOUBLE, nullable = False, server_default = '0' ),
                              Column( 'Running', DOUBLE, nullable = False, server_default = '0' ),
                              Column( 'Done', DOUBLE, nullable = False, server_default = '0' ),
                              Column( 'Name', String( 64 ), nullable = False, primary_key = True ),
                              Column( 'Stalled', DOUBLE, nullable = False, server_default = '0' ),
                              Column( 'Checking', DOUBLE, nullable = False, server_default = '0' ),
                              Column( 'Completed', DOUBLE, nullable = False, server_default = '0' ),
                              Column( 'Killed', DOUBLE, nullable = False ),
                              Column( 'LastCheckTime', DateTime, nullable = False ),
                              Column( 'Matched', DOUBLE, nullable = False, server_default = '0' ),
                              mysql_engine = 'InnoDB' )


    PilotAccountingCache    = Table( 'PilotAccountingCache', self.metadata,
                              Column( 'Name', String( 64 ), nullable = False, primary_key = True ),
                              Column( 'LastCheckTime', DateTime, nullable = False ),
                              Column( 'Deleted', DOUBLE, nullable = False, server_default = '0' ),
                              Column( 'Failed', DOUBLE, nullable = False, server_default = '0' ),
                              Column( 'Done', DOUBLE, nullable = False, server_default = '0' ),
                              Column( 'Aborted', DOUBLE, nullable = False, server_default = '0' ),
                              mysql_engine = 'InnoDB' )


    # TABLES THAT WILL EVENTUALLY BE DELETED

    SLST1Service         = Table( 'SLST1Service', self.metadata,
                              Column( 'HostUptime', INTEGER ),
                              Column( 'Version', String( 32 ) ),
                              Column( 'ServiceUptime', INTEGER ),
                              Column( 'TimeStamp', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')),
                              Column( 'Message', Text ),
                              Column( 'Availability', TINYINT, nullable = False ),
                              Column( 'Site', String( 64 ), nullable = False, primary_key = True ),
                              Column( 'System', String( 32 ), nullable = False, primary_key = True ),
                              mysql_engine = 'InnoDB' )

    SLSLogSE              = Table( 'SLSLogSE', self.metadata,
                              Column( 'DataPartitionTotal', BIGINT ),
                              Column( 'Name', String( 32 ), server_default = '0' ),
                              Column( 'TimeStamp', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP')),
                              Column( 'DataPartitionUsed', TINYINT ),
                              Column( 'ValidityDuration', String( 32 ), nullable = False ),
                              Column( 'Availability', TINYINT, nullable = False ),
                              mysql_engine = 'InnoDB' )


    # create tables

    try:
      self.metadata.create_all( self.engine )
    except exc.SQLAlchemyError as e:
      self.log.exception( "createTables: unexpected exception", lException = e )
      return S_ERROR( "createTables: unexpected exception %s" % e )

    return S_OK()

#...............................................................................
#EOF
