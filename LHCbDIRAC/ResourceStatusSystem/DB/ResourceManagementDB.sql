-- -----------------------------------------------------------
-- Resource Management database extension
-- -----------------------------------------------------------

SOURCE DIRAC/ResourceStatusSystem/DB/ResourceManagementDB.sql

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS MonitoringTests;
CREATE TABLE MonitoringTests (
  MetricName VARCHAR(512) NOT NULL,
  INDEX (MetricName),
  ServiceURI VARCHAR(256) NOT NULL,
  INDEX (ServiceURI),
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  ServiceFlavour VARCHAR(64) NOT NULL,
  MetricStatus VARCHAR(512) NOT NULL,
  SummaryData VARCHAR(64) NOT NULL,
  Timestamp DATETIME NOT NULL,
  LastUpdate DATETIME NOT NULL,
  PRIMARY KEY  (`MetricName`,`ServiceURI`)
) ENGINE=MyISAM;

DROP TABLE IF EXISTS HammerCloudTests;
CREATE TABLE HammerCloudTests(
  TestID INT UNSIGNED,
  SiteName VARCHAR(64) NOT NULL,
  ResourceName VARCHAR(64) NOT NULL,
  TestStatus VARCHAR(16),
  SubmissionTime DATETIME NOT NULL,
  StartTime DATETIME,
  EndTime DATETIME,
  CounterTime DATETIME,
  AgentStatus VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  FormerAgentStatus VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  Counter INT NOT NULL DEFAULT 0,
  PRIMARY KEY(SubmissionTime)
) Engine=InnoDB;

drop table if exists SLSServices;
create table SLSServices (
       System varchar(64) not null,
       Service varchar(32) not null,
       TStamp DATETIME not null,
       Availability DECIMAL(4,2) unsigned not null,
       ServiceUptime INT unsigned,
       HostUptime INT unsigned,
       InstantLoad DECIMAL(10,5) unsigned,
       primary key (System,Service)
) Engine=InnoDB;

drop table if exists SLST1Services;
create table SLST1Services (
       Site varchar(64) not null,
       Service varchar(32) not null,
       TStamp DATETIME not null,
       Availability DECIMAL(4,2) unsigned not null,
       ServiceUptime INT unsigned,
       HostUptime INT unsigned,
       primary key (Site,Service)
) Engine=InnoDB;

drop table if exists SLSLogSE;
create table SLSLogSE (
       ID varchar(32) primary key,
       TStamp         DATETIME not null,
       ValidityDuration VARCHAR(32) not null,
       Availability DECIMAL(4.2) UNSIGNED not null,
       DataPartitionUsed DECIMAL(4,2) UNSIGNED,
       DataPartitionTotal INT UNSIGNED
) Engine=InnoDB;

drop table if exists SLSStorage;
create table SLSStorage (
       ID serial,
       Url varchar(64) not null,
       Item varchar(32) not null,
       TStamp DATETIME not null,
       Availability TINYINT UNSIGNED not null,
       RefreshPeriod varchar(32) not null,
       ValidityDuration varchar(32) not null,
       FreeSpace int unsigned not null,
       OccupiedSpace int unsigned not null,
       TotalSpace int unsigned not null,
       Consumed   int unsigned not null,
       Capacity   int unsigned not null,
       primary key (ID)
) Engine=InnoDB;
