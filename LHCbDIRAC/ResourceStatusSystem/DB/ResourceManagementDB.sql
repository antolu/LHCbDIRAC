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
CREATE TABLE HammerCloudTests (
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

DROP TABLE IF EXISTS SLSService;
CREATE TABLE SLSService (
  System varchar(64) NOT NULL,
  Service varchar(32) NOT NULL,
  TimeStamp DATETIME NOT NULL,
  Availability DECIMAL(3) UNSIGNED NOT NULL,
  ServiceUptime INT unsigned,
  HostUptime INT unsigned,
  InstantLoad DECIMAL(10,5) unsigned,
  PRIMARY KEY (System,Service)
) Engine=InnoDB;

DROP TABLE IF EXISTS SLST1Service;
CREATE TABLE SLST1Service (
  Site VARCHAR(64) NOT NULL,
  Service VARCHAR(32) NOT NULL,
  TimeStamp DATETIME NOT NULL,
  Availability DECIMAL(3) UNSIGNED NOT NULL,
  ServiceUptime INT UNSIGNED,
  HostUptime INT UNSIGNED,
  PRIMARY KEY (Site,Service)
) Engine=InnoDB;

DROP TABLE IF EXISTS SLSLogSE;
CREATE TABLE SLSLogSE (
  ID VARCHAR(32) PRIMARY KEY,
  TimeStamp DATETIME NOT NULL,
  ValidityDuration VARCHAR(32) NOT NULL,
  Availability DECIMAL(3) UNSIGNED NOT NULL,
  DataPartitionUsed DECIMAL(3) UNSIGNED,
  DataPartitionTotal INT UNSIGNED
) Engine=InnoDB;

DROP TABLE IF EXISTS SLSStorage;
CREATE TABLE SLSStorage (
  Site VARCHAR(64) NOT NULL,
  Token VARCHAR(32) NOT NULL,
  TimeStamp DATETIME NOT NULL,
  Availability DECIMAL(3) UNSIGNED,
  RefreshPeriod VARCHAR(32) NOT NULL,
  ValidityDuration VARCHAR(32) NOT NULL,
  TotalSpace INT UNSIGNED,
  GuaranteedSpace INT UNSIGNED,
  FreeSpace INT UNSIGNED,
  PRIMARY KEY (Site, Token)
) Engine=InnoDB;

DROP TABLE IF EXISTS SLSCondDB;
CREATE TABLE SLSCondDB (
  Site VARCHAR(64) PRIMARY KEY,
  TimeStamp DATETIME NOT NULL,
  Availability DECIMAL(3) UNSIGNED,
  AccessTime INT UNSIGNED
) Engine=InnoDB;
