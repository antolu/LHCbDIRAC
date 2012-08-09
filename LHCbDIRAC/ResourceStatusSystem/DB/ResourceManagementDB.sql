-- -----------------------------------------------------------
-- Resource Management database extension
-- -----------------------------------------------------------

SOURCE DIRAC/ResourceStatusSystem/DB/ResourceManagementDB.sql

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS MonitoringTest;
CREATE TABLE MonitoringTest (
  MetricName VARCHAR(128) NOT NULL,
  INDEX (MetricName),
  ServiceURI VARCHAR(128) NOT NULL,
  INDEX (ServiceURI),
  SiteName VARCHAR(64) NOT NULL,
  INDEX (SiteName),
  ServiceFlavour VARCHAR(64) NOT NULL,
  MetricStatus VARCHAR(512) NOT NULL,
  SummaryData BLOB NOT NULL,
  Timestamp DATETIME NOT NULL,
  LastCheckTime DATETIME NOT NULL,
  PRIMARY KEY  (`MetricName`,`ServiceURI`)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS HammerCloudTest;
CREATE TABLE HammerCloudTest(
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

DROP TABLE IF EXISTS SLSTest;
CREATE TABLE SLSTest(
  TestName VARCHAR(64) NOT NULL,
  Target VARCHAR(255) NOT NULL,
  Availability INT UNSIGNED NOT NULL,
  Result INT NOT NULL,
  Description VARCHAR(511) NOT NULL,
  DateEffective DATETIME NOT NULL,
  PRIMARY KEY  ( TestName, Target )
) Engine=InnoDB;
  
DROP TABLE IF EXISTS SLSService;
CREATE TABLE SLSService (
  System VARCHAR(64) NOT NULL,
  Service VARCHAR(32) NOT NULL,
  Availability TINYINT UNSIGNED NOT NULL,
  TimeStamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  Host VARCHAR(64),
  ServiceUptime INT UNSIGNED,
  HostUptime INT UNSIGNED,
  InstantLoad FLOAT UNSIGNED,
  Message TEXT,
  PRIMARY KEY (System,Service)
) Engine=InnoDB;

DROP TABLE IF EXISTS SLSRMStats;
CREATE TABLE SLSRMStats (
  Site VARCHAR(64) REFERENCES SLST1Service,
  System VARCHAR(32) REFERENCES SLST1Service,
  Name VARCHAR(32) NOT NULL,
  Assigned INT UNSIGNED,
  Waiting INT UNSIGNED,
  Done INT UNSIGNED,
  PRIMARY KEY (Site,System,Name)
) Engine=InnoDB;

DROP TABLE IF EXISTS SLST1Service;
CREATE TABLE SLST1Service (
  Site VARCHAR(64) NOT NULL,
  System VARCHAR(32) NOT NULL,
  Availability TINYINT UNSIGNED NOT NULL,
  TimeStamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  Version VARCHAR(32),
  ServiceUptime INT UNSIGNED,
  HostUptime INT UNSIGNED,
  Message TEXT,
  PRIMARY KEY (Site,System)
) Engine=InnoDB;

DROP TABLE IF EXISTS SLSLogSE;
CREATE TABLE SLSLogSE (
  Name VARCHAR(32) PRIMARY KEY,
  Availability TINYINT UNSIGNED NOT NULL,
  TimeStamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  ValidityDuration VARCHAR(32) NOT NULL,
  DataPartitionUsed TINYINT UNSIGNED,
  DataPartitionTotal BIGINT UNSIGNED
) Engine=InnoDB;

DROP TABLE IF EXISTS SLSStorage;
CREATE TABLE SLSStorage (
  Site VARCHAR(64) NOT NULL,
  Token VARCHAR(32) NOT NULL,
  Availability TINYINT UNSIGNED,
  TimeStamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  RefreshPeriod VARCHAR(32) NOT NULL,
  ValidityDuration VARCHAR(32) NOT NULL,
  TotalSpace BIGINT UNSIGNED,
  GuaranteedSpace BIGINT UNSIGNED,
  FreeSpace BIGINT UNSIGNED,
  PRIMARY KEY (Site, Token)
) Engine=InnoDB;

DROP TABLE IF EXISTS SLSCondDB;
CREATE TABLE SLSCondDB (
  Site VARCHAR(64) PRIMARY KEY,
  Availability TINYINT UNSIGNED,
  TimeStamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  AccessTime FLOAT UNSIGNED
) Engine=InnoDB;
