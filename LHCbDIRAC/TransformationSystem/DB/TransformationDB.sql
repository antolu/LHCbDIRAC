-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Transformation/TransformationDB.sql,v 1.15 2009/08/26 07:17:46 rgracian Exp $
-- -------------------------------------------------------------------------------
--  Schema definition for the TransformationDB database a generic
--  engine to define input data streams and support dynamic data 
--  grouping per unit of execution.

SOURCE DIRAC/TransformationSystem/DB/TransformationDB.sql

-------------------------------------------------------------------------------
DROP TABLE IF EXISTS BkQueries;
CREATE TABLE BkQueries (
  BkQueryID INT(11) NOT NULL AUTO_INCREMENT,
  SimulationConditions VARCHAR(1024) NOT NULL DEFAULT 'All',
  INDEX (SimulationConditions),
  DataTakingConditions VARCHAR(1024) NOT NULL DEFAULT 'All',
  INDEX (DataTakingConditions),
  ProcessingPass VARCHAR(1024) NOT NULL DEFAULT 'All',
  INDEX (ProcessingPass),
  FileType VARCHAR(1024) NOT NULL DEFAULT 'All',
  INDEX (FileType),
  EventType VARCHAR(1024) NOT NULL DEFAULT 'All',
  INDEX (EventType),
  ConfigName VARCHAR(1024) NOT NULL DEFAULT 'All',
  INDEX (ConfigName),
  ConfigVersion VARCHAR(1024) NOT NULL DEFAULT 'All',
  INDEX (ConfigVersion),
  ProductionID VARCHAR(1024) NOT NULL DEFAULT 'All',
  INDEX (ProductionID),
  DataQualityFlag VARCHAR(1024) NOT NULL DEFAULT 'All',
  INDEX (DataQualityFlag),
  StartRun INT(11) NOT NULL DEFAULT 0,
  INDEX (StartRun),
  EndRun INT(11) NOT NULL DEFAULT 0,
  INDEX (EndRun),
  RunNumbers VARCHAR(2048) NOT NULL DEFAULT 'All',
  TCK VARCHAR(1024) NOT NULL DEFAULT 'All',
  Visible VARCHAR(8) NOT NULL DEFAULT 'All',
  PRIMARY KEY  (`BkQueryID`)
) ENGINE=InnoDB;

ALTER TABLE TransformationFiles ADD COLUMN RunNumber INT(11) DEFAULT 0;
ALTER TABLE TransformationTasks ADD COLUMN RunNumber INT(11) DEFAULT 0;
 
DROP TABLE IF EXISTS TransformationRuns;
CREATE TABLE TransformationRuns(
  TransformationID INTEGER NOT NULL,
  INDEX (TransformationID),
  RunNumber INT(11) NOT NULL,
  INDEX (RunNumber),
  SelectedSite VARCHAR(256) DEFAULT '',
  Status CHAR(32) DEFAULT 'Active',
  LastUpdate DATETIME,
  PRIMARY KEY (TransformationID,RunNumber)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS RunsMetadata;
CREATE TABLE RunsMetadata(
  RunNumber INT(11) NOT NULL,
  INDEX (RunNumber),
  Name VARCHAR(256) NOT NULL,
  Value VARCHAR(256) NOT NULL,
  PRIMARY KEY (RunNumber, Name)
) ENGINE=InnoDB;

