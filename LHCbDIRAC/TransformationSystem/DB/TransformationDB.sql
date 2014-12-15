-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Transformation/TransformationDB.sql,v 1.15 2009/08/26 07:17:46 rgracian Exp $
-- -------------------------------------------------------------------------------
--  Schema definition for the TransformationDB database a generic
--  engine to define input data streams and support dynamic data 
--  grouping per unit of execution.

SOURCE DIRAC/TransformationSystem/DB/TransformationDB.sql

SET FOREIGN_KEY_CHECKS = 0;

-- -----------------------------------------------------------------------------

ALTER TABLE Transformations ADD COLUMN Hot BOOLEAN DEFAULT FALSE;

DROP TABLE IF EXISTS BkQueries;
CREATE TABLE BkQueries (
  BkQueryID INT(11) NOT NULL AUTO_INCREMENT,
  SimulationConditions VARCHAR(1024) NOT NULL DEFAULT 'All',
  DataTakingConditions VARCHAR(1024) NOT NULL DEFAULT 'All',
  ProcessingPass VARCHAR(1024) NOT NULL DEFAULT 'All',
  FileType VARCHAR(1024) NOT NULL DEFAULT 'All',
  EventType VARCHAR(1024) NOT NULL DEFAULT 'All',
  ConfigName VARCHAR(1024) NOT NULL DEFAULT 'All',
  ConfigVersion VARCHAR(1024) NOT NULL DEFAULT 'All',
  ProductionID VARCHAR(1024) NOT NULL DEFAULT 'All',
  DataQualityFlag VARCHAR(1024) NOT NULL DEFAULT 'All',
  StartRun INT(11) NOT NULL DEFAULT 0,
  EndRun INT(11) NOT NULL DEFAULT 0,
  RunNumbers VARCHAR(2048) NOT NULL DEFAULT 'All',
  TCK VARCHAR(1024) NOT NULL DEFAULT 'All',
  Visible VARCHAR(8) NOT NULL DEFAULT 'All',
  PRIMARY KEY  (BkQueryID),
  INDEX (SimulationConditions),
  INDEX (DataTakingConditions),
  INDEX (ProcessingPass),
  INDEX (FileType),
  INDEX (EventType),
  INDEX (ConfigName),
  INDEX (ConfigVersion),
  INDEX (ProductionID),
  INDEX (DataQualityFlag),
  INDEX (StartRun),
  INDEX (EndRun)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS BkQueriesNew;
CREATE TABLE BkQueriesNew (
    TransformationID INTEGER NOT NULL,
    ParameterName VARCHAR(32) NOT NULL,
    ParameterValue LONGBLOB NOT NULL,
    PRIMARY KEY(TransformationID,ParameterName),
    FOREIGN KEY (TransformationID) REFERENCES Transformations(TransformationID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


ALTER TABLE TransformationFiles ADD COLUMN RunNumber INT(11) DEFAULT 0;
ALTER TABLE TransformationTasks ADD COLUMN RunNumber INT(11) DEFAULT 0;
 
DROP TABLE IF EXISTS TransformationRuns;
CREATE TABLE TransformationRuns(
  TransformationID INTEGER NOT NULL,
  RunNumber INT(11) NOT NULL,
  SelectedSite VARCHAR(255) DEFAULT '',
  Status CHAR(32) DEFAULT 'Active',
  LastUpdate DATETIME,
  PRIMARY KEY (TransformationID,RunNumber),
  INDEX (RunNumber),
  FOREIGN KEY (TransformationID) REFERENCES Transformations (TransformationID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS RunsMetadata;
CREATE TABLE RunsMetadata(
  RunNumber INT(11) NOT NULL,
  Name VARCHAR(255) NOT NULL,
  Value VARCHAR(255) NOT NULL,
  PRIMARY KEY (RunNumber, Name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS RunDestination;
CREATE TABLE RunDestination(
  RunNumber INT(11) NOT NULL,
  SE VARCHAR(255) NOT NULL,
  PRIMARY KEY (RunNumber, Destination)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


DROP TABLE IF EXISTS StoredJobDescription;
CREATE TABLE StoredJobDescription(
  TransformationID INTEGER NOT NULL,
  JobDescription LONGBLOB NOT NULL,
  PRIMARY KEY (TransformationID),
  FOREIGN KEY (TransformationID) REFERENCES Transformations (TransformationID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


SET FOREIGN_KEY_CHECKS = 1;
