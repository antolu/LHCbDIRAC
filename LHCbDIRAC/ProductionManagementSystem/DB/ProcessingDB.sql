-------------------------------------------------------------
-- Processing database  definition
-- author: A. Tsaregorodtsev
-- date: 25.09.2005
-------------------------------------------------------------

DROP DATABASE IF EXISTS ProcessingDB;

CREATE DATABASE ProcessingDB;


-- Create user DIRAC
use mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProcessingDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'lhcbMySQL';
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProcessingDB.* TO 'Dirac'@'%' IDENTIFIED BY 'lhcbMySQL';

FLUSH PRIVILEGES;

-------------------------------------------------------------
-- Create ProcessingDB tables

use ProcessingDB;

DROP TABLE IF EXISTS DataFiles;
CREATE TABLE DataFiles (
   FileID INTEGER NOT NULL AUTO_INCREMENT,
   LFN VARCHAR(255) UNIQUE,
   Status varchar(32) DEFAULT 'AprioriGood',
   PRIMARY KEY (FileID, LFN)
);

DROP TABLE IF EXISTS Replicas;
CREATE TABLE Replicas (
  FileID INTEGER NOT NULL,
  PFN VARCHAR(255),
  SE VARCHAR(32),
  Status VARCHAR(32) DEFAULT 'AprioriGood',
  PRIMARY KEY (FileID, SE)
);

DROP TABLE IF EXISTS Transformations;
CREATE TABLE Transformations (
  TransID INTEGER NOT NULL AUTO_INCREMENT,
  Production CHAR(8) UNIQUE,
  CreateDate DATETIME,
  Status VARCHAR(32) DEFAULT "NEW",
  Progress FLOAT(6,2) DEFAULT 0.0,
  PRIMARY KEY  (`TransID`)
);

DROP TABLE IF EXISTS InputStreams;
CREATE TABLE InputStreams (
  TransID INTEGER NOT NULL,
  StreamName VARCHAR(32),
  LFNMask VARCHAR(255),
  GroupBySiteFlag ENUM ( "True", "False" ) DEFAULT "True" NOT NULL,
  GroupSize INTEGER NOT NULL DEFAULT 1
);

