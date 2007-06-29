-- $Header $
--------------------------------------------------------------------------------
--
--  Schema definition for the ProductionRepositoryDB database - containing Productions and WorkFlows (Templates)
--  history ( logging ) information
---
--------------------------------------------------------------------------------
DROP DATABASE IF EXISTS ProductionRepositoryDB;
CREATE DATABASE ProductionRepositoryDB;
--------------------------------------------------------------------------------

-- Database owner definition
USE mysql;
DELETE FROM user WHERE user='Dirac';

-- Must set passwords for database user by replacing "must_be_set".
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProductionRepositoryDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProductionRepositoryDB.* TO Dirac@volhcb03.cern.ch IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProductionRepositoryDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
FLUSH PRIVILEGES;

-------------------------------------------------------------------------------
USE ProductionRepositoryDB;

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS Workflows;
CREATE TABLE Workflows (
    WFType VARCHAR(255) NOT NULL,
    PublisherDN VARCHAR(255) NOT NULL,
    PublishingTime DATETIME,
    Body BLOB NOT NULL,
    PRIMARY KEY(WFType)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS Productions;
CREATE TABLE Productions (
    ProductionID VARCHAR(32) NOT NULL DEFAULT '00000000',
    WFName VARCHAR(255) NOT NULL,
    WFType VARCHAR(255) NOT NULL,
    PublisherDN VARCHAR(255) NOT NULL,
    PublishingTime DATETIME,
    JobsTotal INTEGER NOT NULL DEFAULT 0,
    JobsSubmitted INTEGER NOT NULL DEFAULT 0,
    LastSubmittedJob INTEGER NOT NULL DEFAULT 0,
    Status  VARCHAR(255) NOT NULL,
    Body BLOB NOT NULL,
    PRIMARY KEY(ProductionID)
);

--------------------------------------------------------------------------------
--- this will be a dynamic table one per Production
DROP TABLE IF EXISTS InputViectors_<ProductionID>;
CREATE TABLE InputViectors (
    vectorID INTEGER NOT NULL AUTO_INCREMENT,
    vector_body BLOB NOT NULL,
    PRIMARY KEY (vectorID)
);
