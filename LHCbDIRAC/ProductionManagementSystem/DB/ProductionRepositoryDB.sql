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
    WFName VARCHAR(255) NOT NULL,
    WFParent VARCHAR(255),
    Description  VARCHAR(255),
    PublisherDN VARCHAR(255) NOT NULL,
    PublishingTime TIMESTAMP,
    Body BLOB NOT NULL,
    PRIMARY KEY(WFName)
);

--------------------------------------------------------------------------------
DROP TABLE IF EXISTS Productions;
CREATE TABLE Productions (
    ProductionID INTEGER NOT NULL AUTO_INCREMENT,
    PRName VARCHAR(255) NOT NULL,
    PRParent VARCHAR(255) NOT NULL,
    Description  VARCHAR(255),
    PublisherDN VARCHAR(255) NOT NULL,
    PublishingTime TIMESTAMP,
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


-    self.always    = 'ALWAYS'
-    self.info      = 'INFO'
-    self.verbose   = 'VERB'
-    self.debug     = 'DEBUG'
-    self.warn      = 'WARN'
-    self.error     = 'ERROR'
-    self.exception = 'EXCEPT'
-    self.fatal     = 'FATAL'
-    self.__levelDict = {
-       self.always    : 30,
-       self.info      : 20,
-       self.verbose   : 10,
-       self.debug     : 0,
-       self.warn      : -10,
-       self.error     : -20,
-       self.exception : -20,
-       self.fatal     : -30
