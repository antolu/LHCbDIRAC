-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/DB/ProductionRequestDB.sql,v 1.3 2009/10/15 15:27:45 azhelezo Exp $
-- ------------------------------------------------------------------------------
DROP DATABASE IF EXISTS ProductionRequestDB;
CREATE DATABASE ProductionRequestDB;
-- ------------------------------------------------------------------------------

-- Database owner definition
USE mysql;

-- Must set passwords for database user by replacing "must_be_set".
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProductionRequestDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProductionRequest.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
FLUSH PRIVILEGES;

--
--  Schema definition for the Production Requests table 
--  history ( logging ) information
-- -
-- ------------------------------------------------------------------------------
USE ProductionRequestDB;
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS ProductionRequests;
CREATE TABLE ProductionRequests (
    RequestID INTEGER NOT NULL AUTO_INCREMENT,
    ParentID INTEGER,
    INDEX(ParentID),
    MasterID INTEGER,
    INDEX(MasterID),
    RequestName VARCHAR(128) DEFAULT "",
    INDEX(RequestName),
    RequestType VARCHAR(32) DEFAULT "",
    INDEX(RequestType),
    RequestState VARCHAR(32) DEFAULT "",
    INDEX(RequestState),
    RequestPriority VARCHAR(32) DEFAULT "",
    INDEX(RequestPriority),
    RequestAuthor VARCHAR(128) DEFAULT "",
    INDEX(RequestAuthor),
    RequestPDG VARCHAR(128) DEFAULT "",

    SimCondition VARCHAR(128) DEFAULT "",
    INDEX(SimCondition),
    SimCondID INTEGER,
    SimCondDetail BLOB,
    
    ProPath VARCHAR(128) DEFAULT "",
    INDEX(ProPath),
    ProID   INTEGER,
    ProDetail BLOB,
    
    EventType VARCHAR(128) DEFAULT "",
    INDEX(EventType),
    NumberOfEvents INTEGER DEFAULT 0,
    INDEX(NumberOfEvents),
    Description BLOB,
    Comments BLOB,
    Inform   BLOB,
    PRIMARY KEY(RequestID)
);

DROP TABLE IF EXISTS ProductionProgress;
CREATE TABLE ProductionProgress (
    ProductionID INTEGER NOT NULL,
    RequestID INTEGER,
    INDEX(RequestID),
    Used TINYINT(1) DEFAULT 1, 
    INDEX(Used),
    BkEvents INTEGER,
    PRIMARY KEY(ProductionID)
);

DROP TABLE IF EXISTS RequestHistory;
CREATE TABLE RequestHistory (
    RequestID INTEGER NOT NULL,
    INDEX(RequestID),
    RequestState VARCHAR(32)  DEFAULT "",
    RequestUser  VARCHAR(128) DEFAULT "",
    TimeStamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX(TimeStamp)
);
