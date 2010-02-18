-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/DB/ProductionDB.sql,v 1.21 2009/08/26 09:39:53 rgracian Exp $
-- ------------------------------------------------------------------------------
--
--  Schema definition for the ProductionDB database - containing Productions and WorkFlows (Templates)
-- ------------------------------------------------------------------------------
DROP DATABASE IF EXISTS ProductionDB;
CREATE DATABASE ProductionDB;
-- ------------------------------------------------------------------------------

-- Database owner definition
USE mysql;

-- Must set passwords for database user by replacing "must_be_set".
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProductionDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProductionDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
FLUSH PRIVILEGES;

-- -----------------------------------------------------------------------------
USE ProductionDB;
-- -----------------------------------------------------------------------------
-- This table keeps Workflow for the purpose of creating Productions
-- WFName - name of the WF taken from the xml field "name"
-- WFParent - name of the parent Workflow used to create the current one.
--            taken from the XML field "type"
-- Description - short description of the workflow taken from the field "descr_short" of XML
-- PublisherDN - last persone to update WF
-- PublishingTime - time stamp
-- Body - XML body of the Workflow
-- ------------------------------------------------------------------------------

DROP TABLE IF EXISTS Workflows;
CREATE TABLE Workflows (
    WFName VARCHAR(255) NOT NULL,
    INDEX (WFName),
    WFParent VARCHAR(255),
    Description  VARCHAR(255),
    LongDescription  BLOB,
    AuthorDN VARCHAR(255) NOT NULL,
    AuthorGroup VARCHAR(255) NOT NULL,
    PublishingTime TIMESTAMP,
    Body BLOB,
    PRIMARY KEY(WFName)
);

SOURCE LHCbDIRAC/ProductionManagementSystem/DB/TransformationDB.sql
