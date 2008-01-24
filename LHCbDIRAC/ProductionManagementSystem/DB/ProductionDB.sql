-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/DB/ProductionDB.sql,v 1.2 2008/01/24 15:11:26 acsmith Exp $
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

-- Must set passwords for database user by replacing "must_be_set".
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProductionRepositoryDB.* TO Dirac@localhost IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProductionRepositoryDB.* TO Dirac@volhcb03.cern.ch IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ProductionRepositoryDB.* TO Dirac@'%' IDENTIFIED BY 'must_be_set';
FLUSH PRIVILEGES;

-------------------------------------------------------------------------------
USE ProductionRepositoryDB;
-------------------------------------------------------------------------------
-- This table keeps Workflow for the purpose of creating Productions
-- WFName - name of the WF taken from the xml field "name"
-- WFParent - name of the parent Workflow used to create the current one.
--            taken from the XML field "type"
-- Description - short description of the workflow taken from the field "descr_short" of XML
-- PublisherDN - last persone to update WF
-- PublishingTime - time stamp
-- Body - XML body of the Workflow
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
--
-- Added the standard base class database tables here
--
--------------------------------------------------------------------------------

SOURCE TransformationDB.sql