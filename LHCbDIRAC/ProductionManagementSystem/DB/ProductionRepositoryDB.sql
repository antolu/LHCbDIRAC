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
- This table keeps Workflow for the purpose of creating Productions
- WFName - name of the WF taken from the xml field "name"
- WFParent - name of the parent Workflow used to create the current one.
-            taken from the XML field "type"
- Description - short description of the workflow taken from the field "descr_short" of XML
- PublisherDN - last persone to update WF
- PublishingTime - time stamp
- Body - XML body of the Workflow
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
- This table store Productions
- TransformationID - Transformation index
- Name - name of the Transformation (taken from the xml field "name")
- Description - short description of the workflow (taken from the field "descr_short" of XML)
- LongDescription - short description of the workflow (taken from the field "description" of XML)
- PublishingTime - time stamp
- PublisherDN - persone published Production
- PublisherGroup - group used to publish
- Type - type of the workflow. At this point is not clear what to put in there
-   SIMULATION - Montecarlo production, no input data required
-   PROCESSING - Processing production, input files required
-   REPLICATION - data replication production, no body required
- Mode - information about submission mode of the production for the submission agent
-   MANUAL
-   AUTOMATIC
- Status - information about current status of the production
-   NEW - newly created, equivalent to STOPED
-   ACTIVE - can submit
-   STOPPED - stopped by manager
-   DONE - job limits reached, extension is possible
-   ERROR - Production with error, equivalent to STOPPED
-   TERMINATED - stopped, extension impossible
- FileMask - filter mask
- Body - XML body of the Production if required
---- In the Table Peremeters
-     PRParent - name of the parent Workflow used to create the current one.
-            taken from the XML field "type"
-     GroupSize - number of files per Transformation
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS Transformations;
CREATE TABLE Transformations (
    TransformationID INTEGER NOT NULL AUTO_INCREMENT,
    Name VARCHAR(255) NOT NULL,
    Description VARCHAR(255),
    LongDescription  BLOB,
    PublishingTime TIMESTAMP,
    PublisherDN VARCHAR(255) NOT NULL,
    PublisherGroup VARCHAR(255) NOT NULL,
    Type CHAR(16) DEFAULT 'SIMULATION'
    Mode CHAR(16) DEFAULT 'MANUAL',
    Status  CHAR(16) DEFAULT 'NEW',
    FileMask VARCHAR(255),
    Body BLOB,
    PRIMARY KEY(TransformationID)
);

--------------------------------------------------------------------------------
- For each row in the Productions table we going to have associated Job table
- JobID - job index within production
- WwsStatus - job status in the WMS, for example:
-   CREATED - newly created job
-   SUBMITTED - job submitted to WMS
-   DONE - job finished
- JobWmsID - index of this job in the WMS
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS Jobs_<ProductionID>;
CREATE TABLE  Jobs_<ProductionID>(
  JobID INTEGER NOT NULL AUTO_INCREMENT,
  WmsStatus char(16) DEFAULT 'CREATED',
  JobWmsID char(16),
  InputVector BLOB DEFAULT 0,
  PRIMARY KEY(JobID)
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
