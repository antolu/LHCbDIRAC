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
- ProductionID - production index
- PRName - name of the Production taken from the xml field "name"
- PRParent - name of the parent Workflow used to create the current one.
-            taken from the XML field "type"
- Description - short description of the workflow taken from the field "descr_short" of XML
- PublisherDN - last persone to update Production
- PublishingTime - time stamp
- Type - type of the workflow. At this point is not clear what to put in there
-   SIMULATION - Montecarlo production, no input data required
-   PROCESSING - Processing production, input files required
-   REPLICATION - data replication production, no body required
- Status - information about current status of the production
-   NEW - newly created, equivalent to STOPED
-   ACTIVE - can submit
-   STOPPED - stopped by manager
-   DONE - job limits reached, extension is possible
-   ERROR - Production with error, equivalent to STOPPED
-   TERMINATED - stopped, extension impossible
- Mode - information about submission mode of the production for the submission agent
-   MANUAL
-   AUTOMATIC
- Body - XML body of the Production if required
- removed from table
--    JobsTotal INTEGER NOT NULL DEFAULT 0,
--    JobsSubmitted INTEGER NOT NULL DEFAULT 0,
--    LastSubmittedJob INTEGER NOT NULL DEFAULT 0,
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS Productions;
CREATE TABLE Productions (
    ProductionID INTEGER NOT NULL AUTO_INCREMENT,
    PRName VARCHAR(255) NOT NULL,
    PRParent VARCHAR(255),
    Description  VARCHAR(255),
    PublisherDN VARCHAR(255) NOT NULL,
    PublishingTime TIMESTAMP,
    Type CHAR(16) DEFAULT 'SIMULATION'
    Status  CHAR(16) DEFAULT 'NEW',
    Progress FLOAT(6,2) DEFAULT 0.0,
    Mode CHAR(16) DEFAULT 'MANUAL',
    LFNMask VARCHAR(255),
    GroupBySiteFlag ENUM ( "True", "False" ) DEFAULT "True" NOT NULL,
    GroupSize INTEGER NOT NULL DEFAULT 1,
    Body BLOB,
    PRIMARY KEY(ProductionID)
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
