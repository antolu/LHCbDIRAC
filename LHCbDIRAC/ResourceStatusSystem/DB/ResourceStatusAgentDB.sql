-- -----------------------------------------------------------
-- Resource Status Agent database definition
-- -----------------------------------------------------------

DROP DATABASE IF EXISTS ResourceStatusAgentDB;

CREATE DATABASE ResourceStatusAgentDB;
--
-- Must set passwords for database user by replacing "must_be_set".
--
-- Create user DIRAC
USE mysql;
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ResourceStatusAgentDB.* TO 'Dirac'@'localhost' IDENTIFIED BY 'must_be_set';
GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ResourceStatusAgentDB.* TO 'Dirac'@'%' IDENTIFIED BY 'must_be_set';

FLUSH PRIVILEGES;

-- -----------------------------------------------------------

USE ResourceStatusAgentDB;

DROP TABLE IF EXISTS HCAgent;
CREATE TABLE HCAgent(
  TestID INT UNSIGNED,
  SiteName VARCHAR(64) NOT NULL,
  TestStatus VARCHAR(16), 
  SubmissionTime DATETIME NOT NULL,
  StartTime DATETIME,
  EndTime DATETIME,
  CounterTime DATETIME,
  Reason VARCHAR(255) NOT NULL DEFAULT 'Unspecified',
  Counter INT NOT NULL DEFAULT 0,
  PRIMARY KEY(SubmissionTime)
) Engine=InnoDB;

