-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/DB/ErrorLoggingDB.sql,v 1.1 2009/10/22 09:02:25 roma Exp $

-- ------------------------------------------------------------------------------
--
--  Schema definition for the NotificationDB database - containing the alarms
--  data
-- -
-- ------------------------------------------------------------------------------

DROP DATABASE IF EXISTS ErrorLoggingDB;

CREATE DATABASE ErrorLoggingDB;

-- ------------------------------------------------------------------------------
-- Database owner definition

-- USE mysql;
-- DELETE FROM user WHERE user='Dirac';

--
-- Must set passwords for database user by replacing "must_be_set".
--

-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ErrorLoggingDB.* TO Dirac@localhost IDENTIFIED BY 'lhcbMySQL';
-- GRANT SELECT,INSERT,LOCK TABLES,UPDATE,DELETE,CREATE,DROP,ALTER ON ErrorLoggingDB.* TO Dirac@'%' IDENTIFIED BY 'lhcbMySQL';

-- FLUSH PRIVILEGES;

-- ----------------------------------------------------------------------------- 
USE ErrorLoggingDB;

-- ------------------------------------------------------------------------------
DROP TABLE IF EXISTS ErrorLog;
CREATE TABLE ErrorLog (
    ErrorID INTEGER NOT NULL AUTO_INCREMENT,
    ProductionID INTEGER NOT NULL,
    Project VARCHAR(32) NOT NULL DEFAULT 'Unknown',
    Version VARCHAR(32) NOT NULL DEFAULT 'Unknown',
    ErrorNumber INTEGER NOT NULL DEFAULT 0,
    ErrorDate DATETIME NOT NULL,
    Primary Key (ErrorID),
    Index (ProductionID),
    Index (Project, Version)

);

