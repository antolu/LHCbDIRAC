-- $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/Core/Transformation/TransformationDB.sql,v 1.15 2009/08/26 07:17:46 rgracian Exp $
-- -------------------------------------------------------------------------------
--  Schema definition for the TransformationDB database a generic
--  engine to define input data streams and support dynamic data 
--  grouping per unit of execution.

SOURCE DIRAC/TransformationSystem/DB/TransformationDB.sql

-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS BkQueries;
CREATE TABLE BkQueries (
  BkQueryID int(11) NOT NULL auto_increment,
  SimulationConditions varchar(128) NOT NULL default 'All',
  INDEX (SimulationConditions),
  DataTakingConditions varchar(128) NOT NULL default 'All',
  INDEX (DataTakingConditions),
  ProcessingPass varchar(128) NOT NULL default 'All',
  INDEX (ProcessingPass),
  FileType varchar(32) NOT NULL default 'All',
  INDEX (FileType),
  EventType int(11) NOT NULL default '0',
  INDEX (EventType),
  ConfigName varchar(64) NOT NULL default 'All',
  INDEX (ConfigName),
  ConfigVersion varchar(64) NOT NULL default 'All',
  INDEX (ConfigVersion),
  ProductionID int(11) NOT NULL default '0',
  INDEX (ProductionID),
  DataQualityFlag varchar(32) NOT NULL default 'All',
  INDEX (DataQualityFlag),
  PRIMARY KEY  (`BkQueryID`)
) ENGINE=MyISAM

