CREATE TABLE APPLICATIONS (	APPLICATIONID NUMBER, 
	APPLICATIONNAME VARCHAR2(128 BYTE) NOT NULL, 
	APPLICATIONVERSION VARCHAR2(128 BYTE) NOT NULL, 
	OPTIONFILES VARCHAR2(1000 BYTE), 
	DDDB VARCHAR2(256 BYTE), 
	CONDDB VARCHAR2(256 BYTE), 
	EXTRAPACKAGES VARCHAR2(256 BYTE), 
	 PRIMARY KEY (APPLICATIONID));

CREATE TABLE CONFIGURATIONSe (	CONFIGURATIONID NUMBER, 
	CONFIGNAME VARCHAR2(128 BYTE) NOT NULL, 
	CONFIGVERSION VARCHAR2(128 BYTE) NOT NULL, 
	 PRIMARY KEY (CONFIGURATIONID),
	 CONSTRAINT CONFIGURATION_UK  UNIQUE (CONFIGNAME,CONFIGVERSION));

CREATE TABLE DATA_TAKING_CONDITIONS 
   (	DAQPERIODID NUMBER, 
	DESCRIPTION VARCHAR2(256 BYTE), 
	BEAMCOND VARCHAR2(256 BYTE), 
	BEAMENERGY VARCHAR2(256 BYTE), 
	MAGNETICFIELD VARCHAR2(256 BYTE), 
	VELO VARCHAR2(256 BYTE), 
	IT VARCHAR2(256 BYTE), 
	TT VARCHAR2(256 BYTE), 
	OT VARCHAR2(256 BYTE), 
	RICH1 VARCHAR2(256 BYTE), 
	RICH2 VARCHAR2(256 BYTE), 
	SPD_PRS VARCHAR2(256 BYTE), 
	ECAL VARCHAR2(256 BYTE), 
	HCAL VARCHAR2(256 BYTE), 
	MUON VARCHAR2(256 BYTE), 
	L0 VARCHAR2(256 BYTE), 
	HLT VARCHAR2(256 BYTE), 
	VELOPOSITION VARCHAR2(255 BYTE), 
	 	PRIMARY KEY (DAQPERIODID));
	 
 CREATE INDEX DATA_TAKING_CONDITION_ID_DESC ON DATA_TAKING_CONDITIONS (DAQPERIODID, DESCRIPTION);
 
 
CREATE TABLE DATAQUALITY (	
  	QUALITYID NUMBER, 
	DATAQUALITYFLAG VARCHAR2(256 BYTE), 
	 	PRIMARY KEY (QUALITYID));


CREATE TABLE EVENTTYPES 
   (	DESCRIPTION VARCHAR2(256 BYTE), 
	EVENTTYPEID NUMBER, 
	PRIMARY VARCHAR2(256 BYTE), 
	 PRIMARY KEY (EVENTTYPEID));

CREATE TABLE FILES 
   (	FILEID NUMBER, 
	ADLER32 VARCHAR2(256 BYTE), 
	CREATIONDATE TIMESTAMP (6), 
	EVENTSTAT NUMBER, 
	EVENTTYPEID NUMBER, 
	FILENAME VARCHAR2(256 BYTE) NOT NULL, 
	FILETYPEID NUMBER, 
	GOTREPLICA VARCHAR2(3 BYTE) DEFAULT 'No', 
	GUID VARCHAR2(256 BYTE) NOT NULL, 
	JOBID NUMBER(38,0), 
	MD5SUM VARCHAR2(256 BYTE) NOT NULL, 
	FILESIZE NUMBER DEFAULT 0, 
	QUALITYID NUMBER DEFAULT 1, 
	INSERTTIMESTAMP TIMESTAMP (6) DEFAULT current_timestamp NOT NULL, 
	FULLSTAT NUMBER, 
	PHYSICSTAT NUMBER, 
	LUMINOSITY NUMBER DEFAULT 0, 
	VISIBILITYFLAG CHAR(1 BYTE) DEFAULT 'Y', 
	INSTLUMINOSITY NUMBER DEFAULT 0, 
	 CONSTRAINT FILES_PK11 PRIMARY KEY (FILEID), 
	 CONSTRAINT FILES_FILENAME_UNIQUE UNIQUE (FILENAME) 
	 CONSTRAINT CHECK_PHYSICSTAT CHECK (physicstat < 0), 
	 CHECK (visibilityFlag in ('N','Y')), 
	 CONSTRAINT FILES_FK11 FOREIGN KEY (EVENTTYPEID)
	  REFERENCES EVENTTYPES (EVENTTYPEID), 
	 CONSTRAINT FILES_FK21 FOREIGN KEY (FILETYPEID)
	  REFERENCES FILETYPES (FILETYPEID), 
	 CONSTRAINT FK_QUALITYID FOREIGN KEY (QUALITYID)
	  REFERENCES DATAQUALITY (QUALITYID), 
	 CONSTRAINT FILES_FK31 FOREIGN KEY (JOBID)
	  REFERENCES JOBS (JOBID) ON DELETE CASCADE
   ) PARTITION BY RANGE (JOBID) 
 (PARTITION SECT_0020M  VALUES LESS THAN (20000000), 
 PARTITION SECT_0040M  VALUES LESS THAN (40000000), 
 PARTITION SECT_0060M  VALUES LESS THAN (60000000), 
 PARTITION SECT_0080M  VALUES LESS THAN (80000000), 
 PARTITION SECT_0100M  VALUES LESS THAN (100000000), 
 PARTITION SECT_0120M  VALUES LESS THAN (120000000), 
 PARTITION SECT_0140M  VALUES LESS THAN (140000000), 
 PARTITION SECT_0160M  VALUES LESS THAN (160000000), 
 PARTITION SECT_0180M  VALUES LESS THAN (180000000), 
 PARTITION SECT_0200M  VALUES LESS THAN (200000000), 
 PARTITION SECT_0220M  VALUES LESS THAN (220000000), 
 PARTITION SECT_0240M  VALUES LESS THAN (240000000), 
 PARTITION SECT_0260M  VALUES LESS THAN (260000000), 
 PARTITION SECT_0280M  VALUES LESS THAN (280000000), 
 PARTITION SECT_0300M  VALUES LESS THAN (300000000), 
 PARTITION SECT_0320M  VALUES LESS THAN (320000000), 
 PARTITION SECT_0340M  VALUES LESS THAN (340000000), 
 PARTITION SECT_0360M  VALUES LESS THAN (360000000), 
 PARTITION SECT_0380M  VALUES LESS THAN (380000000), 
 PARTITION SECT_0400M  VALUES LESS THAN (400000000), 
 PARTITION SECT_0420M  VALUES LESS THAN (420000000), 
 PARTITION SECT_0440M  VALUES LESS THAN (440000000), 
 PARTITION SECT_0460M  VALUES LESS THAN (460000000), 
 PARTITION SECT_0480M  VALUES LESS THAN (480000000), 
 PARTITION SECT_0500M  VALUES LESS THAN (500000000), 
 PARTITION SECT_0520M  VALUES LESS THAN (520000000)) NOLOGGING;

CREATE INDEX FILES_FILETYPEID ON FILES (FILETYPEID); 
ALTER INDEX FILES_FILETYPEID  UNUSABLE;

CREATE INDEX FILES_GUID ON FILES (GUID);

CREATE INDEX FILES_JOB_EVENT_FILETYPE ON FILES (JOBID, EVENTTYPEID, FILETYPEID) local; 
  
CREATE INDEX FILES_TIME_GOTREPLICA ON FILES (INSERTTIMESTAMP, GOTREPLICA); 
ALTER INDEX FILES_TIME_GOTREPLICA invisible;
  
CREATE INDEX F_GOTREPLICA ON FILES (GOTREPLICA, VISIBILITYFLAG, JOBID) local; 

CREATE TABLE FILETYPES 
   (	DESCRIPTION VARCHAR2(256 BYTE), 
	FILETYPEID NUMBER, 
	NAME VARCHAR2(256 BYTE), 
	VERSION VARCHAR2(256 BYTE), 
	 PRIMARY KEY (FILETYPEID), 
	 CONSTRAINT FILETYPES_NAME_VERSION UNIQUE (NAME, VERSION), 
	 CONSTRAINT FILETYPES_ID_NAME_UK UNIQUE (FILETYPEID, NAME));

CREATE TABLE INPUTFILES 
   (	FILEID NUMBER, 
	JOBID NUMBER, 
	 CONSTRAINT PK_INPUTFILES_ PRIMARY KEY (FILEID, JOBID), 
	 CONSTRAINT FILES_FK1 FOREIGN KEY (FILEID)
	  REFERENCES FILES (FILEID), 
	 CONSTRAINT INPUTFILES_FK31 FOREIGN KEY (JOBID)
	  REFERENCES JOBS (JOBID) ON DELETE CASCADE); 

CREATE INDEX INPUTFILES_JOBID ON INPUTFILES (JOBID); 


  CREATE TABLE JOBS 
   (	JOBID NUMBER, 
	CONFIGURATIONID NUMBER, 
	DIRACJOBID NUMBER, 
	DIRACVERSION VARCHAR2(256 BYTE), 
	EVENTINPUTSTAT NUMBER, 
	EXECTIME FLOAT(126), 
	FIRSTEVENTNUMBER NUMBER, 
	GEOMETRYVERSION VARCHAR2(256 BYTE), 
	GRIDJOBID VARCHAR2(256 BYTE), 
	JOBEND TIMESTAMP (6), 
	JOBSTART TIMESTAMP (6), 
	LOCALJOBID VARCHAR2(256 BYTE), 
	LOCATION VARCHAR2(256 BYTE), 
	NAME VARCHAR2(256 BYTE), 
	NUMBEROFEVENTS NUMBER, 
	PRODUCTION NUMBER, 
	PROGRAMNAME VARCHAR2(256 BYTE), 
	PROGRAMVERSION VARCHAR2(256 BYTE), 
	STATISTICSREQUESTED NUMBER, 
	WNCPUPOWER VARCHAR2(256 BYTE), 
	CPUTIME FLOAT(126), 
	WNCACHE VARCHAR2(256 BYTE), 
	WNMEMORY VARCHAR2(256 BYTE), 
	WNMODEL VARCHAR2(256 BYTE), 
	WORKERNODE VARCHAR2(256 BYTE), 
	GENERATOR VARCHAR2(256 BYTE), 
	RUNNUMBER NUMBER, 
	FILLNUMBER NUMBER, 
	WNCPUHS06 FLOAT(126) DEFAULT 0.0, 
	TOTALLUMINOSITY NUMBER DEFAULT 0, 
	TCK VARCHAR2(20 BYTE) DEFAULT 'None', 
	STEPID NUMBER, 
	WNMJFHS06 FLOAT(126), 
	HLT2TCK VARCHAR2(20 BYTE), 
	  PRIMARY KEY (JOBID),
	 CONSTRAINT JOB_NAME_UNIQUE UNIQUE (NAME), 
	 CONSTRAINT FK_PRODCONT_PROD FOREIGN KEY (PRODUCTION)
	  REFERENCES PRODUCTIONSCONTAINER (PRODUCTION), 
	 CONSTRAINT JOBS_FK1 FOREIGN KEY (CONFIGURATIONID)
	  REFERENCES CONFIGURATIONS (CONFIGURATIONID), 
	 CONSTRAINT FK_JOBS_STEPID FOREIGN KEY (STEPID)
	  REFERENCES STEPS (STEPID)
   ) 
  PARTITION BY RANGE (PRODUCTION) 
  SUBPARTITION BY HASH (CONFIGURATIONID) 
  SUBPARTITION TEMPLATE ( 
    SUBPARTITION CONFIG1, 
    SUBPARTITION CONFIG2, 
    SUBPARTITION CONFIG3, 
    SUBPARTITION CONFIG4, 
    SUBPARTITION CONFIG5, 
    SUBPARTITION CONFIG6, 
    SUBPARTITION CONFIG7, 
    SUBPARTITION CONFIG8 ) 
 (PARTITION RUNLAST  VALUES LESS THAN (-187450), 
 PARTITION RUN2  VALUES LESS THAN (-90000), 
 PARTITION RUN1  VALUES LESS THAN (0), 
 PARTITION PROD1  VALUES LESS THAN (33612), 
 PARTITION PROD2  VALUES LESS THAN (42466), 
 PARTITION PROD3  VALUES LESS THAN (49181), 
 PARTITION PRODLAST  VALUES LESS THAN (MAXVALUE));

  CREATE INDEX CONF_JOB_RUN ON JOBS (CONFIGURATIONID, JOBID, RUNNUMBER);

  CREATE INDEX JOBSPROGNAMEANDVERSION ON JOBS (PROGRAMNAME, PROGRAMVERSION);
  
  CREATE INDEX JOBS_DIRACJOBID_JOBID ON JOBS (DIRACJOBID, JOBID) LOCAL; 

  CREATE INDEX JOBS_FILL_RUNNUMBER ON JOBS (FILLNUMBER, RUNNUMBER);

  CREATE INDEX JOBS_PRODUCTIONID ON JOBS (PRODUCTION);

  CREATE INDEX JOBS_PROD_CONFIG_JOBID ON JOBS (PRODUCTION, CONFIGURATIONID, JOBID) LOCAL; 
  
  CREATE INDEX PROD_START_END ON JOBS (PRODUCTION, JOBSTART, JOBEND);

  CREATE INDEX RUNNUMBER ON JOBS (RUNNUMBER);

  CREATE TABLE NEWRUNQUALITY 
   (	RUNNUMBER NUMBER, 
	QUALITYID NUMBER, 
	PROCESSINGID NUMBER, 
	 CONSTRAINT PK_RUN_QUALITY PRIMARY KEY (RUNNUMBER, PROCESSINGID), 
	 CONSTRAINT FK_QUALITYID_RUN FOREIGN KEY (QUALITYID)
	  REFERENCES DATAQUALITY (QUALITYID), 
	 CONSTRAINT PROCESSING_ID FOREIGN KEY (PROCESSINGID)
	  REFERENCES PROCESSING (ID);

  CREATE INDEX NEWRUNQUALITY_PROC ON NEWRUNQUALITY (PROCESSINGID);

  CREATE OR REPLACE EDITIONABLE TRIGGER RUNQUALITY 
before update or insert on newrunquality
referencing new as new old as old
for each row  
begin
   update files SET insertTimestamp=sys_extract_utc(systimestamp + interval '5' minute), files.qualityid= :new.qualityid where jobid in (select j.jobid from jobs j where j.runnumber= :new.runnumber and j.production<0);
   update files SET insertTimestamp=sys_extract_utc(systimestamp + interval '5' minute), files.qualityid= :new.qualityid where files.fileid in (select f.fileid from files f, jobs j where j.jobid=f.jobid and j.runnumber= :new.runnumber and f.gotreplica='Yes'and j.production in (select prod.production from productionscontainer prod where prod.processingid= :new.processingid));
end;
/
ALTER TRIGGER RUNQUALITY ENABLE;

CREATE TABLE PROCESSING 
   (	ID NUMBER, 
	PARENTID NUMBER, 
	NAME VARCHAR2(256 BYTE), 
	 CONSTRAINT PROCESSING_PK PRIMARY KEY (ID)
	 CONSTRAINT PROCESSING_FK FOREIGN KEY (PARENTID)
	  REFERENCES PROCESSING (ID));

  CREATE INDEX PROCESSING_PID ON PROCESSING (PARENTID); 
  
  CREATE INDEX PROCESSING_PID_NAME ON PROCESSING (PARENTID, NAME);

  CREATE OR REPLACE EDITIONABLE TRIGGER PROCESSING_BEFORE_INSERT 
BEFORE INSERT
   ON processing
   FOR EACH ROW
DECLARE
BEGIN
    
  if INSTR(:new.name,'/') > 0 then
    RAISE_APPLICATION_ERROR(-20001,'The processing pass name can not contain / characther!!!');
  END if;
   
END;
/
ALTER TRIGGER PROCESSING_BEFORE_INSERT ENABLE;

CREATE TABLE PRODUCTIONOUTPUTFILES 
   (	PRODUCTION NUMBER, 
	STEPID NUMBER, 
	EVENTTYPEID NUMBER, 
	FILETYPEID NUMBER, 
	VISIBLE CHAR(1 BYTE) DEFAULT 'Y', 
	 CONSTRAINT PK_PRODUCTIONOUTPUTFILES_P PRIMARY KEY (PRODUCTION, STEPID, FILETYPEID, EVENTTYPEID, VISIBLE), 
	 CONSTRAINT FK_PRODUCTIONOUTPUTFILES_STEPS FOREIGN KEY (STEPID)
	  REFERENCES STEPS (STEPID), 
	 CONSTRAINT FK_PRODUCTIONOUTPUTFILES_EVT FOREIGN KEY (EVENTTYPEID)
	  REFERENCES EVENTTYPES (EVENTTYPEID), 
	 CONSTRAINT FK_PRODUCTIONOUTPUTFILES_FT FOREIGN KEY (FILETYPEID)
	  REFERENCES FILETYPES (FILETYPEID), 
	 CONSTRAINT FK_PRODUCTIONOUTPUTFILES_PROD FOREIGN KEY (PRODUCTION)
	  REFERENCES PRODUCTIONSCONTAINER (PRODUCTION) ON DELETE CASCADE
   );
 
 CREATE TABLE PRODUCTIONSCONTAINER 
   (	PRODUCTION NUMBER, 
	PROCESSINGID NUMBER, 
	SIMID NUMBER, 
	DAQPERIODID NUMBER, 
	TOTALPROCESSING VARCHAR2(256 BYTE), 
	CONFIGURATIONID NUMBER, 
	 CONSTRAINT PK_PRODUCTIONSCONTAINER PRIMARY KEY (PRODUCTION), 
	 CONSTRAINT FK1_PRODUCTIONSCONTAINER FOREIGN KEY (SIMID)
	  REFERENCES SIMULATIONCONDITIONS (SIMID), 
	 CONSTRAINT FK2_PRODUCTIONSCONTAINER FOREIGN KEY (DAQPERIODID)
	  REFERENCES DATA_TAKING_CONDITIONS (DAQPERIODID), 
	 CONSTRAINT FK_PRODUCTIONSCONTAINER_PROC FOREIGN KEY (PROCESSINGID)
	  REFERENCES PROCESSING (ID), 
	 FOREIGN KEY (CONFIGURATIONID)
	  REFERENCES CONFIGURATIONS (CONFIGURATIONID)
   );

  CREATE INDEX PRODCONTDAQ ON PRODUCTIONSCONTAINER (DAQPERIODID, PRODUCTION);

  CREATE INDEX PRODCONTPSIM ON PRODUCTIONSCONTAINER (SIMID, PRODUCTION);

  CREATE INDEX PRODCONT_PROC ON PRODUCTIONSCONTAINER (PROCESSINGID);

  CREATE INDEX PRODCONT_PROC_PROD ON PRODUCTIONSCONTAINER (PROCESSINGID, PRODUCTION);
  
  CREATE TABLE RUNSTATUS 
   (	RUNNUMBER NUMBER, 
	JOBID NUMBER, 
	FINISHED CHAR(1 BYTE) DEFAULT 'N', 
	 CONSTRAINT PK_RUNSTATUS PRIMARY KEY (RUNNUMBER, JOBID), 
	 CONSTRAINT FK_RUNSTATUS FOREIGN KEY (JOBID)
	  REFERENCES JOBS (JOBID)
   );
  
   CREATE OR REPLACE EDITIONABLE TRIGGER RUNSTATUS 
before update on runstatus
referencing new as new old as old
for each row
begin
   BOOKKEEPINGORACLEDB.updateLuminosity(:new.runnumber);
end;
/
ALTER TRIGGER RUNSTATUS ENABLE;

 CREATE TABLE RUNTIMEPROJECTS 
   (	STEPID NUMBER, 
	RUNTIMEPROJECTID NUMBER, 
	 CONSTRAINT RUNTIMEPROJECT_PK PRIMARY KEY (RUNTIMEPROJECTID, STEPID), 
	 CONSTRAINT RUNTIMEPROJECT_FK1 FOREIGN KEY (STEPID)
	  REFERENCES STEPS (STEPID), 
	 CONSTRAINT RUNTIMEPROJECT_FK2 FOREIGN KEY (RUNTIMEPROJECTID)
	  REFERENCES STEPS (STEPID) 
   );
   
    CREATE TABLE SIMULATIONCONDITIONS 
   (	SIMID NUMBER, 
	SIMDESCRIPTION VARCHAR2(256 BYTE), 
	BEAMCOND VARCHAR2(256 BYTE), 
	BEAMENERGY VARCHAR2(256 BYTE), 
	GENERATOR VARCHAR2(256 BYTE), 
	MAGNETICFIELD VARCHAR2(256 BYTE), 
	DETECTORCOND VARCHAR2(256 BYTE), 
	LUMINOSITY VARCHAR2(256 BYTE), 
	G4SETTINGS VARCHAR2(256 BYTE) DEFAULT ' ', 
	VISIBLE CHAR(1 BYTE) DEFAULT 'Y', 
	INSERTTIMESTAMPS TIMESTAMP (6) DEFAULT sys_extract_utc(systimestamp), 
	 CONSTRAINT SIMCOND_PK PRIMARY KEY (SIMID), 
	 CONSTRAINT SIMDESC UNIQUE (SIMDESCRIPTION), 
	 CHECK (visible in ('N','Y'))
   );
   
   
  CREATE TABLE STEPS 
   (	STEPID NUMBER, 
	STEPNAME VARCHAR2(256 BYTE), 
	APPLICATIONNAME VARCHAR2(128 BYTE) NOT NULL DISABLE, 
	APPLICATIONVERSION VARCHAR2(128 BYTE) NOT NULL DISABLE, 
	OPTIONFILES VARCHAR2(1000 BYTE), 
	DDDB VARCHAR2(256 BYTE), 
	CONDDB VARCHAR2(256 BYTE), 
	EXTRAPACKAGES VARCHAR2(256 BYTE), 
	INSERTTIMESTAMPS TIMESTAMP (6) DEFAULT sys_extract_utc(systimestamp), 
	VISIBLE CHAR(1 BYTE) DEFAULT 'Y', 
	INPUTFILETYPES FILETYPESARRAY , 
	OUTPUTFILETYPES FILETYPESARRAY, 
	PROCESSINGPASS VARCHAR2(256 BYTE), 
	USABLE VARCHAR2(10 BYTE) DEFAULT 'Not ready', 
	DQTAG VARCHAR2(256 BYTE), 
	OPTIONSFORMAT VARCHAR2(30 BYTE), 
	ISMULTICORE CHAR(1 BYTE) DEFAULT 'N', 
	SYSTEMCONFIG VARCHAR2(256 BYTE), 
	MCTCK VARCHAR2(256 BYTE), 
	 CHECK (visible in ('N','Y')), 
	 PRIMARY KEY (STEPID), 
	 CONSTRAINT S_PROCESSINGPASS CHECK (processingpass is not null), 
	 CHECK (USABLE='Yes' OR USABLE='Not ready' OR USABLE='Obsolete'), 
	 CHECK (isMulticore in ('N','Y'))
   );

  CREATE OR REPLACE EDITIONABLE TRIGGER STEP_INSERT 
before insert on steps
referencing new as new old as old
for each row
declare  
begin
if :new.DDDB ='NULL' or :new.DDDB = 'None' or :new.DDDB = '' then 
   :new.DDDB:=null; 
end if;
if :new.conddb='NULL' or :new.Conddb = 'None' or :new.Conddb = '' then
  :new.conddb := null;
end if;
end;
/
ALTER TRIGGER STEP_INSERT ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER STEPS_BEFORE_INSERT 
BEFORE INSERT
   ON steps
   FOR EACH ROW
DECLARE
BEGIN
    
  if INSTR(:new.processingpass,'/') > 0 then
    RAISE_APPLICATION_ERROR(-20001,'The processing pass name can not contain / characther!!!');
  END if;
   
END;
/
ALTER TRIGGER STEPS_BEFORE_INSERT ENABLE;

  CREATE OR REPLACE EDITIONABLE TRIGGER STEP_UPDATE 
before update on steps
referencing new as new old as old
for each row
declare  
rowcnt number;
begin
SELECT COUNT(*) INTO rowcnt from stepscontainer s where s.stepid=:new.stepid;
if rowcnt > 0 then 
   DBMS_OUTPUT.PUT_LINE('      Tag: '||:new.Visible||:old.stepname);
   :new.stepname:=:old.stepname;
   :new.applicationname:=:old.applicationname;
   :new.applicationversion:=:old.applicationversion;
   :new.optionfiles:=:old.optionfiles;
   :new.DDDB:=:old.DDDB;
   :new.conddb:=:old.conddb;
   :new.extrapackages:=:old.extrapackages; 
   :new.visible:=:old.visible;
   :new.inputfiletypes:=:old.inputfiletypes;
   :new.outputfiletypes:=:old.outputfiletypes;
   :new.processingpass:=:old.processingpass;
   --raise_application_error (-20999,'You are not allowed to modify already used steps!');
      
end if;
end;
/
ALTER TRIGGER STEP_UPDATE ENABLE;

CREATE TABLE STEPSCONTAINER 
   (	PRODUCTION NUMBER, 
	STEPID NUMBER, 
	STEP NUMBER, 
	EVENTTYPEID NUMBER, 
	 CONSTRAINT PK_STEPCONTAINER PRIMARY KEY (PRODUCTION, STEPID), 
	 CONSTRAINT FK_STEPCONTAINER FOREIGN KEY (STEPID)
	  REFERENCES STEPS (STEPID), 
	 CONSTRAINT FK_STEPSCONTAINER_EVENTTYPEID FOREIGN KEY (EVENTTYPEID)
	  REFERENCES EVENTTYPES (EVENTTYPEID)
   );

  CREATE INDEX STEPS_ID ON STEPSCONTAINER (STEPID);


