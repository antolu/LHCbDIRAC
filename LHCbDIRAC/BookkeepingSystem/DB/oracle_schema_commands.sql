drop type bulk_collect_directoryMetadata;
/
create or replace  type
directoryMetadata is object
(production number,
configname varchar2(256),
configversion  varchar2(256),
eventtypeid number,
filetype varchar2(256),
processingpass varchar2(256),
ConditionDescription varchar2(256),
VISIBILITYFLAG CHAR(1));
/
create or replace type bulk_collect_directoryMetadata is table of directoryMetadata;
/
drop type bulk_collect_directoryMet_new;
/
create or replace  type
directoryMetadata_new is object
(lfn varchar2(256),
production number,
configname varchar2(256),
configversion  varchar2(256),
eventtypeid number,
filetype varchar2(256),
processingpass varchar2(256),
ConditionDescription varchar2(256),
VISIBILITYFLAG CHAR(1));
/
create or replace type bulk_collect_directoryMet_new is table of directoryMetadata_new;
/
drop type metadata_table;
/
create or replace type metadata0bj is object (
  FILENAME varchar2(128),
  ADLER32 varchar2(256),
  CREATIONDATE timestamp(6),
  EVENTSTAT NUMBER,
  EVENTTYPEID NUMBER,
  Name varchar2(256),
  GOTREPLICA varchar2(3),
  GUID varchar2(256),
  MD5SUM varchar2(256),
  FILESIZE number,
  FullStat number, 
  DATAQUALITYFLAG varchar2(256), 
  jobid number(38,0), 
  runnumber number, 
  inserttimestamp timestamp(6),
  luminosity number,
  instluminosity number ,
  VISIBILITYFLAG CHAR(1)
 );
/
create or replace type metadata_table is table of metadata0bj;
/
create index jobs_fill_runnumber on jobs(fillnumber,runnumber);
create index djobid on jobs(diracjobid);

CREATE TABLE runstatus(runnumber NUMBER, JOBID NUMBER, FINISHED CHAR(1 BYTE) DEFAULT 'N',  
CONSTRAINT PK_runstatus PRIMARY KEY (Runnumber, JOBID),
CONSTRAINT FK_runstatus FOREIGN KEY(jobid) REFERENCES jobs(jobid));
insert into runstatus (runnumber, jobid, finished) select runnumber, jobid, 'Y' from jobs where production < 0 and runnumber is not null;
