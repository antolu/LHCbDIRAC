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
  FILENAME varchar2(256),
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

alter table jobs add StepID number;
alter table jobs add constraint fk_jobs_stepid FOREIGN KEY (StepId) references steps(stepid);

CREATE OR REPLACE TRIGGER RUNQUALITY
before update or insert on newrunquality
referencing new as new old as old
for each row  
begin
   update files set files.qualityid= :new.qualityid where jobid in (select j.jobid from jobs j where j.runnumber= :new.runnumber and j.production<0);
   update files set files.qualityid= :new.qualityid where files.fileid in (select f.fileid from files f, jobs j where j.jobid=f.jobid and j.runnumber= :new.runnumber and f.gotreplica='Yes'and j.production in (select prod.production from productionscontainer prod where prod.processingid= :new.processingid));
end;


CREATE OR REPLACE TRIGGER RUNSTATUS
before update on runstatus
referencing new as new old as old
for each row  
begin
   BOOKKEEPINGORACLEDB.updateLuminosity(:new.runnumber);
end;
/


alter table jobs add WNMJFHS06 float;
create or replace  type jobMetadata is object(lfn varchar2(256),
  DiracJobId                  NUMBER,
  DiracVersion                VARCHAR2(256),
  EventInputStat              NUMBER,
  ExecTime                    FLOAT,
  FirstEventNumber            NUMBER,
  Location                    VARCHAR2(256),
  Name                        VARCHAR2(256),
  NumberOfEvents              NUMBER,
  StatisticsRequested         NUMBER,
  WNCPUPower                  VARCHAR2(256),
  CPUTime                     FLOAT,
  WNCache                     VARCHAR2(256),
  WNMemory                    VARCHAR2(256),
  WNModel                     VARCHAR2(256),
  WORKERNODE                  varchar2(256),
  WNCPUHS06                   FLOAT,
  jobid                       number,
  totalLuminosity             NUMBER,
  production                  NUMBER,
  ProgramName                 VARCHAR2(256),
  ProgramVersion              VARCHAR2(256),
  WNMJFHS06                   FLOAT);
 drop type bulk_collect_jobMetadata;
create or replace type bulk_collect_jobMetadata is table of jobMetadata;

ALTER TABLE productionscontainer ADD configurationid number;
ALTER TABLE productionscontainer ADD FOREIGN KEY (CONFIGURATIONID) REFERENCES configurations(CONFIGURATIONID);
