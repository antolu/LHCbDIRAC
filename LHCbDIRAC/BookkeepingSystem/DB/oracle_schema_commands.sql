drop type metadata_table;
create or replace
type metadata_table is table of metadata0bj;


create or replace
type metadata0bj is object (
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
create index jobs_fill_runnumber on jobs(fillnumber,runnumber);
create index djobid on jobs(diracjobid);
