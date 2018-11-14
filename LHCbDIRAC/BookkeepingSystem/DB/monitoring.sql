select distinct final_blocking_session, final_blocking_instance from v$session;
select * from V$SESSION_LONGOPS where  SQL_ID=xxxxx;

select * from V$SESSION_LONGOPS where sid in (select sid from v$session where username='LHCB_DIRACBOOKKEEPING' and status='ACTIVE') and sofar!=totalwork;

select S.USERNAME, s.sid, s.osuser, t.sql_id, sql_text
from v$sqltext_with_newlines t,V$SESSION s
where t.address =s.sql_address
and t.hash_value = s.sql_hash_value
and s.status = 'ACTIVE'
and s.username <> 'SYSTEM'
order by s.sid,t.piece;

select
  object_name, 
  object_type, 
  session_id, 
  type,         -- Type or system/user lock
  lmode,        -- lock mode in which session holds lock
  request, 
  block, 
  ctime         -- Time since current mode was granted
from
  v$locked_object, all_objects, v$lock
where
  v$locked_object.object_id = all_objects.object_id AND
  v$lock.id1 = all_objects.object_id AND
  v$lock.sid = v$locked_object.session_id
order by
  session_id, ctime desc, object_name;
  
SELECT sid, to_char(start_time,'hh24:mi:ss') stime, 
message,( sofar/totalwork)* 100 percent 
FROM v$session_longops
WHERE sofar/totalwork < 1;

select s.username,s.sid,s.serial#,s.last_call_et/60 mins_running,q.sql_text from v$session s 
join v$sqltext_with_newlines q
on s.sql_address = q.address
 where status='ACTIVE'
and type <>'BACKGROUND'
and last_call_et> 60
order by sid,serial#,q.piece;

 SELECT index_name, last_analyzed, status
   FROM user_indexes
   WHERE index_name = 'PROD_CONFIG';

set pages 999
set lines 80

drop table t1;
create table t1 as select
   o.object_name    object_name,
   o.object_type    object_type,
   
   count(1)         num_blocks
from
   dba_objects  o,
   v$bh         bh
where
   o.object_id  = bh.objd
and
   o.owner not in ('SYS','SYSTEM')
group by
   o.object_name,
   o.object_type
order by
   count(1) desc;

column c1 heading "Object|Name"                 format a30
column c2 heading "Object|Type"                 format a12
column c3 heading "Number of|Blocks"            format 999,999,999,999
column c4 heading "Percentage|of object|data blocks|in Buffer" format 999

select
   object_name       c1,
   object_type       c2,
   num_blocks        c3,
   (num_blocks/decode(sum(blocks), 0, .001, sum(blocks)))*100 c4
from
   t1,
   dba_segments s
where
   s.segment_name = t1.object_name
and
   num_blocks > 10
group by
   object_name,
   object_type,
   num_blocks
order by
   num_blocks desc;

----start--
--******************************************************************
--   Contents of Data Buffers
--******************************************************************
set pages 999
set lines 92
 
ttitle 'Contents of Data Buffers'
 
drop table t1;
 
create table t1 as
select
   o.owner          owner,
   o.object_name    object_name,
   o.subobject_name subobject_name,
   o.object_type    object_type,
   count(distinct file# || block#)         num_blocks
from
   dba_objects  o,
   v$bh         bh
where
   o.data_object_id  = bh.objd
and
   o.owner not in ('SYS','SYSTEM')
and
   bh.status != 'free'
and 
  owner='LHCB_DIRACBOOKKEEPING'
group by
   o.owner,
   o.object_name,
   o.subobject_name,
   o.object_type
order by
   count(distinct file# || block#) desc
;
 
column c0 heading "Owner"                                    format a12
column c1 heading "Object|Name"                              format a30
column c2 heading "Object|Type"                              format a8
column c3 heading "Number of|Blocks in|Buffer|Cache"         format 99,999,999
column c4 heading "Percentage|of object|blocks in|Buffer"    format 999
column c5 heading "Buffer|Pool"                              format a7
column c6 heading "Block|Size"                               format 99,999
\
select
   t1.owner                                          c0,
   object_name                                       c1,
   case when object_type = 'TABLE PARTITION' then 'TAB PART'
        when object_type = 'INDEX PARTITION' then 'IDX PART'
        else object_type end c2,
   sum(num_blocks)                                     c3,
   (sum(num_blocks)/greatest(sum(blocks), .001))*100 c4,
   buffer_pool                                       c5,
   sum(bytes)/sum(blocks)                            c6
from
   t1,
   dba_segments s
where
   s.segment_name = t1.object_name
and
   s.owner = t1.owner
and
   s.segment_type = t1.object_type
and
   nvl(s.partition_name,'-') = nvl(t1.subobject_name,'-')
group by
   t1.owner,
   object_name,
   object_type,
   buffer_pool
having
   sum(num_blocks) > 10
order by
   sum(num_blocks) desc;
---end--
 SELECT name, value
FROM V$SYSSTAT
WHERE name IN ('db block gets from cache', 'consistent gets from cache', 
'physical reads cache');

SELECT name, physical_reads, db_block_gets, consistent_gets,
       1 - (physical_reads / (db_block_gets + consistent_gets)) "Hit Ratio"
  FROM V$BUFFER_POOL_STATISTICS;

SELECT TRUNC((1-(sum(decode(name,'physical reads',value,0))/
                (sum(decode(name,'db block gets',value,0))+
                (sum(decode(name,'consistent gets',value,0)))))
             )* 100) "Buffer Hit Ratio"
FROM v$SYSSTAT;

SELECT A.value + B.value  "logical_reads",
       C.value            "phys_reads",
       D.value            "phy_writes",
       ROUND(100 * ((A.value+B.value)-C.value) / (A.value+B.value))  
         "BUFFER HIT RATIO"
FROM V$SYSSTAT A, V$SYSSTAT B, V$SYSSTAT C, V$SYSSTAT D
WHERE
   A.statistic# = 37
AND
   B.statistic# = 38
AND
   C.statistic# = 39
AND
   D.statistic# = 40; 
   
column c1   heading 'Cache Size (meg)'   format 999,999,999,999  
 select
   size_for_estimate          c1,
   buffers_for_estimate       c2,
   estd_physical_read_factor  c3,
   estd_physical_reads        c4
from
   v$db_cache_advice
where
   name = 'DEFAULT'
and
   block_size  = (SELECT value FROM V$PARAMETER
                   WHERE name = 'db_block_size')
and
   advice_status = 'ON';
   
SELECT table_schema                                        "DB Name", 
   Round(Sum(data_length + index_length) / 1024 / 1024, 1) "DB Size in MB" 
FROM   information_schema.tables 
GROUP  BY table_schema; 

select sum(bytes)/1024/1024/1024/1024 size_in_TB from dba_data_files WHERE TABLESPACE_NAME like 'LHCB_DIRAC%';
select FILE_NAME, TABLESPACE_NAME, BLOCKS, ONLINE_STATUS, bytes/1024/1024/1024 size_in_GB from dba_data_files WHERE TABLESPACE_NAME like 'LHCB_DIRAC%';

select * from all_jobs;

EXECUTE DBMS_MVIEW.REFRESH('prodview', 'F');

BEGIN
  DBMS_SCHEDULER.RUN_JOB(
    JOB_NAME            => 'LHCB_DIRACBOOKKEEPING.PRODVIEW',
    USE_CURRENT_SESSION => FALSE);
END;

exec dbms_job.broken(59546, true);
exec dbms_job.broken(61726, true);

exec dbms_job.change(61746, NULL, next_date=>sysdate+2/24, interval=>'sysdate+3/24');
exec dbms_job.change(61726, NULL, next_date=>sysdate+3/24, interval=>'sysdate+2/24');


/*+ INDEX( files, F_GOTREPLICA) */
select /*+ INDEX( files, F_GOTREPLICA) */ files.fileid, files.jobid, files.EventTypeId, files.filetypeid, files.EVENTSTAT
from files
where
files.gotreplica='Yes' and
files.visibilityflag ='Y';
/
select * from table(dbms_xplan.display_cursor(format=>'allstats last +cost'));
 
select DBMS_STATS.GET_STATS_HISTORY_AVAILABILITY  from dual;
select TABLE_NAME, STATS_UPDATE_TIME from dba_tab_stats_history where table_name='FILES'; and owner='SYSTEM';

-- check current statistics
select table_name,partition_name,num_rows,cast(last_analyzed as timestamp),dbms_stats.get_prefs('PUBLISH',owner,table_name)
from dba_tab_statistics where owner='LHCB_DIRACBOOKKEEPING' and table_name in ('PRODUCTIONSCONTAINER','JOBS', 'FILES')
order by object_type desc,table_name,partition_name nulls first,partition_position;
exec dbms_stats.set_table_prefs('LHCB_DIRACBOOKKEEPING','PRODUCTIONSCONTAINER','publish','false');
exec dbms_stats.set_table_prefs('LHCB_DIRACBOOKKEEPING','JOBS','publish','false');
-- gather stats (then in pending mode)
exec dbms_stats.gather_table_stats('LHCB_DIRACBOOKKEEPING','PRODUCTIONSCONTAINER', cascade=>True);
exec dbms_stats.gather_table_stats('LHCB_DIRACBOOKKEEPING','JOBS',cascade=>True, degree=>16);
exec dbms_stats.gather_table_stats('LHCB_DIRACBOOKKEEPING','FILES',cascade=>True,degree=>16);
exec dbms_stats.GATHER_INDEX_STATS('LHCB_DIRACBOOKKEEPING','JOBS_PROD_CONFIG_JOBID',degree=>16);
exec dbms_stats.GATHER_INDEX_STATS('LHCB_DIRACBOOKKEEPING','F_GOTREPLICA',degree=>16);
exec dbms_stats.GATHER_INDEX_STATS('LHCB_DIRACBOOKKEEPING','FILES_JOB_EVENT_FILETYPE',degree=>16);

-- test with pending stats in my session
alter session set optimizer_use_pending_statistics=true;
--query to test
select /*+ gather_plan_statistics */ distinct jobs.Production, eventTypes.EventTypeId, eventTypes.Description,
configurations.Configname, configurations.Configversion,
productionscontainer. simid,productionscontainer.daqperiodid, files.filetypeid, jobs.programname, jobs.programversion
from eventTypes, files, jobs,configurations,productionscontainer
where eventTypes.eventTypeid=files.eventTypeid and
files.gotreplica='Yes' and files.visibilityflag ='Y' and jobs.jobid=files.jobid and jobs.configurationid=configurations.configurationId and jobs.production=productionscontainer.production
/
-- check execution plan
select * from table(dbms_xplan.display_cursor(format=>'allstats last +cost'));
-- end testing with pending statistics
alter session set optimizer_use_pending_statistics=false;
-- if not ok just delete the pending stats
exec dbms_stats.delete_pending_stats('LHCB_DIRACBOOKKEEPING_I','PRODUCTIONSCONTAINER');
exec dbms_stats.delete_pending_stats('LHCB_DIRACBOOKKEEPING_I','JOBS');
-- else if ok, publish them (invalidate all cursors immediately so that they use new statistics - if any regression better to see them immediately)
exec dbms_stats.publish_pending_stats('LHCB_DIRACBOOKKEEPING','PRODUCTIONSCONTAINER',no_invalidate=>false);
exec dbms_stats.publish_pending_stats('LHCB_DIRACBOOKKEEPING','JOBS',no_invalidate=>false);
-- at the end put back in publish mode
exec dbms_stats.set_table_prefs('LHCB_DIRACBOOKKEEPING','PRODUCTIONSCONTAINER','publish','true');
exec dbms_stats.set_table_prefs('LHCB_DIRACBOOKKEEPING','JOBS','publish','true');
-- check new statistics
select table_name,partition_name,num_rows,cast(last_analyzed as timestamp),dbms_stats.get_prefs('PUBLISH',owner,table_name)
from dba_tab_statistics where owner='LHCB_DIRACBOOKKEEPING' and table_name in ('PRODUCTIONSCONTAINER','JOBS', 'FILES')
order by object_type desc,table_name,partition_name nulls first,partition_position;
-- optionally check the difference (here from 1 day ago)
select report from table(dbms_stats.diff_table_stats_in_history('LHCB_DIRACBOOKKEEPING','PRODUCTIONSCONTAINER',sysdate-1,sysdate,0));
select report from table(dbms_stats.diff_table_stats_in_history('LHCB_DIRACBOOKKEEPING','JOBS',sysdate-1,sysdate));
-- in case of regression encountered later, restore the old stats:
exec dbms_stats.restore_table_stats('LHCB_DIRACBOOKKEEPING_','PRODUCTIONSCONTAINER',sysdate-1,no_invalidate=>false);
exec dbms_stats.restore_table_stats('LHCB_DIRACBOOKKEEPING_INT','JOBS',sysdate-1,no_invalidate=>false);
-- show the history of operations
select end_time,end_time-start_time,operation,target,notes,status
from DBA_OPTSTAT_OPERATIONS where target in ('LHCB_DIRACBOOKKEEPING.PRODUCTIONSCONTAINER','LHCB_DIRACBOOKKEEPING_INT.JOBS') and end_time>sysdate-1;
 
select name from v$database;
select * from v$locked_object join dba_objects using (object_id);
select * from v$locked_object join dba_objects using (object_id);

select * from user_tables;
select * from sys.aux_stats$;

set echo off
set linesize 200 pagesize 1000
column pname format a30
column sname format a20
column pval2 format a20
select pname,pval2 from sys.aux_stats$ where sname='SYSSTATS_INFO';
select pname,pval1,calculated,formula from sys.aux_stats$ where sname='SYSSTATS_MAIN'
model
  reference sga on (
    select name,value from v$sga
        ) dimension by (name) measures(value)
  reference parameter on (
    select name,decode(type,3,to_number(value)) value from v$parameter where name='db_file_multiblock_read_count' and ismodified!='FALSE'
    union all
    select name,decode(type,3,to_number(value)) value from v$parameter where name='sessions'
    union all
    select name,decode(type,3,to_number(value)) value from v$parameter where name='db_block_size'
        ) dimension by (name) measures(value)
partition by (sname) dimension by (pname) measures (pval1,pval2,cast(null as number) as calculated,cast(null as varchar2(60)) as formula) rules(
  calculated['MBRC']=coalesce(pval1['MBRC'],parameter.value['db_file_multiblock_read_count'],parameter.value['_db_file_optimizer_read_count'],8),
  calculated['MREADTIM']=coalesce(pval1['MREADTIM'],pval1['IOSEEKTIM'] + (parameter.value['db_block_size'] * calculated['MBRC'] ) / pval1['IOTFRSPEED']),
  calculated['SREADTIM']=coalesce(pval1['SREADTIM'],pval1['IOSEEKTIM'] + parameter.value['db_block_size'] / pval1['IOTFRSPEED']),
  calculated['   multi block Cost per block']=round(1/calculated['MBRC']*calculated['MREADTIM']/calculated['SREADTIM'],4),
  calculated['   single block Cost per block']=1,
  formula['MBRC']=case when pval1['MBRC'] is not null then 'MBRC' when parameter.value['db_file_multiblock_read_count'] is not null then 'db_file_multiblock_read_count' when parameter.value['_db_file_optimizer_read_count'] is not null then '_db_file_optimizer_read_count' else '= _db_file_optimizer_read_count' end,
  formula['MREADTIM']=case when pval1['MREADTIM'] is null then '= IOSEEKTIM + db_block_size * MBRC / IOTFRSPEED' end,
  formula['SREADTIM']=case when pval1['SREADTIM'] is null then '= IOSEEKTIM + db_block_size        / IOTFRSPEED' end,
  formula['   multi block Cost per block']='= 1/MBRC * MREADTIM/SREADTIM',
  formula['   single block Cost per block']='by definition',
  calculated['   maximum mbrc']=sga.value['Database Buffers']/(parameter.value['db_block_size']*parameter.value['sessions']),
  formula['   maximum mbrc']='= buffer cache size in blocks / sessions'
);
set echo on

If you query SYS.AUX_STATS$ on LHCBR you have MREADTIM=4.691 for multi-block read milliseconds, SREADTIM=2.189 for single-block, 
and MBRC=102 for the number of blocks in multiblock reads.
Then the average time per block in multi-block read is MREADTIM/MBRC and Oracle cost is in equivalent number of single blog reads, 
so MREADTIM/MBRC/SREADTIM is the 0.021 and this means that to read 1000 blocks the full table scan is estimated at cost=21 (whereas single block reads would be cost=1000). 
Same calculation on INT12R gives cost=179 for a 1000 block full scan.

select 
   sum(a.time_waited_micro)/sum(a.total_waits)/1000000 c1, 
   sum(b.time_waited_micro)/sum(b.total_waits)/1000000 c2,
   (
      sum(a.total_waits) / 
      sum(a.total_waits + b.total_waits)
   ) * 100 c3,
   (
      sum(b.total_waits) / 
      sum(a.total_waits + b.total_waits)
   ) * 100 c4,
  (
      sum(b.time_waited_micro) /
      sum(b.total_waits)) / 
      (sum(a.time_waited_micro)/sum(a.total_waits)
   ) * 100 c5 
from 
   dba_hist_system_event a, 
   dba_hist_system_event b
where 
   a.snap_id = b.snap_id
and 
   a.event_name = 'db file scattered read'
and 
   b.event_name = 'db file sequential read';
   
  select * from all_jobs;