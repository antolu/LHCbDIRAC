.. _administrate_oracle:

=======================================
LHCbBookkeeping database administration
=======================================

This document contains all the information needed to manage the Bookkeeping Oracle
database.

Login to the database
=====================

We are using 3 database accounts:

    1. LHCB_DIRACBOOKKEEPING_users (reader account)

    2. LHCB_DIRACBOOKKEEPING_server (write account)

    3. LHCB_DIRACBOOKKEEPING (main account)

The main account is always locked. Every time when you want to use it you have to unlock.

Before login to the development db, you have to unlock the database:

    https://cern.ch/service-db-actionmanagement

Important: you must only unlock the following account: lhcbr   lhcb_diracbookkeeping

You have two ways to login:

1. using sqldeveloper

    ConnectionName: give a name

    UserName: LHCB_DIRACBOOKKEEPING

    Password: xxxx

    Hostname: itrac50087-v.cern.ch

    Port: 10121

    Service name: lhcb_diracbookkeeping.cern.ch

 You can retreive the information using the tns.ora. You can found it:

    vi $TNS_ADMIN/phydb/tnsnames.phydb.ora

    You have to search to LHCB_DIRACBOOKKEEPING and after you will found the Service name and Hostname


 2. using sqlplus

    source /afs/cern.ch/project/oracle/script/setoraenv.csh

    setoraenv -s 12101

    sqlplus LHCB_DIRACBOOKKEEPING/password@LHCB_DIRACBOOKKEEPING

Compile oracle stored procedure
===============================

In order to compile the stored procedure you need the sql file: https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/blob/master/LHCbDIRAC/BookkeepingSystem/DB/oracle_schema_storedprocedures.sql

You can find it in the tar.gz file in the release in case you want to update the prodction db.

NOTE: I recommend to use the stored procedure, which is in the tar.gz.


    1. Login the database using sqlplus

    2. in the termineal execute @/home/user/oracle_schema_storedprocedures.sql

    3. commit;

    In case of error you have to use 'show errors' command

    In case of a schema change, you can find the command that needs to be executed: https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/blob/master/LHCbDIRAC/BookkeepingSystem/DB/oracle_schema_commands.sql


Discover slow queries in the db
===============================
Note: If you are not familiar with Oracle better to send a mail to phydb.support@cern.ch mailing list. You can write we have problem with the database the queries are very slow. IT/DB expert will find the slow queries and will probaly tell what is the problem and try to solve.

https://cern.ch/session-manager is a portal provided by IT/DB where you can logon and find the running query. You can find the query which is running very long. You can get the execution plan and also can take the query and run in sqlplus. So you can compare the execution plan which is in the web and in sqlplus.

Login to the session manager:

    You have a reader and writer account. All the select queries are running in the reader account.

    Login form:


    UserName: LHCB_DIRACBOOKKEEPING_users

    Password: pass

    Database: LHCBR


How to identify problematic queries:


    You can find the queries which takes very long using https://cern.ch/session-manager, but it maybe normal. You can check the execution plan
    in the following way.

        1. Login to sqlplus (you have to use the main owner account)

        2. set autot traceo

        3. set timing on

        4. set linesize 1000

        5. execute the qurey

After when the query will finish then you will have the execution plan and you will have the real execution time as well. I propose to look the following
parameters::

   Cost (%CPU) , consistent gets, physical reads

For example::



   Elapsed: 00:00:00.12
   Execution Plan
   ----------------------------------------------------------
   Plan hash value: 3340191443
   ---------------------------------------------------------------------------------------------------------------------
   | Id  | Operation              | Name     | Rows  | Bytes | Cost (%CPU)| Time     | Pstart| Pstop |
   ---------------------------------------------------------------------------------------------------------------------
   |   0 | SELECT STATEMENT          |          |  4960 |  2232K| 21217   (1)| 00:00:01 |       |     |
   |   1 |  NESTED LOOPS             |          |     |     |     |     |     |     |
   |   2 |   NESTED LOOPS            |          |  4960 |  2232K| 21217   (1)| 00:00:01 |       |     |
   |   3 |    PARTITION RANGE ALL          |          |  4897 |  1219K|  1619   (1)| 00:00:01 |     1 |  20 |
   |   4 |     TABLE ACCESS BY LOCAL INDEX ROWID| JOBS      |  4897 |  1219K|  1619   (1)| 00:00:01 |     1 |  20 |
   |*  5 |      INDEX RANGE SCAN           | PROD_CONFIG  |  4897 |     |  88   (0)| 00:00:01 |     1 |  20 |
   |   6 |    PARTITION RANGE ITERATOR        |          |   1 |     |   3   (0)| 00:00:01 |   KEY | KEY |
   |*  7 |     INDEX RANGE SCAN         | JOBS_REP_VIS |     1 |     |   3   (0)| 00:00:01 |   KEY | KEY |
   |   8 |   TABLE ACCESS BY LOCAL INDEX ROWID  | FILES     |   1 | 206 |   4   (0)| 00:00:01 |     1 |   1 |
   ---------------------------------------------------------------------------------------------------------------------
   Predicate Information (identified by operation id):
   ---------------------------------------------------
    5 - access("J"."PRODUCTION"=51073)
    7 - access("J"."JOBID"="F"."JOBID" AND "F"."GOTREPLICA"='Yes')
   Statistics
   ----------------------------------------------------------
    46  recursive calls
     0  db block gets
   508  consistent gets
    46  physical reads
        1452  redo size
       56603  bytes sent via SQL*Net to client
   640  bytes received via SQL*Net from client
    10  SQL*Net roundtrips to/from client
     1  sorts (memory)
     0  sorts (disk)
   131  rows processed


   Problems:
       - the cost is a big number.
       - the consistent gets is very high
       - physical reads are very high

Note:

    - You may have query which needs to read lot of data. In this case the consistent gets and physical reads are very high numbers. In that example if the consistent gets and physical reads are very high for example more than 10k we have problem. This is because the query only returned 131 rows.
    - TABLE ACCESS FULL is not good. You have to make sure that the query uses an index. This is not always true.
    - parallel execution you have to make sure if the query is running parallel, the processes does not send to much data between each other. If you run a query parallel and the consistent gets is very high then you have a problem. Contact to oracle IT/DB if you do not know what to do...
    - CARTESIAN join: If you see that word in the execution plan, the query is wrong.

=================================
Steps in the Bookkeeping database
=================================

Steps are used to process/produce data. The steps are used by the Production Management system and work flow. The steps are stored in the steps table which has the following columns::

   STEPID
   STEPNAME
   APPLICATIONNAME
   APPLICATIONVERSION
   OPTIONFILES
   DDDB
   CONDDB
   EXTRAPACKAGES
   INSERTTIMESTAMPS
   VISIBLE
   INPUTFILETYPES
   OUTPUTFILETYPES
   PROCESSINGPASS
   USABLE
   DQTAG
   OPTIONSFORMAT
   ISMULTICORE
   SYSTEMCONFIG
   MCTCK

The steps table has 3 triggers::

   STEP_INSERT: This trigger is used to replace NULL, None to an empty string.
   steps_before_insert: It checks that the processing pass contains a '/'.
   step_update: The steps which are already used can not be modified.

Modifying steps
===============

We may want to modify an already used steps. A step can be modified if the trigger is disabled. The following commands has to be performed in order to modify a step:

.. code-block:: sql

   alter trigger step_update disable;
   update steps set stepname='Reco16Smog for 2015 pA', processingpass='Reco16Smog' where stepid=129609; --an alternative is to used the StepManager page
   alter trigger step_update enable;

==================================
Processing pass in the Bookkeeping
==================================
The processing pass is a collection of steps. The processing pass is stored in the processing table::

   ID
   ParentID
   Name

The following example illustrates how to create a step:

.. code-block:: sql

    select max(id)+1 from processing;
    select * from processing where name='Real Data';
    insert into processing(id,parentid, name)values(1915,12,'Reco16Smog');

In this example we have created the following processing pass: /Real Data/Reco16Smog

The following query can be used to check the step:

.. code-block:: sql

    SELECT * FROM (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID
         FROM processing v   START WITH id in (select distinct id from processing where name='Real Data')
    CONNECT BY NOCYCLE PRIOR  id=parentid) v   where v.path='/Real Data/Reco16Smog';

If we know the processing id, we can use the following query to found out the processing pass:

.. code-block:: sql

    SELECT v.id,v.path FROM (SELECT distinct  LEVEL-1 Pathlen, SYS_CONNECT_BY_PATH(name, '/') Path, id
      FROM processing
      WHERE LEVEL > 0 and id=1915
      CONNECT BY PRIOR id=parentid order by Pathlen desc) v where rownum<=1;

=====================
Bookkeeping down time
=====================
The following services/agent needs to be stopped before the deep down time (SystemAdministrator can be used in order to manage the services)::

	RMS:
		RequestExecutingAgent
			check it really stops (may take long time)
	TS:
		BookkeepingWatchAgent
		TransformationAgent - Reco, DM, MergePlus (this to be checked). This was not stopped the latest deep downtime
		TransformationCleaningAgent
		MCSimulationTestingAgent
	PMS:
		ProductionStatusAgent
		RequestTrackingAgent
	DMS:
		PopularityAgent
	StorageHistoryAgents(s)

Just before the intervention stop all Bookkeeping services.


===============================================
Automatic updating of the productionoutputfiles
===============================================
Create an oracle periodic job:

.. code-block:: sql

  BEGIN
    DBMS_SCHEDULER.CREATE_JOB (
       job_name             => 'produpdatejob',
       job_type             => 'PLSQL_BLOCK',
       job_action           => 'BEGIN BKUTILITIES.updateProdOutputFiles(); END;',
       repeat_interval      => 'FREQ=MINUTELY; interval=10',
       start_date           => systimestamp,
       enabled              =>  TRUE
       );
    END;
    /

For monitoring:

.. code-block:: sql

    select JOB_NAME, STATE, LAST_START_DATE, LAST_RUN_DURATION, NEXT_RUN_DATE, RUN_COUNT, FAILURE_COUNT from USER_SCHEDULER_JOBS;

Debugging the produpdatejob in case of failure:

- sqlplus LHCB_DIRACBOOKKEEPING/xxxxx@LHCB_DIRACBOOKKEEPING
- set serveroutput on
- exec BKUTILITIES.updateProdOutputFiles();

You will see the problematic production, which you will need to fix. 

====================
Managing partitions
====================
`jobs` and `files` tables are partitioned. `jobs` table RANGE-HASH partitioned by `production` and `configurationid`. `files` table RANGE partitioned by `jobid`.

jobs table partitions
=====================

The last partitions called `prodlast` and `runlast`. The maximum value of the `prodlast` partition is MAXVALUE. This may require to split, if we see performance degradation.
This can happen if two many rows (`jobs`) belong to this partition. The recommended way to split the partition is to declare a down time, because when the partition split then the
non partitioned indexes become invalid. The non partitioned indexes needs to be recreated, which will block writing to the DB. 
The procedure for splitting the `prodlast` partition:

.. code-block:: sql

	select max(production) from jobs PARTITION(prodlast) where production!=99998;
	ALTER TABLE jobs SPLIT PARTITION prodlast AT (xxxxx) INTO (PARTITION prodXXX, PARTITION prodlast);

One of the possibility is to split the `prodlast` using the last production, which can be retrieved using the following query above.
`xxxxx` is the result of the query. `prodXXX` is the last partition+1. For example:

.. code-block:: sql

	select max(production) from jobs PARTITION(prodlast) where production!=99998;

which result is 83013

.. code-block:: sql

	ALTER TABLE jobs SPLIT PARTITION prodlast AT (83013) INTO (PARTITION prod4, PARTITION prodlast);
	
Rebuild the non partitioned indexes:

.. code-block:: sql
   
   ALTER INDEX SYS_C00302478 REBUILD; 
   ALTER INDEX JOB_NAME_UNIQUE REBUILD;
   
files table partitions
======================

This table is RANGE partitioned by `jobid`, which can reach the maximum value of the existing partition. It this happen, the following error will appear::

	2018-07-30 01:12:00 UTC dirac-jobexec/UploadOutputData ERROR: Could not send Bookkeeping XML file to server:
	Unable to create file /lhcb/MC/2015/SIM/00075280/0000/00075280_00009971_1.sim ! ERROR: Excution failed.: (
	ORA-14400: inserted partition key does not map to any partition
	ORA-06512: at "LHCB_DIRACBOOKKEEPING.BOOKKEEPINGORACLEDB", line 976
	ORA-06512: at line 1

In order to fix the issue a new partition has to be created:

.. code-block:: sql

	alter table files add PARTITION SECT_0620M  VALUES LESS THAN (620000000);

===================
Database monitoring
===================

Various queries are available in the BookkeepingSystem/DB/monitoring.sql file. They can be used for discovering problems such as database locks, broken oracle jobs, sessions, used indexes, etc.

