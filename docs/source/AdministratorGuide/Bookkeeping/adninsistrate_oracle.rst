=======================================
LHCbBookkeeping database administration
=======================================

This document contains all the information needed to manage the Bookkeeping Oracle 
database.

Logon to the database
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

You can found it in the tar.gz file in the release in case if you want to update the prodction db.

NOTE: I recomend to use the stored procedure, which is in the tar.gz.

    1. Login the database using sqlplus
    
    2. in the termineal execute @/home/user/oracle_schema_storedprocedures.sql
    
    3. commit;
    
    In case of error you have to use 'show errors' command
    
    In case of a scheme change, you can found the command what needs to be executed: https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/blob/master/LHCbDIRAC/BookkeepingSystem/DB/oracle_schema_commands.sql
    

Discover slow queries in the db
===============================

Note: If you are not familiar with Oracle better to send a mail to phydb.support@cern.ch mailing list. You can write we have problem with the database the queries
are very slow. IT/DB expert will found the slow queries and will probaly tell what is the problem and try to solve.

https://cern.ch/session-manager is a portal provided by IT/DB where you can logon and found the running query. You can found the query which is running very long. You can get the 
execution plan and also can take the query and run in sqlplus. So you can compare the execution plan which is in the web and in sqlplus.

Login to the session manager:

    You have a reader and writer account. All the select queries are running in the reader account.
    
    Login form:
    
    
    UserName: LHCB_DIRACBOOKKEEPING_users
    
    Password: pass
    
    Database: LHCBR
    

How to identify problematic queries:

    
    You can found the queries which takes very long using https://cern.ch/session-manager, but it maybe normal. You can check the execution plan
    in the following way.
    
        1. Login to sqlplus (you have to use the main owner account)
        
        2. set autot traceo
        
        3. set timing on
        
        4. set linesize 1000
        
        5. execute the qurey
        
    After when the query will finish then you will have the execution plan and you will have the real execution time as well. I propose to look the following 
    parameters:
    
        Cost (%CPU) , consistent gets, physical reads
        
        For example:
        


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

    -You may have query which needs to read lot of data. In this case the consistent gets and physical reads are very high numbers. 
In that example if the consistemt gets and physical reads are very high for example more than 10k we have problem. This is because the query 
only returned 131 rows. 
    - TABLE ACCESS FULL is not good. You have to make sure that the query uses an index. This is not always true.
    -parallel execution you have to make sure if the query is running parallel, the processes does not send to much data between each other. If you run 
    a query parallel and the consistent gets is very high then you have a problem. Contact to oracle IT/DB if you do not know what to do...
    -CERTASIAN join: If you see that word in the execution plan, the query is wrong.

        
