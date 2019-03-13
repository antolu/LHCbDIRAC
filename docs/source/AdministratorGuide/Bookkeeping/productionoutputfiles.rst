.. _productionoutputfiles:

================================
Fill ProductionOutputFiles table
================================

The productionoutputfiles table is used for removing the materialized views (MV). It is introduced June 2017.
This document is describes about how the table propagated with some meaningful data.

Note: This document can be useful if we want to know what changes applied to the db.
Before we created a table for keeping track about the migration.

.. code-block:: sql

	create table prods as select distinct production from jobs where production not in (select production from productionoutputfiles);

- This table contains all productions, which are not in the productionoutputfiles table. The productions which are entered after June 2017 are
	already in this table

.. code-block:: sql

    alter table prods add processed char(1) default 'N' null;

- In order to keep which productions are inserted to the productionoutputfiles table.

.. code-block:: sql

	alter table prods add stepid char(1) default 'N' null;

- The all jobs which belong to this prod does not have stepid.

.. code-block:: sql

    alter table prods add problematic char(1) default 'N' null;

- Duplicated steps in the stepscontainer table.

The migration started with the runs (production<0) and with the prods.processed='N':

.. code-block:: sql

    declare
	begin
	FOR stcont in (select distinct ss.production from stepscontainer ss where ss.production in (select p.production from prods p where p.processed='N' and p.production<0)) LOOP
	  DBMS_OUTPUT.put_line (stcont.production);
	  FOR st in (select s.stepid, step from steps s, stepscontainer st where st.stepid=s.stepid and st.production=stcont.production order by step) LOOP
	    FOR f in (select distinct j.stepid,ft.name, f.eventtypeid, ft.filetypeid, f.visibilityflag from jobs j, files f, filetypes ft
	                  where ft.filetypeid=f.filetypeid and f.jobid=j.jobid and
	                  j.production=stcont.production and j.stepid=st.stepid and f.filetypeid not in (9,17) and eventtypeid is not null) LOOP
	      DBMS_OUTPUT.put_line (stcont.production||'->'||st.stepid||'->'||f.filetypeid||'->'||f.visibilityflag||'->'||f.eventtypeid);
	      BOOKKEEPINGORACLEDB.insertProdnOutputFtypes(stcont.production, st.stepid, f.filetypeid, f.visibilityflag,f.eventtypeid);
	      update prods set processed='Y' where production=stcont.production;
	    END LOOP;
	  END LOOP;
	  commit;
	END LOOP;
	END;
	/
	END LOOP;
	END;
	/

After I have noticed we have jobs without stepid. In order to fix this issue executed the following commands:

.. code-block:: sql

	create table stepscontainer_2018_09_20 as select * from stepscontainer;

- this is used for backup, because the duplicated entries will be deleted...

To fill the stepid for the non processed runs:

.. code-block:: sql

	declare
	found number;
	prname varchar2(256);
	prversion varchar2(256);
	prev_name varchar2(256);
	prev_version varchar2(256);
	rep number;
	begin
	FOR stcont in (select p.production from prods p where p.processed='N' and p.production<0) LOOP
	  found:=0;
	  select count(*) into found from jobs where production=stcont.production and stepid is null;
	  if found>0 then
	    prev_name:=null;
	    prev_version:=null;
	    for sts in (select stepid, step from stepscontainer where production=stcont.production order by step) LOOP
	      DBMS_OUTPUT.put_line ('Stepid'||sts.stepid||'Prod'||stcont.production);
	      select applicationname, applicationversion into prname,prversion from steps where stepid=sts.stepid;
	      if prev_name is null and prev_version is null then
	        prev_name:=prname;
	        prev_version:=prversion;
	        --DBMS_OUTPUT.put_line ('Update:'|| stcont.production);
	        update jobs set stepid=sts.stepid where programname=prname and programversion=prversion and production=stcont.production;
	        update prods set stepid='Y' where production=stcont.production;
	      elsif prev_name=prname and prev_version=prversion then
	         DBMS_OUTPUT.put_line ('Problematic:'|| stcont.production);
	         delete stepscontainer where production=stcont.production and stepid=sts.stepid;
	         update prods set problematic='Y' where production=stcont.production;
	      else
	        --DBMS_OUTPUT.put_line ('Update:'|| stcont.production);
	        update jobs set stepid=sts.stepid where programname=prname and programversion=prversion and production=stcont.production;
	        update prods set stepid='Y' where production=stcont.production;
	        prev_name:=prname;
	        prev_version:=prversion;
	      END if;
	    END LOOP
	    commit;
	  END if;
	END LOOP;
	END;
	/

After executing this procedure 21309 productions are fixed:

.. code-block:: sql

    select count(*) from prods where stepid='Y' and production<0;

Now we can add these productions to the productionoutputfiles table:

    Check how many runs are processed:

    .. code-block:: sql

        select count(*) from prods where processed='Y' and production<0;

    the result is 14026
    Check all the runs which are not processed:

    .. code-block:: sql

        select count(*) from prods where stepid='Y' and processed='N' and production<0; result is 21308

    Note: 21309!=21308 because I did a test before executing the procedure.

.. code-block:: sql

	declare
	begin
	FOR stcont in (select distinct ss.production from stepscontainer ss where ss.production in (select p.production from prods p where stepid='Y' and p.processed='N' and p.production<0)) LOOP
	  DBMS_OUTPUT.put_line (stcont.production);
	  FOR st in (select s.stepid, step from steps s, stepscontainer st where st.stepid=s.stepid and st.production=stcont.production order by step) LOOP
	    FOR f in (select distinct j.stepid,ft.name, f.eventtypeid, ft.filetypeid, f.visibilityflag from jobs j, files f, filetypes ft
	                  where ft.filetypeid=f.filetypeid and f.jobid=j.jobid and
	                  j.production=stcont.production and j.stepid=st.stepid and f.filetypeid not in (9,17) and eventtypeid is not null) LOOP
	      DBMS_OUTPUT.put_line (stcont.production||'->'||st.stepid||'->'||f.filetypeid||'->'||f.visibilityflag||'->'||f.eventtypeid);
	      BOOKKEEPINGORACLEDB.insertProdnOutputFtypes(stcont.production, st.stepid, f.filetypeid, f.visibilityflag,f.eventtypeid);
	      update prods set processed='Y' where production=stcont.production;
	    END LOOP;
	  END LOOP;
	  commit;
	END LOOP;
	END;
	/
	END LOOP;
	END;
	/

.. code-block:: sql

    select count(*) from prods where stepid='Y' and processed='N' and production<0;

the result is 260.
Checking one of the production -22595: this run does not has associated files.

The following script is used to fix the 260 problematic runs:

.. code-block:: sql

    DECLARE
    nbfiles number;
    BEGIN
    for prod in (select production from prods where stepid='Y' and processed='N' and production<0)
    LOOP
       select count(*) into nbfiles from jobs j, files f where j.jobid=f.jobid and j.production=prod.production and j.production<0;
       if nbfiles = 0 then
         DBMS_OUTPUT.put_line ('DELETE:'|| prod.production);
         delete runstatus where runnumber=-1 * prod.production;
         delete jobs where production<0 and production=prod.production;
         delete productionscontainer where production=prod.production;
         delete stepscontainer where production=prod.production;
         update prods set processed='Y' where production=prod.production;
         commit;
       END IF;
    END LOOP;
    END;
    /


After checking the result:

.. code-block:: sql

    SQL> select production from prods where stepid='Y' and processed='N' and production<0;

    PRODUCTION
    ----------
        -9

After this fix we check how many runs are not in the productionoutputfiles table:

.. code-block:: sql

    SQL> select count(*) from prods p where p.processed='N' and p.production<0;

    COUNT(*)
    ----------
       155

After checking the runs, we noticed the stepid is okay, but the runs do not have any files. For fixing:

.. code-block:: sql

    DECLARE
    nbfiles number;
    BEGIN
    for prod in (select production from prods where processed='N' and production<0)
    LOOP
       select count(*) into nbfiles from jobs j, files f where j.jobid=f.jobid and j.production=prod.production and j.production<0;
       if nbfiles = 0 then
         DBMS_OUTPUT.put_line ('DELETE:'|| prod.production);
         delete runstatus where runnumber=-1 * prod.production;
         delete jobs where production<0 and production=prod.production;
         delete productionscontainer where production=prod.production;
         delete stepscontainer where production=prod.production;
         update prods set processed='Y' where production=prod.production;
         commit;
       END IF;
    END LOOP;
    END;
    /

We can check how many runs are remained:

.. code-block:: sql

    SQL> select * from prods p where p.processed='N' and p.production<0;

    PRODUCTION P S P
    ---------- - - -
    -42854 N N N
        -9 N Y N

-9 can be deleted:

.. code-block:: sql

    SQL> select count(*) from jobs j, files f where j.jobid=f.jobid and j.production=-9 and f.gotreplica='Yes';

    COUNT(*)
    ----------
         0

The runs are almost fixed:

.. code-block:: sql

    SQL> select * from prods p where p.processed='N' and p.production<0;

    PRODUCTION P S P
    ---------- - - -
    -42854 N N N

Fixing the productions which are not in the stepscontainer:

.. code-block:: sql

    declare
    stepid number;
    stnum number;
    begin
    for prod in (select p.production from prods p where p.processed='N' and p.production>0 and p.production not in (select distinct ss.production from stepscontainer ss))
    LOOP
      stnum:=0;
      FOR jprod in (select j.programName, j.programVersion, f.filetypeid, ft.name, f.visibilityflag, f.eventtypeid from jobs j, files f, filetypes ft where ft.filetypeid=f.filetypeid and j.jobid=f.jobid and j.production=prod.production and j.stepid is null and f.filetypeid not in (9,17) and f.eventtypeid is not null group by j.programName, j.programVersion, f.filetypeid, ft.name, f.visibilityflag, f.eventtypeid
       Order by( CASE j.PROGRAMNAME WHEN 'Gauss' THEN '1' WHEN 'Boole' THEN '2' WHEN 'Moore' THEN '3' WHEN 'Brunel' THEN '4' WHEN 'Davinci' THEN '5' WHEN 'LHCb' THEN '6' ELSE '7' END))
      LOOP
        stnum:=stnum+1;
         DBMS_OUTPUT.put_line ('Production:'||prod.production||'  applicationname:'|| jprod.programname||'  APPLICATIONVERSION:'||jprod.programversion||stnum);
        select count(*) into stepid from steps s, table(s.outputfiletypes) o where s.applicationname=jprod.programname and s.APPLICATIONVERSION=jprod.programversion and o.name=jprod.name and o.visible=jprod.visibilityflag and ROWNUM<2;
        if stepid>0 then
          select s.STEPID into stepid from steps s, table(s.outputfiletypes) o where s.applicationname=jprod.programname and s.APPLICATIONVERSION=jprod.programversion and o.name=jprod.name and o.visible=jprod.visibilityflag and ROWNUM<2;
          --DBMS_OUTPUT.put_line ('Stepid:'|| stepid);
          BOOKKEEPINGORACLEDB.insertProdnOutputFtypes(prod.production, stepid, jprod.filetypeid, jprod.visibilityflag,jprod.eventtypeid);
          update prods set processed='Y', stepid='Y' where production=prod.production;
          update jobs j set j.stepid=stepid where j.production=prod.production and j.programname=jprod.programname and j.programversion=jprod.programversion;
          BOOKKEEPINGORACLEDB.insertStepsContainer(prod.production,stepid,stnum);
        else
          select count(*) into stepid from steps s, table(s.outputfiletypes) o where s.applicationname=jprod.programname and s.APPLICATIONVERSION=jprod.programversion and o.name=jprod.name and ROWNUM<2;
          if stepid > 0 then
            select s.stepid into stepid from steps s, table(s.outputfiletypes) o where s.applicationname=jprod.programname and s.APPLICATIONVERSION=jprod.programversion and o.name=jprod.name and ROWNUM<2;
            BOOKKEEPINGORACLEDB.insertProdnOutputFtypes(prod.production, stepid, jprod.filetypeid, jprod.visibilityflag,jprod.eventtypeid);
            update prods set processed='Y', stepid='Y' where production=prod.production;
            update jobs j set j.stepid=stepid where j.production=prod.production and j.programname=jprod.programname and j.programversion=jprod.programversion;
            BOOKKEEPINGORACLEDB.insertStepsContainer(prod.production,stepid,stnum);
          else
            --DBMS_OUTPUT.put_line ('insert');
            SELECT applications_index_seq.nextval into stepid from dual;
            insert into steps(stepid,applicationName,applicationversion, processingpass)values(stepid,jprod.programname,jprod.programversion,'FixedStep');
            BOOKKEEPINGORACLEDB.insertProdnOutputFtypes(prod.production, stepid, jprod.filetypeid, jprod.visibilityflag,jprod.eventtypeid);
            update prods set processed='Y', stepid='Y' where production=prod.production;
            update jobs j set j.stepid=stepid where j.production=prod.production and j.programname=jprod.programname and j.programversion=jprod.programversion;
            BOOKKEEPINGORACLEDB.insertStepsContainer(prod.production,stepid,stnum);
          END IF;
        END IF;
        commit;
      END LOOP;
    END LOOP;
    END;
    /

NOTE: The files which do not have event type it is not added to the productionoutputfiles...

.. code-block:: sql

    SQL> select * from prods p where p.processed='N' and p.production>0 and p.production not in (select distinct ss.production from stepscontainer ss);

    PRODUCTION P S P
    ---------- - - -
     52192 N N N

Added to the productionoutputfile:

.. code-block:: sql

    exec BOOKKEEPINGORACLEDB.insertProdnOutputFtypes(52192, 128808, 88, 'Y',11114044);
    exec BOOKKEEPINGORACLEDB.insertProdnOutputFtypes(52192, 129669, 121, 'Y',11114044);

Fix the remained productions:

.. code-block:: sql

    declare
    nb number;
    begin
    FOR stcont in (select distinct ss.production from stepscontainer ss where ss.production in (select p.production from prods p where p.processed='N' and p.production>0)) LOOP
      DBMS_OUTPUT.put_line (stcont.production);
      FOR st in (select s.stepid, step from steps s, stepscontainer st where st.stepid=s.stepid and st.production=stcont.production order by step) LOOP
        select count(*) into nb from jobs j, files f, filetypes ft where ft.filetypeid=f.filetypeid and f.jobid=j.jobid and j.production=stcont.production and j.stepid=st.stepid and f.filetypeid not in (9,17) and eventtypeid is not null;
        if nb=0 then
          update jobs set stepid=st.stepid where production=stcont.production;
          commit;
        END IF;
        FOR f in (select distinct j.stepid,ft.name, f.eventtypeid, ft.filetypeid, f.visibilityflag from jobs j, files f, filetypes ft
                      where ft.filetypeid=f.filetypeid and f.jobid=j.jobid and
                      j.production=stcont.production and j.stepid=st.stepid and f.filetypeid not in (9,17) and eventtypeid is not null) LOOP
            DBMS_OUTPUT.put_line (stcont.production||'->'||st.stepid||'->'||f.filetypeid||'->'||f.visibilityflag||'->'||f.eventtypeid);
            BOOKKEEPINGORACLEDB.insertProdnOutputFtypes(stcont.production, st.stepid, f.filetypeid, f.visibilityflag,f.eventtypeid);
            update prods set processed='Y' where production=stcont.production;
        END LOOP;
      END LOOP;
      commit;
    END LOOP;
    END;
    /

.. code-block:: sql

    select * from prods where processed='N';

    PRODUCTION P S P
    ---------- - - -
     24179 N N N
    -42854 N N N

Two production are problematic. The eventtypeid is null for 24179. -42854 is not yet deleted...

==================
Consistency checks
==================
We run some consistent checks in order to make sure the productionoutputfiles table correctly filled.

.. code-block:: sql

	declare
	counter number;
	nb number;
	begin
	counter:=0;
	for p in (select production,EVENTTYPEID,FILETYPEID, programname, programversion, simid, daqperiodid from prodview)LOOP
	   if p.simid>0 then
	    select count(*) into nb from productionoutputfiles prod, productionscontainer ct, steps s where ct.production=prod.production and
	     prod.production=p.production and prod.filetypeid=p.filetypeid and prod.eventtypeid=p.eventtypeid and prod.gotreplica='Yes' and prod.Visible='Y' and
	     ct.simid=p.simid and s.stepid=prod.stepid and s.applicationname=p.programname and s.applicationversion=p.programversion;
	    else
	     select count(*) into nb from productionoutputfiles prod, productionscontainer ct, steps s where ct.production=prod.production and
	     prod.production=p.production and prod.filetypeid=p.filetypeid and prod.eventtypeid=p.eventtypeid and prod.gotreplica='Yes' and prod.Visible='Y' and
	     ct.daqperiodid=p.daqperiodid and s.stepid=prod.stepid and s.applicationname=p.programname and s.applicationversion=p.programversion;
	   end if;
	   if nb=0 then
	    DBMS_OUTPUT.put_line (nb||' '||p.production||'  '||p.EVENTTYPEID||' '||p.FILETYPEID);
	    counter:=counter+1;
	   end if;
	   if nb>1 then
	    DBMS_OUTPUT.put_line ('DOUBLE:'||nb||' '||p.production||'  '||p.EVENTTYPEID||' '||p.FILETYPEID);
	   END IF;
	END LOOP;
	DBMS_OUTPUT.put_line ('COUNTER:'||counter);
	END;
	/

1035 production found.

The following script is used to fix the productions which are wrong in the productionoutputfiles tabe.

.. code-block:: sql

	declare
		counter number;
		nb number;
		begin
		counter:=0;
		for p in (select production,EVENTTYPEID,FILETYPEID, programname, programversion, simid, daqperiodid from prodview)
	    LOOP
	   if p.simid>0 then
	    select count(*) into nb from productionoutputfiles prod, productionscontainer ct, steps s where ct.production=prod.production and
	     prod.production=p.production and prod.filetypeid=p.filetypeid and prod.eventtypeid=p.eventtypeid and prod.gotreplica='Yes' and prod.Visible='Y' and
	     ct.simid=p.simid and s.stepid=prod.stepid;
	    else
	     select count(*) into nb from productionoutputfiles prod, productionscontainer ct, steps s where ct.production=prod.production and
	     prod.production=p.production and prod.filetypeid=p.filetypeid and prod.eventtypeid=p.eventtypeid and prod.gotreplica='Yes' and prod.Visible='Y' and
	     ct.daqperiodid=p.daqperiodid and s.stepid=prod.stepid;
	   end if;
	   if nb=0 then
	    for dat in (select j.production, J.STEPID, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag from
	        jobs j, files f where j.jobid=f.jobid and j.production=p.production and f.filetypeid not in (9,17) and
	        f.eventtypeid is not null GROUP BY j.production, j.stepid, f.eventtypeid, f.filetypeid, f.gotreplica, f.visibilityflag Order by f.gotreplica,f.visibilityflag asc)
	    LOOP
	     select count(*) into nb from productionoutputfiles where production=dat.production and
	        stepid=dat.stepid and filetypeid=dat.filetypeid and visible=dat.visibilityflag and
	        eventtypeid=dat.eventtypeid and gotreplica=dat.gotreplica;
	     if nb=0 then
	        DBMS_OUTPUT.put_line (nb||' '||p.production||'  '||p.EVENTTYPEID||' '||p.FILETYPEID);
	        select count(*) into nb from productionoutputfiles where production=dat.production and
	        stepid=dat.stepid and filetypeid=dat.filetypeid and visible=dat.visibilityflag and
	        eventtypeid=dat.eventtypeid;
	        if nb=0 then
	            INSERT INTO productionoutputfiles(production, stepid, filetypeid, visible, eventtypeid,gotreplica)VALUES(dat.production,dat.stepid, dat.filetypeid, dat.visibilityflag,dat.eventtypeid, dat.gotreplica);
	        else
	            update productionoutputfiles set gotreplica=dat.gotreplica where production=dat.production and
	        stepid=dat.stepid and filetypeid=dat.filetypeid and visible=dat.visibilityflag and
	        eventtypeid=dat.eventtypeid;
	        END IF;
	        counter:=counter+1;
	     end if;
	    END LOOP;
	   end if;
	   if nb>1 then
	    DBMS_OUTPUT.put_line ('DOUBLE:'||nb||' '||p.production||'  '||p.EVENTTYPEID||' '||p.FILETYPEID);
	   END IF;
	END LOOP;
	DBMS_OUTPUT.put_line ('COUNTER:'||counter);
	END;
	/