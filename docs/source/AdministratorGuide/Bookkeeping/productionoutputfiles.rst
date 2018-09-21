.. _productionoutputfiles:


================================
Fill ProductionOutputFiles table 
================================

The productionoutputfiles table is used for removing the materialized views (MV). It is introduced June 2017.
This document is describes about how the table propagated with some meaningful data.

Note: This document can be useful if we want to know what changes applied to the db.
Before we created a table for keeping track about the migration.

	create table prods as select distinct production from jobs where production not in (select production from productionoutputfiles);
	- This table contains all productions, which are not in the productionoutputfiles table. The productions which are entered after June 2017 are
	already in this table
	
	alter table prods add processed char(1) default 'N' null;
	- In order to keep which productions are inserted to the productionoutputfiles table.
	alter table prods add stepid char(1) default 'N' null;
	- The all jobs which belong to this prod does not have jobid.
	alter table prods add problematic char(1) default 'N' null;
	- Duplicated steps in the stepscontainer table.  

The migration started with the runs (production<0) and of course with the prods.processed='N':

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

	create table stepscontainer_2018_09_20 as select * from stepscontainer;
	- this is used for backup, because the duplicated entries will be deleted...

To fill the stepid for the non processed runs:
	
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
	
After executing this procedure 21309 productions are fixed: select count(*) from prods where stepid='Y' and production<0;

Now we can add these productions to the productionoutputfiles table:
	
	Check how many runs are processed: select count(*) from prods where processed='Y' and production<0; the result is 14026
	Check all the runs which are not processed: select count(*) from prods where stepid='Y' and processed='N' and production<0; result is 21308
	Note: 21309!=21308 because I did a test before executing the procedure.
	
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
	
select count(*) from prods where stepid='Y' and processed='N' and production<0; the result is 260. 
Checking one of the production -22595: this run does not has associated files.