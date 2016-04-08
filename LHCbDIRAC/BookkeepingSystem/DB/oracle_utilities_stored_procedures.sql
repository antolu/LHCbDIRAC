CREATE OR REPLACE package BKUTILITIES as

  type udt_RefCursor is ref cursor;
  --TYPE ifileslist is VARRAY(30) of varchar2(10);

  TYPE ifileslist IS TABLE OF VARCHAR2(30)
    INDEX BY PLS_INTEGER;

  TYPE numberarray  IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  TYPE varchararray IS TABLE OF VARCHAR2(256) INDEX BY PLS_INTEGER;
  TYPE bigvarchararray IS TABLE OF VARCHAR2(2000) INDEX BY PLS_INTEGER;
  
procedure updateNbevt(v_production number);
procedure updateJobNbofevt(v_jobid number);
procedure updateEventInputStat(v_production number, fixstripping BOOLEAN);
procedure updateEvtinpStat(v_jobid number, fixstripping BOOLEAN);

end;
/

CREATE OR REPLACE package body BKUTILITIES as
procedure updateNbevt(v_production number)is
begin
for c in (select j.jobid 
            from 
              jobs j 
                where 
                  j.production=v_production) 
 LOOP
  updateJobNbofevt(c.jobid);
 END LOOP;
end;

procedure updateJobNbofevt(v_jobid number)is
sumevt number;
begin

select sum(f.eventstat) into sumevt 
  from 
   jobs j, files f, inputfiles i 
   where 
      i.jobid=v_jobid and 
      i.fileid=f.fileid and 
      f.jobid=j.jobid  and 
      f.eventstat is not null and 
      f.filetypeid not in (select filetypeid from filetypes where name='RAW');
if sumevt > 0 then
  update jobs set numberofevents=sumevt where jobid=v_jobid;
  --for c in (select j.jobid 
  --            from jobs j, files f, inputfiles i 
  --              where 
  --                i.jobid=v_jobid and 
  --                i.fileid=f.fileid and 
  --                f.jobid=j.jobid and 
  --                f.eventstat is not null and f.filetypeid not in (select filetypeid from filetypes where name='RAW')) 
  --LOOP
 --   updateJobNbofevt(c.jobid);
  --END LOOP;
 End if;
end;

procedure updateEventInputStat(v_production number, fixstripping BOOLEAN) is
begin
if fixstripping = TRUE THEN
  FOR c in (select j.jobid from jobs j, files f where j.jobid=f.jobid and j.production=v_production)
    LOOP
      updateEvtinpStat(c.jobid, fixstripping);
    END LOOP;
ELSE
for c in (select j.jobid 
            from 
              jobs j, files f 
                where 
                  j.jobid=f.jobid and 
                  j.production=v_production and 
                  f.gotreplica='Yes' and 
                  f.visibilityflag='Y')
  LOOP
    updateEvtinpStat(c.jobid, fixstripping);
  END LOOP;
END IF;
end;

procedure updateEvtinpStat(v_jobid number, fixstripping BOOLEAN) is
sumevtinp number;
BEGIN
 if fixstripping=TRUE THEN
  select sum(j.eventinputstat) into sumevtinp from jobs j, files f, inputfiles i where i.jobid=v_jobid and i.fileid=f.fileid and f.jobid=j.jobid;
 ELSE
  select sum(f.eventstat) into sumevtinp from jobs j, files f, inputfiles i where i.jobid=v_jobid and i.fileid=f.fileid and f.jobid=j.jobid;
 END IF;
 IF sumevtinp > 0 THEN
    update jobs set eventinputstat=sumevtinp where jobid=v_jobid;
 END IF;
END;

END;
/