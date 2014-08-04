CREATE OR REPLACE package BOOKKEEPINGORACLEDB as

  type udt_RefCursor is ref cursor;
  --TYPE ifileslist is VARRAY(30) of varchar2(10);

  TYPE ifileslist IS TABLE OF VARCHAR2(30)
    INDEX BY PLS_INTEGER;

  TYPE numberarray  IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
  TYPE varchararray IS TABLE OF VARCHAR2(256) INDEX BY PLS_INTEGER;

procedure funny(a number);
function  ext return udt_RefCursor;
procedure getAvailableFileTypes(a_Cursor out udt_RefCursor );
function insertFileTypes( v_name varchar2, description varchar2,filetype varchar2) return number;
procedure getAvailableConfigurations(a_Cursor out udt_RefCursor);
procedure getStepsForSpecificIfiles(iftypes ifileslist, a_Cursor out udt_RefCursor);
procedure getStepsForSpecificOfiles(oftypes ifileslist, a_Cursor out udt_RefCursor);
procedure getStepsForIfiles(iftypes ifileslist , a_Cursor out udt_RefCursor);
procedure getStepsForOfiles(oftypes ifileslist, a_Cursor out udt_RefCursor);
procedure getAvailebleSteps(iftypes ifileslist , a_Cursor out udt_RefCursor); --I can delete
procedure getAvailebleStepsRealAndMC(iftypes ifileslist , a_Cursor out udt_RefCursor); --I can delete
function getStepsForFiletypes(iftypes lists, oftypes lists, match varchar2) return step_table PIPELINED;
function getProductionProcessingPass(prod number) return varchar2;
function getProductionPorcPassName(v_procid number) return varchar2;
function getProductionProcessingPassId(prod number) return number;
function getProcessingPassId(root varchar2, fullpath varchar2) return number;
procedure getAvailableEventTypes(a_Cursor out udt_RefCursor);
procedure getJobInfo(lfn varchar2, a_Cursor out udt_RefCursor);
procedure insertTag(V_name varchar2, V_tag varchar2);
function getDataQualityId(name varchar2) return number;
function getQFlagByRunAndProcId(rnumber number, procid number) return varchar2;
Procedure getRunByQflagAndProcId(procid number, flag number, a_Cursor out udt_RefCursor);
procedure getLFNsByProduction(prod number, a_Cursor out udt_RefCursor);
function getFileID(v_FileName VARCHAR2) RETURN number;
procedure getJobIdFromInputFiles(v_FileId number, a_Cursor out udt_RefCursor);
procedure getFNameFiDRepWithJID(v_jobid NUMBER, a_Cursor out udt_RefCursor);
procedure getFileAndJobMetadata( v_jobid NUMBER, prod BOOLEAN, a_Cursor out udt_RefCursor);
procedure checkfile(name varchar2, a_Cursor out udt_RefCursor);
function checkFileTypeAndVersion (v_NAME  VARCHAR2,  v_VERSION VARCHAR2) return number;
procedure checkEventType (v_EVENTTYPEID NUMBER, a_Cursor out udt_RefCursor);
function insertJobsRow (
     v_ConfigName                  VARCHAR2,
     v_ConfigVersion               VARCHAR2,
     v_DiracJobId                  NUMBER,
     v_DiracVersion                VARCHAR2,
     v_EventInputStat              NUMBER,
     v_ExecTime                    FLOAT,
     v_FirstEventNumber            NUMBER,
     v_JobEnd                      TIMESTAMP,
     v_JobStart                    TIMESTAMP,
     v_Location                    VARCHAR2,
     v_Name                        VARCHAR2,
     v_NumberOfEvents              NUMBER,
     v_Production                  NUMBER,
     v_ProgramName                 VARCHAR2,
     v_ProgramVersion              VARCHAR2,
     v_StatisticsRequested         NUMBER,
     v_WNCPUPower                  VARCHAR2,
     v_CPUTime                   FLOAT,
     v_WNCache                     VARCHAR2,
     v_WNMemory                    VARCHAR2,
     v_WNModel                     VARCHAR2,
     v_WorkerNode                  VARCHAR2,
     v_runNumber                   NUMBER,
     v_fillNumber                  NUMBER,
     v_WNCPUHS06                   FLOAT,
     v_totalLuminosity             NUMBER,
     v_tck                         VARCHAR2
  ) return number;

 function insertFilesRow (
    v_Adler32                         VARCHAR2,
    v_CreationDate                    TIMESTAMP,
    v_EventStat                       NUMBER,
    v_EventTypeId                     NUMBER,
    v_FileName                        VARCHAR2,
    v_FileTypeId                      NUMBER,
    v_GotReplica                      VARCHAR2,
    v_Guid                            VARCHAR2,
    v_JobId                           NUMBER,
    v_MD5Sum                          VARCHAR2,
    v_FileSize                        NUMBER,
    v_FullStat                        NUMBER,
    v_utc                             TIMESTAMP,
    dqflag                            VARCHAR2,
    v_luminosity                      NUMBER,
    v_instluminosity                  Number,
    v_visibilityFlag                  varchar2
  )return number;


procedure insertInputFilesRow (v_FileId NUMBER, v_JobId NUMBER);

procedure updateReplicaRow(v_fileID number,v_replica varchar2);
procedure deleteJob(v_jobid number);
procedure deleteInputFiles(v_jobid number);
procedure deletefile(v_fileid number);
procedure deleteSetpContiner( v_prod number);

function insertSimConditions(
   v_Simdesc                varchar2,
   v_BeamCond               varchar2,
   v_BeamEnergy             varchar2,
   v_Generator              varchar2,
   v_MagneticField          varchar2,
   v_DetectorCond           varchar2,
   v_Luminosity             varchar2,
   v_G4settings             varchar2,
   v_visible                varchar2
 )return number;

procedure getSimConditions(a_Cursor out udt_RefCursor);

function insertDataTakingCond(
     v_DESCRIPTION                                        VARCHAR2,
     v_BEAMCOND                                           VARCHAR2,
     v_BEAMENERGY                                         VARCHAR2,
     v_MAGNETICFIELD                                      VARCHAR2,
     v_VELO                                               VARCHAR2,
     v_IT                                                 VARCHAR2,
     v_TT                                                 VARCHAR2,
     v_OT                                                 VARCHAR2,
     v_RICH1                                              VARCHAR2,
     v_RICH2                                              VARCHAR2,
     v_SPD_PRS                                            VARCHAR2,
     v_ECAL                                               VARCHAR2,
     v_HCAL                                               VARCHAR2,
     v_MUON                                               VARCHAR2,
     v_L0                                                 VARCHAR2,
     v_HLT                                                VARCHAR2,
     v_VeloPosition                                       VARCHAR2
  ) return number;

procedure getFileMetaData(v_fileName varchar2, a_Cursor out udt_RefCursor);
function getFileMetaData2(iftypes lists) return metadata_table PIPELINED;
procedure getFileMetaData3(iftypes varchararray, a_Cursor out udt_RefCursor);
function fileExists(v_fileName varchar2)return number;
PROCEDURE inserteventTypes (v_Description VARCHAR2, v_EventTypeId NUMBER, v_Primary VARCHAR2);
Procedure updateEventTypes(v_Description VARCHAR2, v_EventTypeId NUMBER, v_Primary VARCHAR2);
procedure setFileInvisible(lfn varchar2);
procedure setFileVisible(lfn varchar2);
procedure getConfigsAndEvtType(prodId number, a_Cursor out udt_RefCursor);
procedure getJobsbySites(prodId number,a_Cursor out udt_RefCursor);
procedure getSteps(prodId number, a_Cursor out udt_RefCursor);
procedure getProductionInformation(prodId number, a_Cursor out udt_RefCursor);
procedure getNbOfFiles(prodId number, a_Cursor out udt_RefCursor);
procedure getSizeOfFiles(prodId number, a_Cursor out udt_RefCursor);
procedure getNumberOfEvents(prodId number, a_Cursor out udt_RefCursor);
procedure getJobsNb(prodId number, a_Cursor out udt_RefCursor);
procedure insertStepsContainer(v_prod number, v_stepid number, v_step number);
procedure insertproductionscontainer(v_prod number, v_processingid number, v_simid number, v_daqperiodid number);
procedure getEventTypes(cName varchar2, cVersion varchar2, a_Cursor out udt_RefCursor);
function  getRunNumber(lfn varchar2) return number;
procedure insertRunquality(run number, qid number,procid number);
procedure getRunNbAndTck(lfn varchar2, a_Cursor out udt_RefCursor);
procedure deleteProductionsCont(v_prod number);
procedure getRuns(c_name varchar2, c_version varchar2,  a_Cursor out udt_RefCursor);
function getRunProcPass(v_runNumber number) return run_proc_table;
procedure getRunQuality(runs numberarray , a_Cursor out udt_RefCursor);
procedure getTypeVesrsion(lfn varchar2, a_Cursor out udt_RefCursor);
procedure getRunFiles(v_runNumber number, a_Cursor out udt_RefCursor);
function getProcessedEvents(v_prodid number) return number;
function isVisible(v_stepid number) return number;
function isVisibleProd(v_prod number ) return number;
/*function getConfToBeUpdated return conf_id_name_vers_table PIPELINED;*/
procedure insertRuntimeProject(pr_stepid number, run_pr_stepid number);
procedure updateRuntimeProject(pr_stepid number, run_pr_stepid number);
procedure removeRuntimeProject(pr_stepid number);
procedure getDirectoryMetadata(f_name varchar2, a_Cursor out udt_RefCursor);
function getFilesForGUID(v_guid varchar2) return varchar2;
procedure updateDataQualityFlag(v_qualityid number, lfns varchararray);
procedure bulkcheckfiles(lfns varchararray,  a_Cursor out udt_RefCursor);
procedure bulkupdateReplicaRow(v_replica varchar2, lfns varchararray);
procedure bulkgetTypeVesrsion(lfns varchararray, a_Cursor out udt_RefCursor);
procedure setObsolete;
procedure getDirectoryMetadata_new(lfns varchararray, a_Cursor out udt_RefCursor);
procedure bulkJobInfo(lfns varchararray, a_Cursor out udt_RefCursor);
procedure bulkJobInfoForJobName(jobNames varchararray, a_Cursor out udt_RefCursor);
procedure bulkJobInfoForJobId(jobids numberarray, a_Cursor out udt_RefCursor);
end;
/


CREATE OR REPLACE package body BOOKKEEPINGORACLEDB as
function  ext return udt_RefCursor is
cur udt_RefCursor;
begin
open cur for
  select * from tab;

end;
-------------------------------------------------------------------------------------------------------------------------------
procedure getAvailableFileTypes(a_Cursor out udt_RefCursor )is
begin
open a_Cursor for
  select distinct filetypes.name,filetypes.description from filetypes order by filetypes.name;
end;
---------------------------------------------------------------------------------------------------------------------------------
function insertFileTypes( v_name varchar2, description varchar2,filetype varchar2) return number is
id number;
found number;
ecode    Varchar2(256);
thisproc CONSTANT VARCHAR2(50) := 'trap_errmesg';
found_name EXCEPTION;
descr varchar2(256);
begin
found := 0;
id := -1;
select count(filetypeid) into found from filetypes where filetypes.name=UPPER(v_name) and filetypes.version=filetype;
if found>0 then
  RAISE found_name;
else
select distinct DESCRIPTION into descr from filetypes where
           NAME=UPPER(v_name);
select max(filetypeid)+1 into id from filetypes;
insert into filetypes(filetypeid,name,description,version) values(id, UPPER(v_name),descr,filetype);
commit;
return id;
end if;
EXCEPTION
  WHEN found_name then
  raise_application_error(-20001,'The '||v_name || ' file type is already exist!!!');
  WHEN NO_DATA_FOUND then
   select max(filetypeid)+1 into id from filetypes;
   insert into filetypes(filetypeid,name,description,version) values(id,UPPER(v_name),description,filetype);
   commit;
  return id;
  WHEN OTHERS THEN
    ecode := SQLERRM; --SQLCODE;
    dbms_output.put_line(thisproc || ' - ' || ecode);
    return -1;
end;
-------------------------------------------------------------------------------------------------------------------------------
procedure getAvailableConfigurations(
    a_Cursor                    out udt_RefCursor
  )is
  begin
   open a_Cursor for
     select ConfigName,ConfigVersion from configurations;
  end;
---------------------------------------------------------------------------------------------------------------------------
procedure getStepsForSpecificIfiles(iftypes ifileslist , a_Cursor out udt_RefCursor)is
result BOOLEAN;
begin
if iftypes.COUNT = 0 then
insert into stepsTMP select s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion, s.OptionFiles, s.DDDb, s.condDb, s.extrapackages, s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mcTCK,
                            r.stepid, r.stepname, r.ApplicationName, r.ApplicationVersion, r.OptionFiles, r.DDDb, r.condDb, r.extrapackages, r.visible, r.processingpass, r.usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mcTCK
FROM steps s, steps r, runtimeprojects rr  where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.inputfiletypes is null;
else
--iftypes:=inputfileslist('Charm.DST','SDST');
 for c IN (select s.stepid, s.inputfiletypes from steps s, table(s.inputfiletypes) i where i.name=iftypes(1)) LOOP
  for i in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST loop
  --   DBMS_OUTPUT.PUT_LINE('      Tag: '||c.inputfiletypes.FIRST(i));
  --  DBMS_OUTPUT.PUT_LINE('      Tag: '||c.inputfiletypes(i).NAME);
    result:=iftypes(i)=c.inputfiletypes(i).NAME;
    EXIT WHEN not result;
  end loop;
  if result and iftypes.COUNT=c.inputfiletypes.LAST then
    insert into stepsTMP select  s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion, s.OptionFiles, s.DDDb, s.condDb, s.extrapackages, s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mcTCK,
                                 r.stepid, r.stepname, r.ApplicationName, r.ApplicationVersion, r.OptionFiles, r.DDDb, r.condDb, r.extrapackages, r.visible, r.processingpass, r.usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mcTCK
    FROM steps s,  steps r, runtimeprojects rr where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid;
    DBMS_OUTPUT.PUT_LINE('      COOL: '||c.stepid);
  end if;
end loop;
-- LOOP
    --inputf(i):=ftype(iftypes(i),'Y');
--    DBMS_OUTPUT.PUT_LINE('      Tag: '||iftypes(i));
-- END LOOP;
--for inputfiletypes in c1
--LOOP
   --result := inputf = iftypes;
--   IF result THEN
--      DBMS_OUTPUT.PUT_LINE('emp1 equal to emp2');
--   END IF;
--END LOOP;
end if;
open  a_Cursor for
  select * from stepsTMP;
end;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getStepsForFiletypes(iftypes lists, oftypes lists, match varchar2) return step_table PIPELINED is
input BOOLEAN;
output BOOLEAN;
BEGIN
IF iftypes.COUNT = 0 and oftypes.COUNT = 0 THEN
  FOR cur in (select s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion, s.OptionFiles, s.DDDb, s.condDb, s.extrapackages, s.visible, s.processingpass, s.usable, s.dqtag,s.optionsformat, s.isMulticore, s.systemconfig,s.mcTCK,
                     r.stepid as rid, r.stepname as rsname, r.ApplicationName as rappname, r.ApplicationVersion as rappver, r.OptionFiles as roptsf, r.DDDb as rdddb, r.condDb as rcondb, r.extrapackages as rextra,
                     r.visible as rvisi, r.processingpass as rproc, r.usable as rusab, r.dqtag as rdq,r.optionsformat as ropff, r.isMulticore as rmulticore, r.systemconfig as rsystemconfig, r.mcTCK as rmctck
  FROM steps s, steps r, runtimeprojects rr  where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.inputfiletypes is null and s.usable !='Obsolete') LOOP
  pipe row(stepobj(cur.stepid,cur.stepname,cur.ApplicationName, cur.ApplicationVersion, cur.OptionFiles, cur.DDDb, cur.condDb, cur.extrapackages, cur.visible, cur.processingpass, cur.usable, cur.dqtag, cur.optionsformat, cur.isMulticore, cur.systemconfig, cur.mcTCK,
            cur.rid, cur.rsname, cur.rappname, cur.rappver, cur.roptsf, cur.rdddb, cur.rcondb, cur.rextra, cur.rvisi, cur.rproc, cur.rusab,cur.rdq,cur.ropff, cur.rmulticore, cur.rsystemconfig, cur.rmctck));
  END LOOP;
ELSE
IF iftypes.COUNT>0 THEN
  FOR c IN (select s.stepid, s.inputfiletypes, s.outputfiletypes from steps s  where s.inputfiletypes is not null and s.usable!= 'Obsolete')
    LOOP
     --DBMS_OUTPUT.PUT_LINE('WHY!!? '||c.stepid);
     IF c.inputfiletypes is NOT NULL THEN
       IF match='YES' THEN
          IF c.inputfiletypes.COUNT != iftypes.COUNT THEN
             input:=FALSE;
          ELSE
          FOR i IN c.inputfiletypes.FIRST .. c.inputfiletypes.LAST LOOP
            IF i > iftypes.COUNT THEN
              input:= FALSE;
            ELSE
             input:=iftypes(i)=c.inputfiletypes(i).NAME;
            END IF;
            EXIT WHEN not input;
          END LOOP;
          END IF;
       ELSE
         input:=FALSE;
         FOR i in iftypes.FIRST .. iftypes.LAST LOOP
           FOR j in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST LOOP
             IF iftypes(i)=c.inputfiletypes(j).NAME THEN
	             input:=TRUE;
               EXIT;
             END IF;
           END LOOP;
         EXIT WHEN input;
         END LOOP;
       END IF;
     END IF;
     IF input THEN
       IF oftypes.COUNT > 0 THEN
         output:=FALSE;
         IF c.outputfiletypes is NOT NULL THEN
           IF match='YES' THEN
             IF c.outputfiletypes.COUNT != oftypes.COUNT THEN
                 output:=FALSE;
             ELSE
             FOR i in c.outputfiletypes.FIRST .. c.outputfiletypes.LAST LOOP
               if i > iftypes.COUNT THEN
                  output:=FALSE;
               ELSE
                 output:=oftypes(i)=c.outputfiletypes(i).NAME;
               END IF;
               EXIT WHEN not output;
             END LOOP;
             END IF;
           ELSE
             output:=FALSE;
             FOR i in oftypes.FIRST .. oftypes.LAST LOOP
               FOR j in c.outputfiletypes.FIRST .. c.outputfiletypes.LAST LOOP
                 IF oftypes(i)=c.outputfiletypes(j).NAME THEN
	           output:=TRUE;
                   EXIT;
                 END IF;
               END LOOP;
               EXIT WHEN output;
             END LOOP;
           END IF;
         END IF;
       ELSE
        OUTPUT := TRUE;
       END IF;
    IF input and output THEN
      DBMS_OUTPUT.PUT_LINE('Insert1: '||c.stepid);
      FOR cur in (select s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion, s.OptionFiles, s.DDDb, s.condDb, s.extrapackages, s.visible, s.processingpass, s.usable, s.dqtag,s.optionsformat, s.isMulticore, s.systemconfig,s.mcTCK,
                  r.stepid as rid, r.stepname as rsname, r.ApplicationName as rappname, r.ApplicationVersion as rappver, r.OptionFiles as roptsf, r.DDDb as rdddb, r.condDb as rcondb, r.extrapackages as rextra, r.visible as rvisi,
                  r.processingpass as rproc, r.usable as rusab, r.dqtag as rdq,r.optionsformat as ropff, r.isMulticore as rmulticore, r.systemconfig as rsysconfig, r.mctck as rmctck
        FROM steps s, steps r, runtimeprojects rr  where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid and s.usable!='Obsolete' ) LOOP
      pipe row(stepobj(cur.stepid,cur.stepname,cur.ApplicationName, cur.ApplicationVersion, cur.OptionFiles, cur.DDDb, cur.condDb, cur.extrapackages, cur.visible, cur.processingpass, cur.usable, cur.dqtag, cur.optionsformat, cur.isMulticore, cur.systemconfig, cur.mctck,
               cur.rid, cur.rsname, cur.rappname, cur.rappver, cur.roptsf, cur.rdddb, cur.rcondb, cur.rextra, cur.rvisi, cur.rproc, cur.rusab,cur.rdq,cur.ropff, cur.rmulticore, cur.rsysconfig, cur.mctck));
      END LOOP;
    END IF;
  END IF;
 END LOOP;
ELSE
  FOR c IN (select s.stepid, s.inputfiletypes, s.outputfiletypes from steps s where s.outputfiletypes is not null and s.usable!= 'Obsolete')
    LOOP
     output:=FALSE;
     IF c.outputfiletypes is NOT NULL THEN
       IF match='YES' THEN
         if c.outputfiletypes.COUNT!=oftypes.COUNT THEN
             output:=FALSE;
         ELSE
         FOR i IN c.outputfiletypes.FIRST .. c.outputfiletypes.LAST LOOP
           IF i > oftypes.COUNT THEN
             output:=FALSE;
           ELSE
             output:=oftypes(i)=c.outputfiletypes(i).NAME;
           END IF;
           EXIT WHEN not output;
         END LOOP;
         END IF;
       ELSE
        output:=FALSE;
        FOR i in oftypes.FIRST .. oftypes.LAST LOOP
          FOR j in c.outputfiletypes.FIRST .. c.outputfiletypes.LAST LOOP
            IF oftypes(i)=c.outputfiletypes(j).NAME THEN
	      output:=TRUE;
              EXIT;
            END IF;
          END LOOP;
          EXIT WHEN output;
        END LOOP;
       END IF;
     END IF;
     IF output THEN
       IF iftypes.COUNT > 0 THEN
         input:=FALSE;
         IF match='YES' THEN
           IF c.inputfiletypes.COUNT!=iftypes.COUNT THEN
              input:=FALSE;
           ELSE
           FOR j in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST LOOP
             IF j > iftypes.COUNT THEN
               input:=FALSE;
             ELSE
               input:=iftypes(j)=c.inputfiletypes(j).NAME;
             END IF;
             EXIT WHEN not output;
           END LOOP;
           END IF;
         ELSE
           input:=FALSE;
           FOR i in iftypes.FIRST .. iftypes.LAST LOOP
             FOR j in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST LOOP
               IF iftypes(i)=c.inputfiletypes(j).NAME THEN
	               input:=TRUE;
                 EXIT;
               END IF;
             END LOOP;
             EXIT WHEN input;
           END LOOP;
         END IF;
       ELSE
        input := TRUE;
       END IF;
    IF input and output THEN
      DBMS_OUTPUT.PUT_LINE('Insert2: '||c.stepid);
      FOR cur2 in (select s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion, s.OptionFiles, s.DDDb, s.condDb, s.extrapackages, s.visible, s.processingpass, s.usable, s.dqtag,s.optionsformat, s.isMulticore, s.systemconfig,s.mctck,
                          r.stepid as rid, r.stepname as rsname, r.ApplicationName as rappname, r.ApplicationVersion as rappver, r.OptionFiles as roptsf, r.DDDb as rdddb, r.condDb as rcondb, r.extrapackages as rextra, r.visible as rvisi,
                          r.processingpass as rproc, r.usable as rusab, r.dqtag as rdq,r.optionsformat as ropff, r.isMulticore as rmulticore, r.systemconfig as rsysconfig, r.mctck as rmctck
        FROM steps s, steps r, runtimeprojects rr  where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid and s.usable!='Obsolete') LOOP
        pipe row(stepobj(cur2.stepid,cur2.stepname,cur2.ApplicationName, cur2.ApplicationVersion, cur2.OptionFiles, cur2.DDDb, cur2.condDb, cur2.extrapackages, cur2.visible, cur2.processingpass, cur2.usable, cur2.dqtag, cur2.optionsformat, cur2.isMulticore, cur2.systemconfig,cur2.mctck,
                          cur2.rid, cur2.rsname, cur2.rappname, cur2.rappver, cur2.roptsf, cur2.rdddb, cur2.rcondb, cur2.rextra, cur2.rvisi, cur2.rproc, cur2.rusab,cur2.rdq,cur2.ropff, cur2.rmulticore, cur2.rsysconfig, cur2.rmctck));
      END LOOP;
    END IF;
  END IF;
 END LOOP;
END IF;
END IF;
END;
---------------------------------------------------------------------------------------------------------------------------
procedure getStepsForSpecificOfiles(oftypes ifileslist, a_Cursor out udt_RefCursor)is
result BOOLEAN;
begin
if oftypes.COUNT = 0 then
insert into stepsTMP select s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion,s.OptionFiles,s.DDDb, s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig,s.mctck,
     r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
    FROM steps s, steps r, runtimeprojects rr where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.outputfiletypes is null;
else
 for c IN (select s.stepid, s.outputfiletypes from steps s, table(s.outputfiletypes) i where i.name=oftypes(1)) LOOP
  for i in c.outputfiletypes.FIRST .. c.outputfiletypes.LAST loop
    result:=oftypes(i)=c.outputfiletypes(i).NAME;
    EXIT WHEN not result;
  end loop;
  if result and oftypes.COUNT=c.outputfiletypes.LAST then
    insert into stepsTMP select s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion,s.OptionFiles,s.DDDb, s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
     r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig , r.mctck
    FROM steps s, steps r, runtimeprojects rr where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid;
  end if;
end loop;
end if;
open  a_Cursor for
  select * from stepsTMP;
end;

---------------------------------------------------------------------------------------------------------------------------
procedure getStepsForIfiles(iftypes ifileslist , a_Cursor out udt_RefCursor)is
result BOOLEAN;
begin
if iftypes.COUNT = 0 then
insert into stepsTMP select s.stepid, s.stepname, s.ApplicationName, s.ApplicationVersion,s.OptionFiles,s.DDDb, s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
     r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
     FROM steps s,steps r, runtimeprojects rr
     where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.inputfiletypes is null;
else
 for c IN (select s.stepid, s.inputfiletypes from steps s, table(s.inputfiletypes)) LOOP
  for j in iftypes.FIRST .. iftypes.LAST loop
    result := False;
    for i in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST loop
      result:=iftypes(j)=c.inputfiletypes(i).NAME;
      exit when result;
    end LOOP;
  end loop;
  if result then
    insert into stepsTMP select s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
       FROM steps s, steps r, runtimeprojects rr
     where s.stepid=c.stepid and s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid;
  end if;
end loop;
end if;
open  a_Cursor for
  select distinct * from stepsTMP;
end;
--------------------------------------------------------------------------------------
procedure getStepsForOfiles(oftypes ifileslist, a_Cursor out udt_RefCursor) is
result BOOLEAN;
BEGIN
IF oftypes.COUNT = 0 THEN
  insert into stepsTMP select s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
     from steps s, steps r, runtimeprojects rr  where
 s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.outputfiletypes is null;
ELSE
  FOR c IN (SELECT s.stepid, s.outputfiletypes FROM steps s, table(s.outputfiletypes)) LOOP
    FOR j IN oftypes.FIRST .. oftypes.LAST LOOP
      result := False;
      FOR i in c.outputfiletypes.FIRST .. c.outputfiletypes.LAST LOOP
        result:=oftypes(j)=c.outputfiletypes(i).NAME;
        exit when result;
      END LOOP;
    END LOOP;
    IF result THEN
      INSERT INTO stepsTMP SELECT s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
    FROM steps s, steps r, runtimeprojects rr where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid;
    END IF;
  END LOOP;
END IF;
OPEN a_Cursor for
 select distinct * from stepsTMP;
end;
---------------------------------------------------------------------------------------------------------------------------
procedure getAvailebleSteps(iftypes ifileslist, a_Cursor out udt_RefCursor)is
result BOOLEAN;
begin
if iftypes.COUNT = 0 then
insert into stepsTMP select s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable,s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
      FROM steps s, steps r, runtimeprojects rr where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.inputfiletypes is null;
else
--iftypes:=inputfileslist('Charm.DST','SDST');
 for c IN (select s.stepid, s.inputfiletypes from steps s, table(s.inputfiletypes) i where i.name=iftypes(1)) LOOP
  for i in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST loop
  --   DBMS_OUTPUT.PUT_LINE('      Tag: '||c.inputfiletypes.FIRST(i));
  --  DBMS_OUTPUT.PUT_LINE('      Tag: '||c.inputfiletypes(i).NAME);
    result:=iftypes(i)=c.inputfiletypes(i).NAME;
    EXIT WHEN not result;
  end loop;
  if result and iftypes.COUNT=c.inputfiletypes.LAST then
    insert into stepsTMP select s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
    FROM steps s, steps r, runtimeprojects rr where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid;
    DBMS_OUTPUT.PUT_LINE('      COOL: '||c.stepid);
  end if;
end loop;
-- LOOP
    --inputf(i):=ftype(iftypes(i),'Y');
--    DBMS_OUTPUT.PUT_LINE('      Tag: '||iftypes(i));
-- END LOOP;
--for inputfiletypes in c1
--LOOP
   --result := inputf = iftypes;
--   IF result THEN
--      DBMS_OUTPUT.PUT_LINE('emp1 equal to emp2');
--   END IF;
--END LOOP;
end if;
open  a_Cursor for
  select * from stepsTMP;
end;
procedure  getAvailebleStepsRealAndMC(iftypes ifileslist , a_Cursor out udt_RefCursor)is
result BOOLEAN;
begin
if iftypes.COUNT = 0 then
insert into stepsTMP select s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
    FROM steps s, steps r, runtimeprojects rr where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.inputfiletypes is null;
else
 for c IN (select s.stepid, s.inputfiletypes from steps s, table(s.inputfiletypes)) LOOP
  for j in iftypes.FIRST .. iftypes.LAST loop
    result := False;
    for i in c.inputfiletypes.FIRST .. c.inputfiletypes.LAST loop
      result:=iftypes(j)=c.inputfiletypes(i).NAME;
      exit when result;
    end LOOP;
  end loop;
  if result then
    insert into stepsTMP select s.stepid, s.stepname, s.ApplicationName,s.ApplicationVersion,s.OptionFiles,s.DDDb,s.condDb,s.extrapackages,s.visible, s.processingpass, s.usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mctck,
    r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.isMulticore, r.systemconfig, r.mctck
    FROM steps s, steps r, runtimeprojects rr where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid and s.stepid=c.stepid;
  end if;
end loop;
end if;
open  a_Cursor for
  select distinct * from stepsTMP;
end;

function getProductionProcessingPass(prod number) return varchar2 is
retval varchar2(256);
ecode    NUMBER(38);
thisproc CONSTANT VARCHAR2(50) := 'trap_errmesg';
begin
 select v.path into retval from (SELECT distinct  LEVEL-1 Pathlen, SYS_CONNECT_BY_PATH(name, '/') Path
   FROM processing
   WHERE LEVEL > 0 and id = (select distinct processingid from productionscontainer prod where prod.production=prod)
   CONNECT BY NOCYCLE PRIOR id=parentid order by Pathlen desc) v where rownum<=1;
return retval;
EXCEPTION WHEN OTHERS THEN
raise_application_error(-20004, 'error found! The processing pass does not exists!');
--ecode := SQLERRM; --SQLCODE;
--dbms_output.put_line(thisproc || ' - ' || ecode);
return null;
end;
------------------------------------------------------------------------------------------------------------------------------------------------------
function getProductionProcessingPassId(prod number) return number is
result Number;
ecode    NUMBER(38);
thisproc CONSTANT VARCHAR2(50) := 'trap_errmesg';
begin
select distinct processingid into result from productionscontainer prod where prod.production=prod;
return result;
EXCEPTION WHEN OTHERS THEN
ecode := SQLERRM;
end;
----------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getAvailableEventTypes(a_Cursor out udt_RefCursor)is
begin
 open a_Cursor for
   select distinct EVENTTYPEID, DESCRIPTION from eventtypes;
end;
------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getJobInfo(
   lfn                             varchar2,
   a_Cursor                        out udt_RefCursor
 )is
 begin
  open a_Cursor for
   select  jobs.DIRACJOBID, jobs.DIRACVERSION, jobs.EVENTINPUTSTAT, jobs.EXECTIME, jobs.FIRSTEVENTNUMBER,jobs.LOCATION,  jobs.NAME, jobs.NUMBEROFEVENTS,
                 jobs.STATISTICSREQUESTED, jobs.WNCPUPOWER, jobs.CPUTIME, jobs.WNCACHE, jobs.WNMEMORY, jobs.WNMODEL, jobs.WORKERNODE, jobs.WNCPUHS06, jobs.jobid, jobs.totalluminosity, jobs.production, jobs.programName, jobs.programVersion
   from jobs,files
   where files.jobid=jobs.jobid and  files.filename=lfn;
 end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure insertTag(
    V_name                            varchar2,
    V_tag                             varchar2
 ) is
  tid number;
  begin
  select tags_index_seq.nextval into tid from dual;
  insert into tags(tagid,name, tag) values(tid, V_name, V_tag);
  COMMIT;
end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getProcessingPassId(root varchar2, fullpath varchar2) return number is
result number;
ecode number(38);
begin
result:=-1;
select distinct v.id into result from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID
FROM processing v   START WITH id in (select distinct id from processing where name=root)
CONNECT BY NOCYCLE PRIOR  id=parentid) v
where v.path=fullpath;
return  result;
EXCEPTION WHEN OTHERS THEN
ecode := SQLERRM;
end;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getDataQualityId(name varchar2) return number is
result number;
ecode number(38);
begin
result:=1;
select distinct qualityid into result from dataquality where dataqualityflag=name;
return result;
EXCEPTION WHEN OTHERS THEN
ecode := SQLERRM;
end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getQFlagByRunAndProcId(rnumber number, procid number) return varchar2 is
result varchar2(256);
ecode number(38);
begin
result:= -1;
select d.dataqualityflag into result  from dataquality d, newrunquality r where r.runnumber=rnumber and r.processingid=procid and d.qualityid=r.qualityid;
return  result;
EXCEPTION
WHEN NO_DATA_FOUND THEN
raise_application_error(-20014, 'The data quality does not exists in the newrunquality table!');
WHEN OTHERS THEN
ecode := SQLERRM;
end;

Procedure getRunByQflagAndProcId(procid number, flag number, a_Cursor                out udt_RefCursor ) is
begin
if flag is not null then
open a_Cursor for select runnumber   from newrunquality where processingid=procid and qualityid=flag;
else
open a_Cursor for select runnumber   from newrunquality where processingid=procid;
end if;
end;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getLFNsByProduction(
   prod                    number,
   a_Cursor                out udt_RefCursor
 )is
 begin
   open a_Cursor for
     select filename from files,jobs where jobs.jobid=files.jobid and
     jobs.jobid=files.jobid and jobs.production=prod;

/*   select filename from files join jobs on jobs.jobid=files.jobid and
     jobs.production=1622;
 */
 end;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getFileID(
    v_FileName VARCHAR2
 ) RETURN number is
 fid number;
 begin
  fid := 0;
  select files.fileid into fid from files where files.filename=v_FileName;
  return fid;
  EXCEPTION WHEN OTHERS THEN
  RETURN NULL;
 end;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getJobIdFromInputFiles(
   v_FileId                        number,
   a_Cursor                        out udt_RefCursor
 ) is
 begin
 open a_Cursor for
  select inputfiles.jobid from inputfiles where inputfiles.fileid=v_FileId;
 end;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getFNameFiDRepWithJID(
   v_jobid NUMBER,
   a_Cursor                        out udt_RefCursor
 ) is
 begin
  open a_Cursor for
   select files.fileName,files.fileid,files.gotreplica from files where files.jobid=v_jobid;
 end;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getFileAndJobMetadata(
   v_jobid NUMBER,
   prod BOOLEAN,
   a_Cursor                        out udt_RefCursor
 ) is
 begin
  if not prod  then
    open a_Cursor for
    select files.fileName,files.fileid,files.gotreplica, 0, files.eventstat,
           files.eventtypeid, files.luminosity, files.instLuminosity, filetypes.name from files, filetypes where files.filetypeid=filetypes.filetypeid and files.jobid=v_jobid;
  else
    open a_Cursor for
    select files.fileName,files.fileid,files.gotreplica, jobs.production, files.eventstat,
           files.eventtypeid, files.luminosity, files.instLuminosity, filetypes.name from files, jobs, filetypes where files.filetypeid=filetypes.filetypeid and jobs.jobid=files.jobid and files.jobid=v_jobid;
  end if;
 end;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure checkfile(
      name                            varchar2,
      a_Cursor                        out udt_RefCursor
 )is
 begin
   open a_Cursor for
    select fileId, jobId, filetypeid from files where filename=name;
 end;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function checkFileTypeAndVersion (
       v_NAME                          VARCHAR2,
       v_VERSION                       VARCHAR2
 ) return number is
 id number :=0;
 descr varchar2(256);
 begin
   select filetypeId into id from filetypes where
           NAME=v_NAME and
           version=v_VERSION;
   return id;
   EXCEPTION
    when TOO_MANY_ROWS THEN
     select min(filetypeid) into id from filetypes where NAME=v_NAME and version=v_VERSION; return id;
    WHEN OTHERS THEN
   select count(*) into id from filetypes where
           NAME=v_NAME;
   IF id > 0 then
   select distinct DESCRIPTION into descr from filetypes where
           NAME=v_NAME;
   select max(filetypeid)+1 into id from filetypes;
   insert into filetypes(filetypeid,name,description,version) values(id,v_NAME,descr,v_VERSION);
   commit;
   return id;
   else
     raise_application_error(-20013, 'File type does not exist!');
   end IF;
 end;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure checkEventType (
    v_EVENTTYPEID                  NUMBER,
    a_Cursor                        out udt_RefCursor
 )is
 begin
   open a_Cursor for
    select DESCRIPTION,PRIMARY from eventtypes where
      EVENTTYPEID=v_EVENTTYPEID;
 end;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function insertJobsRow (
     v_ConfigName                  VARCHAR2,
     v_ConfigVersion               VARCHAR2,
     v_DiracJobId                  NUMBER,
     v_DiracVersion                VARCHAR2,
     v_EventInputStat              NUMBER,
     v_ExecTime                    FLOAT,
     v_FirstEventNumber            NUMBER,
     v_JobEnd                      TIMESTAMP,
     v_JobStart                    TIMESTAMP,
     v_Location                    VARCHAR2,
     v_Name                        VARCHAR2,
     v_NumberOfEvents              NUMBER,
     v_Production                  NUMBER,
     v_ProgramName                 VARCHAR2,
     v_ProgramVersion              VARCHAR2,
     v_StatisticsRequested         NUMBER,
     v_WNCPUPower                  VARCHAR2,
     v_CPUTime                   FLOAT,
     v_WNCache                     VARCHAR2,
     v_WNMemory                    VARCHAR2,
     v_WNModel                     VARCHAR2,
     v_WorkerNode                  VARCHAR2,
     v_runNumber                   NUMBER,
     v_fillNumber                  NUMBER,
     v_WNCPUHS06                   FLOAT,
     v_totalLuminosity             NUMBER,
     v_tck                         VARCHAR2
  )return number is
  jid       number;
  configId  number;
  existInDB  number;
  ecode    Varchar2(256);
  begin
    configId := 0;
    select count(*) into existInDB from configurations where ConfigName=v_ConfigName and ConfigVersion=v_ConfigVersion;
    if existInDB=0 then
      select configurationId_seq.nextval into configId from dual;
      insert into configurations(ConfigurationId,ConfigName,ConfigVersion)values(configId, v_ConfigName, v_ConfigVersion);
      commit;
    else
     select configurationid into configId from configurations where ConfigName=v_ConfigName and ConfigVersion=v_ConfigVersion;
    end if;

    select jobId_seq.nextval into jid from dual;
     insert into jobs(
         JobId,
         ConfigurationId,
         DiracJobId,
         DiracVersion,
         EventInputStat,
         ExecTime,
         FirstEventNumber,
         JobEnd,
         JobStart,
         Location,
         Name,
         NumberOfEvents,
         Production,
         ProgramName,
         ProgramVersion,
         StatisticsRequested,
         WNCPUPower,
         CPUTime,
         WNCache,
         WNMemory,
         WNModel,
         WorkerNode,
         RunNumber,
         FillNumber,
         WNCPUHS06,
         TotalLuminosity,
         Tck)
   values(
          jid,
          configId,
          v_DiracJobId,
          v_DiracVersion,
          v_EventInputStat,
          v_ExecTime,
          v_FirstEventNumber,
          v_JobEnd,
          v_JobStart,
          v_Location,
          v_Name,
          v_NumberOfEvents,
          v_Production,
          v_ProgramName,
          v_ProgramVersion,
          v_StatisticsRequested,
          v_WNCPUPower,
          v_CPUTime,
          v_WNCache,
          v_WNMemory,
          v_WNModel,
          v_WorkerNode,
          v_runNumber,
          v_fillNumber,
          v_WNCPUHS06,
          v_totalLuminosity,
          v_tck);

  commit;
  return jid;
  EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    jid:=0;
    if v_Production < 0 then
      select j.jobid into jid from jobs j where j.runnumber=v_runNumber and j.production<0;
    ELSE 
       select j.jobid into jid from jobs j where j.name=v_Name and j.production=v_Production;
    END IF;

    if jid=0 THEN
      ecode:= SQLERRM;
      raise_application_error(ecode, 'It is not a run!');
    else
       update jobs set ConfigurationId=configId,
         DiracJobId=v_DiracJobId,
         DiracVersion=v_DiracVersion,
         EventInputStat=v_EventInputStat,
         ExecTime=v_ExecTime,
         FirstEventNumber=v_FirstEventNumber,
         JobEnd=v_JobEnd,
         JobStart=v_JobStart,
         Location=v_Location,
         Name=v_Name,
         NumberOfEvents=v_NumberOfEvents,
         Production=v_Production,
         ProgramName=v_ProgramName,
         ProgramVersion=v_ProgramVersion,
         StatisticsRequested=v_StatisticsRequested,
         WNCPUPower=v_WNCPUPower,
         CPUTime=v_CPUTime,
         WNCache=v_WNCache,
         WNMemory=v_WNMemory,
         WNModel=v_WNModel,
         WorkerNode=v_WorkerNode,
         FillNumber=v_fillNumber,
         WNCPUHS06=v_WNCPUHS06,
         TotalLuminosity=v_totalLuminosity,
         Tck=v_tck where jobid=jid;
      commit;
    return jid;
    END IF;
    return -1;
  end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function insertFilesRow (
    v_Adler32                         VARCHAR2,
    v_CreationDate                    TIMESTAMP,
    v_EventStat                       NUMBER,
    v_EventTypeId                     NUMBER,
    v_FileName                        VARCHAR2,
    v_FileTypeId                      NUMBER,
    v_GotReplica                      VARCHAR2,
    v_Guid                            VARCHAR2,
    v_JobId                           NUMBER,
    v_MD5Sum                          VARCHAR2,
    v_FileSize                        NUMBER,
    v_FullStat                      NUMBER,
    v_utc                             TIMESTAMP,
    dqflag                            VARCHAR2,
    v_luminosity                      NUMBER,
    v_instluminosity                   Number,
    v_visibilityFlag                  varchar2
  )return number is
  fid number;
  dqid number;
  Begin
    dqid:=1;
    select dataquality.qualityid into dqid from dataquality where dataquality.dataqualityflag=dqflag;
    select fileId_seq.nextval into fid from dual;
    insert into files (
                FileId,
                Adler32,
                CreationDate,
                EventStat,
                EventTypeId,
                FileName,
                FileTypeId,
                GotReplica,
                Guid,
                JobId,
                MD5Sum,
                FileSize,
                FullStat,
                Qualityid,
                inserttimestamp,
                Luminosity,
                InstLuminosity,
                VisibilityFlag
                )
           VALUES (
                fid,
                v_Adler32,
                v_CreationDate,
                v_EventStat,
                v_EventTypeId,
                v_FileName,
                v_FileTypeId,
                v_GotReplica,
                v_Guid,
                v_JobId,
                v_MD5Sum,
                v_FileSize,
                v_FullStat,
                dqid,
                v_utc,
                v_luminosity,
                v_instluminosity,
                v_visibilityFlag
                );
  COMMIT;
  return fid;
  EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    select fileid into fid from files where FileName=v_FileName;
    update files set Adler32=v_Adler32,
                CreationDate=v_CreationDate,
                EventStat=v_EventStat,
                EventTypeId=v_EventTypeId,
                FileTypeId=v_FileTypeId,
                Guid=v_Guid,
                JobId=v_JobId,
                MD5Sum=v_MD5Sum,
                FileSize=v_FileSize,
                FullStat=v_FullStat,
                Qualityid=dqid,
                inserttimestamp=v_utc,
                Luminosity=v_luminosity,
                InstLuminosity=v_instluminosity,
                VisibilityFlag=v_visibilityFlag where fileid=fid;
    return fid;
  end;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE insertInputFilesRow (v_FileId NUMBER, v_JobId NUMBER)is
begin
    insert into inputfiles(
         FileId,
         JobId
         ) VALUES(
                v_FileId,
                v_JobId);
  COMMIT;
  EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    DBMS_OUTPUT.PUT_LINE('The input file of the job is added: '|| v_JobId);
  end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure updateReplicaRow(
   v_fileID number,
   v_replica varchar2
  )is
  begin
   update files set inserttimestamp = sys_extract_utc(systimestamp),gotreplica=v_replica where fileid=v_fileID;
   commit;
  end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure deleteJob(
   v_jobid    number
 )is
  nbOfjobs number;
  prod number;
  begin
   select count(*) into nbOfjobs from jobs where jobs.production=(select production from jobs where jobid=v_jobid);
   if nbOfjobs=1 then
     delete productions where production=(select production from jobs where jobid=v_jobid);
   end if;
   delete jobs where jobid=v_jobid;
   commit;
  end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 procedure deleteInputFiles(
  v_jobid    number
 )is
  begin
   delete inputfiles where jobid=v_jobid;
   commit;
  end;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 procedure deletefile(
   v_fileid                number
 )is
  begin
   delete files where fileId=v_fileid;
   commit;
 end;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure deleteSetpContiner(
  v_prod number
  )is
   begin
   delete stepscontainer where production=v_prod;
   commit;
end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure deleteProductionsCont(
 v_prod number
  )is
   begin
   delete productionscontainer where production=v_prod;
   commit;
end;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function insertSimConditions(
   v_Simdesc                varchar2,
   v_BeamCond               varchar2,
   v_BeamEnergy             varchar2,
   v_Generator              varchar2,
   v_MagneticField          varchar2,
   v_DetectorCond           varchar2,
   v_Luminosity             varchar2,
   v_G4settings             varchar2,
   v_visible                varchar2
 )return number
 is
  simulId number;
 begin
  select simulationCondID_seq.nextval into simulId from dual;
  insert into simulationconditions(
               SimId,
               SIMDESCRIPTION,
               BeamCond,
               BeamEnergy,
               Generator,
               MagneticField,
               DetectorCond,
               Luminosity,
               G4settings,
               visible)values(simulId,v_Simdesc,v_BeamCond,v_BeamEnergy,v_Generator,v_MagneticField,v_DetectorCond,v_Luminosity,v_G4settings, v_visible);
  COMMIT;
  return simulId;
 end;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getSimConditions (
    a_Cursor                        out udt_RefCursor
    )is
   begin
     open a_Cursor for
       select * from simulationconditions where visible='Y' ORDER by simid desc;
   end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function insertDataTakingCond(
     v_DESCRIPTION                                        VARCHAR2,
     v_BEAMCOND                                           VARCHAR2,
     v_BEAMENERGY                                         VARCHAR2,
     v_MAGNETICFIELD                                      VARCHAR2,
     v_VELO                                               VARCHAR2,
     v_IT                                                 VARCHAR2,
     v_TT                                                 VARCHAR2,
     v_OT                                                 VARCHAR2,
     v_RICH1                                              VARCHAR2,
     v_RICH2                                              VARCHAR2,
     v_SPD_PRS                                            VARCHAR2,
     v_ECAL                                               VARCHAR2,
     v_HCAL                                               VARCHAR2,
     v_MUON                                               VARCHAR2,
     v_L0                                                 VARCHAR2,
     v_HLT                                                VARCHAR2,
     v_VeloPosition                                       VARCHAR2
  ) return number
  is
  daq       number;
  begin

      daq := 0;
      select simulationCondID_seq.nextval into daq from dual;
      if v_DESCRIPTION is null then
      insert /* APPEND */ into data_taking_conditions(DAQPERIODID, DESCRIPTION, BEAMCOND, BEAMENERGY, MAGNETICFIELD,
                                       VELO, IT, TT, OT, RICH1, RICH2, SPD_PRS, ECAL, HCAL, MUON, L0, HLT,VELOPOSITION)
                                      values(
                                         daq,
                                         'DataTaking'||daq,
                                         v_BEAMCOND,
                                         v_BEAMENERGY,
                                         v_MAGNETICFIELD,
                                         v_VELO,
                                         v_IT,
                                         v_TT,
                                         v_OT,
                                         v_RICH1,
                                         v_RICH2,
                                         v_SPD_PRS,
                                         v_ECAL,
                                         v_HCAL,
                                         v_MUON,
                                         v_L0,
                                         v_HLT,
                                         v_VeloPosition);
    COMMIT;
    else
       insert /* APPEND */ into data_taking_conditions(DAQPERIODID, DESCRIPTION, BEAMCOND, BEAMENERGY, MAGNETICFIELD,
                                       VELO, IT, TT, OT, RICH1, RICH2, SPD_PRS, ECAL, HCAL, MUON, L0, HLT,VELOPOSITION)
                                      values(
                                         daq,
                                         v_DESCRIPTION,
                                         v_BEAMCOND,
                                         v_BEAMENERGY,
                                         v_MAGNETICFIELD,
                                         v_VELO,
                                         v_IT,
                                         v_TT,
                                         v_OT,
                                         v_RICH1,
                                         v_RICH2,
                                         v_SPD_PRS,
                                         v_ECAL,
                                         v_HCAL,
                                         v_MUON,
                                         v_L0,
                                         v_HLT,
                                         v_VeloPosition);
    commit;
    end if;

    return (daq);
    EXCEPTION WHEN OTHERS THEN
    RETURN 0;
  end;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

procedure getFileMetaData(
   v_fileName              varchar2,
   a_Cursor                out udt_RefCursor
  )is
  begin
   open a_Cursor for
     select files.FILENAME,files.ADLER32,files.CREATIONDATE,files.EVENTSTAT,files.EVENTTYPEID,filetypes.Name,files.GOTREPLICA,files.GUID,files.MD5SUM,files.FILESIZE, files.FullStat, dataquality.DATAQUALITYFLAG, files.jobid, jobs.runnumber, files.inserttimestamp,files.luminosity,files.instluminosity from files,filetypes,dataquality,jobs where
         filename=v_fileName and
         jobs.jobid=files.jobid and
         files.filetypeid=filetypes.filetypeid and
         files.QUALITYID=DataQuality.qualityID;
  end;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getFileMetaData2(iftypes lists) return metadata_table PIPELINED
is
BEGIN
FOR j in iftypes.FIRST .. iftypes.LAST LOOP
  DBMS_OUTPUT.PUT_LINE('FileName: '|| iftypes(j));
  FOR cur in (select files.FILENAME,files.ADLER32,files.CREATIONDATE,files.EVENTSTAT,files.EVENTTYPEID,filetypes.Name,files.GOTREPLICA,files.GUID,files.MD5SUM,files.FILESIZE, files.FullStat, dataquality.DATAQUALITYFLAG, files.jobid, jobs.runnumber, files.inserttimestamp,files.luminosity,files.instluminosity from files,filetypes,dataquality,jobs where
         filename=iftypes(j) and
         jobs.jobid=files.jobid and
         files.filetypeid=filetypes.filetypeid and
         files.QUALITYID=DataQuality.qualityID) LOOP
        pipe row(metadata0bj(cur.FILENAME, cur.ADLER32,cur.CREATIONDATE,cur.EVENTSTAT, cur.EVENTTYPEID, cur.Name, cur.GOTREPLICA, cur.GUID, cur.MD5SUM, cur.FILESIZE, cur.FullStat, cur.DATAQUALITYFLAG, cur.jobid, cur.runnumber, cur.inserttimestamp, cur.luminosity, cur.instluminosity));
  END LOOP;
END LOOP;
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getFileMetaData3(iftypes varchararray, a_Cursor out udt_RefCursor)
is
lfnmeta metadata_table := metadata_table();
n integer := 0;
BEGIN
FOR j in iftypes.FIRST .. iftypes.LAST LOOP
  DBMS_OUTPUT.PUT_LINE('FileName: '|| iftypes(j));
  FOR cur in (select files.FILENAME,files.ADLER32,files.CREATIONDATE,files.EVENTSTAT,files.EVENTTYPEID,filetypes.Name,files.GOTREPLICA,files.GUID,files.MD5SUM,files.FILESIZE, files.FullStat, dataquality.DATAQUALITYFLAG, files.jobid, jobs.runnumber, files.inserttimestamp,files.luminosity,files.instluminosity from files,filetypes,dataquality,jobs where
         filename=iftypes(j) and
         jobs.jobid=files.jobid and
         files.filetypeid=filetypes.filetypeid and
         files.QUALITYID=DataQuality.qualityID) LOOP
 lfnmeta.extend;
 n:=n+1;
 lfnmeta (n):=metadata0bj(cur.FILENAME, cur.ADLER32,cur.CREATIONDATE,cur.EVENTSTAT, cur.EVENTTYPEID, cur.Name, cur.GOTREPLICA, cur.GUID, cur.MD5SUM, cur.FILESIZE, cur.FullStat, cur.DATAQUALITYFLAG, cur.jobid, cur.runnumber, cur.inserttimestamp, cur.luminosity, cur.instluminosity);
  END LOOP;
END LOOP;
open a_Cursor for select * from table(lfnmeta);
END;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function fileExists(
    v_fileName            varchar2
  )return number is
  fid number;
  begin
   select fileid into fid from files where filename=v_fileName;
  return (fid);
    EXCEPTION WHEN OTHERS THEN
    RETURN 0;

end;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROCEDURE inserteventTypes (
        v_Description           VARCHAR2,
        v_EventTypeId           NUMBER,
        v_Primary               VARCHAR2
 )
 is
 begin
   insert into eventtypes(Description,EventTypeId,Primary) values (v_Description, v_EventTypeId, v_Primary);
   commit;
 end;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Procedure  updateEventTypes(
        v_Description           VARCHAR2,
        v_EventTypeId           NUMBER,
        v_Primary               VARCHAR2
 )
 is
 begin
   update eventtypes set Description=v_Description, Primary=v_Primary where EventTypeId=v_EventTypeId;
   commit;
 end;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure setFileInvisible(
  lfn varchar2
 )is
 begin
  update files set visibilityFlag='N' where files.filename=lfn;
  commit;
 end;

procedure setFileVisible(
  lfn varchar2
 )is
 begin
  update files set visibilityFlag='Y' where files.filename=lfn;
  commit;
 end;

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getConfigsAndEvtType(
   prodId                  number,
   a_Cursor                out udt_RefCursor
  )is
  begin
    open a_Cursor for
      select distinct configName, ConfigVersion, eventtypeid from prodview where production=prodId;
    /*
     select distinct configurations.configName,configurations.ConfigVersion,files.eventtypeid from jobs,files,configurations where
       jobs.jobid=files.jobid and
       files.gotreplica='Yes' and
       jobs.production=prodId and
       configurations.configurationid=jobs.configurationid;*/
  end;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getJobsbySites(
   prodId                  number,
   a_Cursor                out udt_RefCursor
 )is
  begin
   open a_Cursor for
    select count(*), jobs.Location from jobs where production=prodId Group By Location;
  end;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getSteps(
   prodId                  number,
   a_Cursor                out udt_RefCursor
  )is
  begin
   open a_Cursor for
    select s.stepName, s.applicationname, s.applicationversion, s.optionfiles, s.dddb, s.conddb, s.extrapackages
      from steps s, stepscontainer prod where
      prod.stepid=s.stepid and
      prod.production=prodId order by prod.step;
  EXCEPTION
  WHEN OTHERS THEN
    raise_application_error(-20003, 'error found the production does not exists  in the productionscontainer table!');
  end;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getProductionInformation(
   prodId                  number,
   a_Cursor                out udt_RefCursor
  )is
  pid number;
  begin
   open a_Cursor for
     select distinct c.configName,c.ConfigVersion,f.eventtypeid,s.stepName, s.applicationname, s.applicationversion, s.optionfiles, s.dddb, s.conddb, s.extrapackages, prod.step
          from steps s, stepscontainer prod, jobs j, configurations c, files f where
           j.jobid=f.jobid and
           f.eventtypeid>0 and
           j.production=prodId and
           c.configurationid=j.configurationid and
           prod.stepid=s.stepid and
           prod.production=j.production order by prod.step;
  end;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getNbOfFiles( prodId                  number,
    a_Cursor                out udt_RefCursor
  )is
  total number;
  begin
   select /*+ INDEX(files FILES_JOB_EVENT_FILETYPE) */ count(*) into total from files, jobs where files.jobid=jobs.jobid and jobs.production=prodId;
   open a_Cursor for
     select /*+ INDEX(files FILES_JOB_EVENT_FILETYPE) */ count(*), filetypes.Name,total as TotalFiles from files, jobs,filetypes where
        files.jobid=jobs.jobid and
        jobs.production=prodId and
        filetypes.filetypeid=files.filetypeid GROUP By filetypes.NAME;
  end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getSizeOfFiles(
    prodId                  number,
    a_Cursor                out udt_RefCursor
  )is
  begin
  open a_Cursor for
    select /*+ INDEX(files FILES_JOB_EVENT_FILETYPE) */  sum(FILESIZE) from files,jobs where files.jobid=jobs.jobid and jobs.production=prodId;
  end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getNumberOfEvents(
    prodId                  number,
    a_Cursor                out udt_RefCursor
  )is
  begin
  open a_Cursor for
   select /*+ INDEX(files FILES_JOB_EVENT_FILETYPE) */ filetypes.name,sum(files.EVENTSTAT), files.eventtypeid, sum(jobs.eventinputstat) from files,jobs,filetypes where
            files.jobid=jobs.jobid and
            files.gotreplica='Yes' and
            jobs.production=prodId and
            filetypes.filetypeid=files.filetypeid GROUP by filetypes.name, files.eventtypeid;
  end;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getJobsNb(
    prodId            number,
    a_Cursor                out udt_RefCursor
  )is
  begin
  open a_Cursor for
    select count(*) from jobs where production=prodId;
end;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure insertStepsContainer(v_prod number, v_stepid number, v_step number)is
begin
insert into stepscontainer(production,stepid,step)values(v_prod, v_stepid, v_step);
commit;
EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
   dbms_output.put_line(v_prod || 'already in the steps container table');
raise_application_error(-20005, 'The production already exists in the steps container table!');
end;
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure insertproductionscontainer(v_prod number, v_processingid number, v_simid number, v_daqperiodid number) is
begin
insert into productionscontainer(production,processingid,simid,daqperiodid)values(v_prod, v_processingid, v_simid, v_daqperiodid);
commit;
EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
   dbms_output.put_line(v_prod || 'already in the steps container table');
   raise_application_error(-20005, 'The production already exists in the productionscontainer table!');
end;
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 procedure getEventTypes(
    cName                 varchar2,
    cVersion              varchar2,
    a_Cursor              out udt_RefCursor
   ) is
begin
  open a_Cursor for
    select distinct EVENTTYPEID, DESCRIPTION from prodview where CONFIGNAME=cName and CONFIGVERSION=cVersion ORDER By EVENTTYPEID DESC;
   end;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function  getRunNumber(lfn varchar2) return number is
id number;
begin
select jobs.runnumber into id from jobs,files where files.jobid=jobs.jobid and files.filename=lfn;
return id;
EXCEPTION
  WHEN OTHERS THEN
  return null;
end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure insertRunquality(run number, qid number, procid number) is
begin
insert into newrunquality(runnumber,qualityid, processingid) values(run,qid,procid);
commit;
EXCEPTION
  WHEN DUP_VAL_ON_INDEX THEN
    UPDATE newrunquality set qualityid=qid where processingid=procid and runnumber=run;
commit;
end;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getRunNbAndTck(lfn varchar2, a_Cursor out udt_RefCursor) is
begin
open a_Cursor for
  select jobs.runnumber, jobs.Tck from jobs,files where files.jobid=jobs.jobid and files.filename=lfn;
end;
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getRuns(c_name varchar2, c_version varchar2,  a_Cursor out udt_RefCursor) is
begin
open a_Cursor for
  select distinct prodrunview.runnumber from prodview, prodrunview where prodview.production=prodrunview.production and configname=c_name and configversion=c_version;
end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getRunProcPass(v_runNumber number) return run_proc_table
is
ret_tab run_proc_table := run_proc_table();
n integer := 0;
ret varchar2(256);
begin
  for r in (select distinct production from jobs where runnumber=v_runNumber and production>0)
    loop
      ret_tab.extend;
      n := n + 1;
      ret:=getProductionProcessingPass(r.production);
      ret_tab(n) := runnb_proc(v_runNumber,ret);
      end loop;
return ret_tab;
end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getRunQuality(runs numberarray , a_Cursor out udt_RefCursor)
is
ret_tab bulk_collect_run_quality_evt:= bulk_collect_run_quality_evt();
n integer := 0;
begin
FOR i in 1 .. runs.COUNT LOOP
 for record in (select distinct jobs.runnumber,dataquality.dataqualityflag,files. eventtypeid from files, jobs,dataquality where files.jobid=jobs.jobid and files.qualityid=dataquality.qualityid  and jobs.production<0 and jobs.runnumber=runs(i)) LOOP
  ret_tab.extend;
  n := n + 1;
  ret_tab(n):= runnb_quality_eventtype(record.runnumber,record.dataqualityflag,record.eventtypeid);
  END LOOP;
  END LOOP;
open a_Cursor for select * from table(ret_tab);
END;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getTypeVesrsion(lfn varchar2, a_Cursor out udt_RefCursor)
is
begin
open a_Cursor for select ftype.version from files f, filetypes ftype where f.filetypeid=ftype.filetypeid and f.filename=lfn;
end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getRunFiles(v_runNumber number, a_Cursor out udt_RefCursor)
is
begin
open a_Cursor for
select f.filename, f.gotreplica, f.filesize,f.guid, f.luminosity, f.INSTLUMINOSITY, f.eventstat, f.fullstat
from jobs j ,files f, filetypes ft
where j.jobid=f.jobid and ft.filetypeid=f.filetypeid and ft.name='RAW' and  j.production<0 and j.runnumber=v_runNumber;
end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getProcessedEvents(v_prodid number) return number
is
retVal number := 0;
begin
select sum(j.numberofevents) into retVal from jobs j, (select scont.production, s.applicationname, s.applicationversion
from stepscontainer scont, steps s
where scont.stepid = s.stepid and
scont.production=v_prodid and
scont.step=(select max(step) from stepscontainer where stepscontainer.production=v_prodid)) firsts where j.production=firsts.production and j.programname=firsts.applicationname and j.programversion=firsts.applicationversion;
return retVal;
EXCEPTION
  WHEN OTHERS THEN
    raise_application_error(-20005, 'error found during the event number calculation');
end;
function isVisible(v_stepid number) return number
is
vis char;
c number;
begin
select count(*) into c from TABLE(SELECT s.outputfiletypes FROM steps s WHERE s.stepid=v_stepid);
if c = 0 then
return v_stepid;
else
SELECT distinct visible into vis FROM TABLE(SELECT s.outputfiletypes FROM steps s WHERE s.stepid=v_stepid)
    WHERE ViSible='Y';
if vis='Y' then
return v_stepid;
else
return 0;
end if;
end if;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
    return -1;
end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function isVisibleProd(v_prod number) return number
is
sid number := 0;
res number := 0;
begin
select st.stepid into sid from stepscontainer st where st.production=v_prod and st.step=(select max(step) from stepscontainer st2 where st2.production=v_prod);
res := isVisible(sid);
if res > 0 then
return v_prod;
else
return -1;
end if;
/*EXCEPTION
   WHEN NO_DATA_FOUND THEN
    return v_prod;*/
end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
/*function getConfToBeUpdated return conf_id_name_vers_table PIPELINED is
v_configurations_table conf_id_name_vers_table := conf_id_name_vers_table();
begin
for cur in ( select * from configurations where configname='LHCb')
LOOP
pipe row(conf_id_name_vers(cur.configurationid,cur.configname,cur.configversion));
END LOOP;
--RETURN v_configurations_table;
END;*/
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure insertRuntimeProject(pr_stepid number, run_pr_stepid number)
is
begin
insert into runtimeprojects(stepid, runtimeprojectid) values (pr_stepid,run_pr_stepid);
commit;
end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure updateRuntimeProject(pr_stepid number, run_pr_stepid number)
is
counter Number;
begin
 select count(*) into counter from runtimeprojects where stepid=pr_stepid;
 if counter > 0 then
  update runtimeprojects set runtimeprojectid=run_pr_stepid where stepid=pr_stepid;
  else
    insertRuntimeProject (pr_stepid, run_pr_stepid);
  end if;
commit;
end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure removeRuntimeProject(pr_stepid number)
is
begin
delete runtimeprojects where stepid=pr_stepid;
commit;
end;
procedure funny(a number)is
b number;
begin
if a > 0 then
  b:=a-1;
  dbms_output.put_line(b || ' - ' || 'coool!');
  funny(b);
end if;
end;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getProductionPorcPassName(v_procid number) return varchar2 is
retval varchar2(256);
ecode    NUMBER(38);
thisproc CONSTANT VARCHAR2(50) := 'trap_errmesg';
begin
 select v.path into retval from (SELECT distinct  LEVEL-1 Pathlen, SYS_CONNECT_BY_PATH(name, '/') Path
   FROM processing
   WHERE LEVEL > 0 and id=v_procid
   CONNECT BY NOCYCLE PRIOR id=parentid order by Pathlen desc) v where rownum<=1;
return retval;
EXCEPTION WHEN OTHERS THEN
raise_application_error(-20004, 'error found! The processing pass does not exists!');
--ecode := SQLERRM; --SQLCODE;
--dbms_output.put_line(thisproc || ' - ' || ecode);
return null;
end;
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getDirectoryMetadata(f_name varchar2, a_Cursor out udt_RefCursor)
is
/*create or replace  type
directoryMetadata is object
(production number,
configname varchar2(256),
configversion  varchar2(256),
eventtypeid number,
filetype varchar2(256),
processingpass varchar2(256),
ConditionDescription varchar2(256));
create or replace
type bulk_collect_directoryMetadata is table of directoryMetadata;
*/
lfnmeta bulk_collect_directoryMetadata := bulk_collect_directoryMetadata();
n integer := 0;
procName varchar2(256);
simdesc varchar2(256);
daqdesc varchar2(256);
begin
for c in (select /*+ INDEX(f FILES_FILENAME_UNIQUE) */ distinct j.production, c.configname, c.configversion, ft.name, f.eventtypeid from files f, jobs j, filetypes ft, configurations c where
c.configurationid=j.configurationid and ft.filetypeid = f.filetypeid and j.jobid=f.jobid and f.gotreplica='Yes' and f.filename like f_name)
LOOP
  select getProductionPorcPassName(prod.processingid),sim.simdescription, daq.description into procName, simdesc, daqdesc from productionscontainer prod, simulationconditions sim, data_taking_conditions daq where
   production=c.production and
   prod.simid=sim.simid(+) and
   prod.daqperiodid=daq.daqperiodid(+);
   lfnmeta.extend;
   n:=n+1;
   if simdesc is NULL or simdesc='' then
     lfnmeta (n):= directoryMetadata(c.production,c.configname, c.configversion, c.eventtypeid, c.name, procname,daqdesc);
   else
     lfnmeta (n):= directoryMetadata(c.production,c.configname, c.configversion, c.eventtypeid, c.name, procname,simdesc);
   END if;
END LOOP;
open a_Cursor for select * from table(lfnmeta);
EXCEPTION
   WHEN NO_DATA_FOUND THEN
    raise_application_error(-20088, 'The file '||f_name||' does not exists in the bookkeeping database!');
end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure getDirectoryMetadata_new(lfns varchararray, a_Cursor out udt_RefCursor)
is
/*create or replace  type
directoryMetadata_new is object
(lfn varchar2(256),
production number,
configname varchar2(256),
configversion  varchar2(256),
eventtypeid number,
filetype varchar2(256),
processingpass varchar2(256),
ConditionDescription varchar2(256));
create or replace
type bulk_collect_directoryMet_new is table of directoryMetadata_new;
*/
lfnmeta bulk_collect_directoryMet_new := bulk_collect_directoryMet_new();
n integer := 0;
procName varchar2(256);
simdesc varchar2(256);
daqdesc varchar2(256);
found number := 0;
BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
  for c in (select /*+ INDEX(f FILES_FILENAME_UNIQUE) */ distinct j.production, c.configname, c.configversion, ft.name, f.eventtypeid from files f, jobs j, filetypes ft, configurations c where
   c.configurationid=j.configurationid and ft.filetypeid = f.filetypeid and j.jobid=f.jobid and f.gotreplica='Yes' and f.filename like lfns(i)) LOOP
   select count(*) into found from productionscontainer where production=c.production;
   if found>0then
     select getProductionPorcPassName(prod.processingid),sim.simdescription, daq.description into procName, simdesc, daqdesc from productionscontainer prod, simulationconditions sim, data_taking_conditions daq where
       production=c.production and
       prod.simid=sim.simid(+) and
       prod.daqperiodid=daq.daqperiodid(+);
     lfnmeta.extend;
     n:=n+1;
    if simdesc is NULL or simdesc='' then
      lfnmeta (n):= directoryMetadata_new(lfns(i),c.production,c.configname, c.configversion, c.eventtypeid, c.name, procname,daqdesc);
    else
      lfnmeta (n):= directoryMetadata_new(lfns(i),c.production,c.configname, c.configversion, c.eventtypeid, c.name, procname,simdesc);
    END if;
 END IF;
  END LOOP;
END LOOP;
open a_Cursor for select * from table(lfnmeta);
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
function getFilesForGUID(v_guid varchar2) return varchar2 is
result varchar2(256);
BEGIN
select filename into result from files where guid=v_guid;
return result;
EXCEPTION
   WHEN NO_DATA_FOUND THEN
    raise_application_error(-20088, 'The file which corresponds to GUID: '||v_guid||' does not exists in the bookkeeping database!');
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure updateDataQualityFlag(v_qualityid number, lfns varchararray )
is
BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
  update files set inserttimestamp=sys_extract_utc(systimestamp), qualityid= v_qualityid where filename=lfns(i);
END LOOP;
commit;
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure bulkcheckfiles(lfns varchararray,  a_Cursor out udt_RefCursor)
is
lfnmeta metadata_table := metadata_table();
n integer := 0;
found number := 0;
BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
  select count(filename) into found from files where filename=lfns(i);
  IF found = 0 THEN
    lfnmeta.extend;
    n:=n+1;
    lfnmeta (n):=metadata0bj(lfns(i), NULL,NULL,NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  END IF;
END LOOP;
open a_Cursor for select filename from table(lfnmeta);
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure bulkupdateReplicaRow(v_replica varchar2, lfns varchararray)
is
BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
 update files set inserttimestamp = sys_extract_utc(systimestamp),gotreplica=v_replica where filename=lfns(i);
END LOOP;
commit;
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure bulkgetTypeVesrsion(lfns varchararray, a_Cursor out udt_RefCursor)
is
lfnmeta metadata_table := metadata_table();
n integer := 0;
found number := 0;
ftype varchar2(256);

begin
FOR i in lfns.FIRST .. lfns.LAST LOOP
  select count(ftype.version) into found from files f, filetypes ftype where f.filetypeid=ftype.filetypeid and f.filename=lfns(i);
  IF found > 0 THEN
    select ftype.version into ftype from files f, filetypes ftype where f.filetypeid=ftype.filetypeid and f.filename=lfns(i);
    lfnmeta.extend;
    n:=n+1;
    lfnmeta (n):=metadata0bj(lfns(i), ftype ,NULL,NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
  END IF;
END LOOP;
open a_Cursor for select * from table(lfnmeta);
end;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure setObsolete
is
BEGIN
update steps set usable='Obsolete' where stepid in (select stepid from steps where trunc(INSERTTIMESTAMPS)<=add_months(sysdate+1,-12) and usable!='Obsolete');
commit;
END;
----------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure bulkJobInfo(lfns varchararray, a_Cursor out udt_RefCursor)
is
/*create or replace  type jobMetadata is object(lfn varchar2(256),
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
  ProgramVersion              VARCHAR2(256));
create or replace
type bulk_collect_jobMetadata is table of jobMetadata;
*/
n integer := 0;
jobmeta bulk_collect_jobMetadata := bulk_collect_jobMetadata();
BEGIN
FOR i in lfns.FIRST .. lfns.LAST LOOP
  for c in (select  jobs.DIRACJOBID, jobs.DIRACVERSION, jobs.EVENTINPUTSTAT, jobs.EXECTIME, jobs.FIRSTEVENTNUMBER,jobs.LOCATION,  jobs.NAME, jobs.NUMBEROFEVENTS,
                 jobs.STATISTICSREQUESTED, jobs.WNCPUPOWER, jobs.CPUTIME, jobs.WNCACHE, jobs.WNMEMORY, jobs.WNMODEL, jobs.WORKERNODE, jobs.WNCPUHS06, jobs.jobid, jobs.totalluminosity, jobs.production, jobs.programName, jobs.programVersion
   from jobs,files where files.jobid=jobs.jobid and  files.filename=lfns(i)) LOOP
     jobmeta.extend;
     n:=n+1;
    jobmeta (n):= jobMetadata(lfns(i), c.DIRACJOBID, c.DIRACVERSION, c.EVENTINPUTSTAT, c.EXECTIME, c.FIRSTEVENTNUMBER,c.LOCATION,  c.NAME, c.NUMBEROFEVENTS,
                 c.STATISTICSREQUESTED, c.WNCPUPOWER, c.CPUTIME, c.WNCACHE, c.WNMEMORY, c.WNMODEL, c.WORKERNODE, c.WNCPUHS06, c.jobid, c.totalluminosity, c.production, c.programName, c.programVersion);
  END LOOP;
END LOOP;
open a_Cursor for select * from table(jobmeta);
END;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure bulkJobInfoForJobName(jobNames varchararray, a_Cursor out udt_RefCursor)
is
n integer := 0;
jobmeta bulk_collect_jobMetadata := bulk_collect_jobMetadata();
BEGIN
FOR i in jobNames.FIRST .. jobNames.LAST LOOP
  for c in (select  jobs.DIRACJOBID, jobs.DIRACVERSION, jobs.EVENTINPUTSTAT, jobs.EXECTIME, jobs.FIRSTEVENTNUMBER,jobs.LOCATION,  jobs.NAME, jobs.NUMBEROFEVENTS,
                 jobs.STATISTICSREQUESTED, jobs.WNCPUPOWER, jobs.CPUTIME, jobs.WNCACHE, jobs.WNMEMORY, jobs.WNMODEL, jobs.WORKERNODE, jobs.WNCPUHS06, jobs.jobid, jobs.totalluminosity, jobs.production, jobs.programName, jobs.programVersion
   from jobs,files where files.jobid=jobs.jobid and  jobs.name=jobNames(i)) LOOP
     jobmeta.extend;
     n:=n+1;
    jobmeta (n):= jobMetadata(jobNames(i), c.DIRACJOBID, c.DIRACVERSION, c.EVENTINPUTSTAT, c.EXECTIME, c.FIRSTEVENTNUMBER,c.LOCATION,  c.NAME, c.NUMBEROFEVENTS,
                 c.STATISTICSREQUESTED, c.WNCPUPOWER, c.CPUTIME, c.WNCACHE, c.WNMEMORY, c.WNMODEL, c.WORKERNODE, c.WNCPUHS06, c.jobid, c.totalluminosity, c.production, c.programName, c.programVersion);
  END LOOP;
END LOOP;
open a_Cursor for select * from table(jobmeta);
END;

----------------------------------------------------------------------------------------------------------------------------------------------------------------------
procedure bulkJobInfoForJobId(jobids numberarray, a_Cursor out udt_RefCursor)
is
n integer := 0;
jobmeta bulk_collect_jobMetadata := bulk_collect_jobMetadata();
BEGIN
FOR i in jobids.FIRST .. jobids.LAST LOOP
  for c in (select  jobs.DIRACJOBID, jobs.DIRACVERSION, jobs.EVENTINPUTSTAT, jobs.EXECTIME, jobs.FIRSTEVENTNUMBER,jobs.LOCATION,  jobs.NAME, jobs.NUMBEROFEVENTS,
                 jobs.STATISTICSREQUESTED, jobs.WNCPUPOWER, jobs.CPUTIME, jobs.WNCACHE, jobs.WNMEMORY, jobs.WNMODEL, jobs.WORKERNODE, jobs.WNCPUHS06, jobs.jobid, jobs.totalluminosity, jobs.production, jobs.programName, jobs.programVersion
   from jobs,files where files.jobid=jobs.jobid and  jobs.diracjobid= jobids(i)) LOOP
     jobmeta.extend;
     n:=n+1;
    jobmeta (n):= jobMetadata(jobids(i), c.DIRACJOBID, c.DIRACVERSION, c.EVENTINPUTSTAT, c.EXECTIME, c.FIRSTEVENTNUMBER,c.LOCATION,  c.NAME, c.NUMBEROFEVENTS,
                 c.STATISTICSREQUESTED, c.WNCPUPOWER, c.CPUTIME, c.WNCACHE, c.WNMEMORY, c.WNMODEL, c.WORKERNODE, c.WNCPUHS06, c.jobid, c.totalluminosity, c.production, c.programName, c.programVersion);
  END LOOP;
END LOOP;
open a_Cursor for select * from table(jobmeta);
END;

END;
/
