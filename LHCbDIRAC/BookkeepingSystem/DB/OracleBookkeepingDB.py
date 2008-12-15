########################################################################
# $Id: OracleBookkeepingDB.py,v 1.45 2008/12/15 15:04:59 zmathe Exp $
########################################################################
"""

"""

__RCSID__ = "$Id: OracleBookkeepingDB.py,v 1.45 2008/12/15 15:04:59 zmathe Exp $"

from types                                                           import *
from DIRAC.BookkeepingSystem.DB.IBookkeepingDB                       import IBookkeepingDB
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                         import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder                     import getDatabaseSection
from DIRAC.Core.Utilities.OracleDB                                   import OracleDB
import datetime
global ALLOWED_ALL 
ALLOWED_ALL = 2
class OracleBookkeepingDB(IBookkeepingDB):
  
  #############################################################################
  def __init__(self):
    """
    """
    super(OracleBookkeepingDB, self).__init__()
    
    self.cs_path = getDatabaseSection('Bookkeeping/BookkeepingDB')
    
    self.dbHost = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingTNS')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: Host')
      return
    self.dbHost = result['Value']
    
    self.dbUser = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingUser')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbUser = result['Value']
    
    self.dbPass = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingPassword')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbPass = result['Value']


    self.dbServer = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingServer')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbServer = result['Value']

    self.dbW_ = OracleDB(self.dbServer, self.dbPass, self.dbHost)
    self.dbR_ = OracleDB(self.dbUser, self.dbPass, self.dbHost)
  #############################################################################
  def getAvailableConfigurations(self):
    """
    """
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getAvailableConfigurations',[])

  #############################################################################
  def getAvailableConfigNames(self):
    command = ' select distinct Configname from configurations'
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getConfigVersions(self, configname):
    command = ' select distinct configversion from configurations where configname=\''+configname+'\''
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getSimulationConditions(self, configName, configVersion, realdata):
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                bookkeepingview.configversion=\''+configVersion+'\''
    if realdata==0:
      command = 'select distinct SIMID, SIMDESCRIPTION,BEAMCOND,BEAMENERGY,GENERATOR,MAGNETICFIELD,DETECTORCOND,LUMINOSITY \
                    from simulationConditions,bookkeepingView where            \
                    bookkeepingview.DAQPeriodId=simulationConditions.simid' + condition
      res = self.dbR_._query(command)
      return res
    else:
      command = 'select data_taking_conditions.DAQPERIODID,data_taking_conditions.DESCRIPTION,data_taking_conditions.BEAMCOND, \
                data_taking_conditions.BEAMENERGY,data_taking_conditions.MAGNETICFIELD,data_taking_conditions.VELO,data_taking_conditions.IT, \
               data_taking_conditions.TT,data_taking_conditions.OT,data_taking_conditions.RICH1,data_taking_conditions.RICH2,data_taking_conditions.SPD_PRS, \
             data_taking_conditions.ECAL \
          from data_taking_conditions,bookkeepingView where \
               bookkeepingview.DAQPeriodId=data_taking_conditions.DAQPERIODID'+ condition
      res = self.dbR_._query(command)
      return res
    return S_ERROR("Data not found!")
               
  #############################################################################
  def getAvailableEventTypes(self):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getAvailableEventTypes', [])
  
  #############################################################################
  def getEventTypes(self, configName, configVersion):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getEventTypes', [configName, configVersion])
  
   #nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnneeeeeeeeeeeeeeeeeeewwwwwwwwwwwwwwwwwwwwwww
  #############################################################################
  def getProPassWithSimCond(self, configName, configVersion, simcondid):
    
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                bookkeepingview.configversion=\''+configVersion+'\''
    
    if simcondid !='ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcondid)
                
    command = 'select distinct processing_pass.TOTALPROCPASS,  \
               pass_index.step0, pass_index.step1, pass_index.step2, \
               pass_index.step3, pass_index.step4,pass_index.step5, pass_index.step6  from bookkeepingview,processing_pass,pass_index where \
               bookkeepingview.production=processing_pass.production and \
               processing_pass.passid=pass_index.passid'+condition
    res = self.dbR_._query(command)
    value = res['Value']
    retvalue = []
    description = ''
    for one in value:
      tmp = list(one)
      groups = tmp[0].split('<')
      description = ''
      for group in groups:
        result = self.getDescription(group)['Value'][0][0]
        description += result +' + '
      tmp[0]=description[:-3] 
      retvalue += [tuple(tmp)]
    return S_OK(retvalue)
  
  #############################################################################
  def getDescription(self, id):
    command = 'select groupdescription from pass_group where groupid='+str(id)
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getGroupId(self, description):
    command = 'select groupid from pass_group where groupdescription=\''+str(description)+'\''
    res = self.dbR_._query(command)
    return res
    
  #############################################################################
  def getEventTypeWithSimcond(self,configName, configVersion, simcondid, procPass):
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                    bookkeepingview.configversion=\''+configVersion+'\''
    if simcondid != 'ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcondid)
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())['Value'][0][0]
        totalproc += str(result)+"<"
      totalproc = totalproc[:-1]
      condition += ' and processing_pass.TOTALPROCPASS=\''+totalproc+'\''
    
    command ='select distinct eventTypes.EventTypeId, eventTypes.Description from eventtypes,bookkeepingview,processing_pass where \
         bookkeepingview.production=processing_pass.production and \
         eventTypes.EventTypeId=bookkeepingview.eventtypeid'+condition
  
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getProductionsWithSimcond(self, configName, configVersion, simcondid, procPass, evtId):
    all = 0
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                    bookkeepingview.configversion=\''+configVersion+'\''
    
    if simcondid != 'ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcondid)
    else:
      all += 1
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())['Value'][0][0]
        totalproc += str(result)+"<"
      totalproc = totalproc[:-1]
      condition += ' and processing_pass.TOTALPROCPASS=\''+totalproc+'\''
    else:
      all += 1
    
    if evtId != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if all > ALLOWED_ALL:
      return S_ERROR('To many ALL selected')
    
    command = 'select distinct bookkeepingview.production from bookkeepingview,processing_pass where \
         bookkeepingview.production=processing_pass.production'+condition
  
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getFileTypesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod):
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                    bookkeepingview.configversion=\''+configVersion+'\''
    
    all = 0
    if simcondid != 'ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcondid)
    else:
      all += 1
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())['Value'][0][0]
        totalproc += str(result)+"<"
      totalproc = totalproc[:-1]
      condition += ' and processing_pass.TOTALPROCPASS=\''+totalproc+'\''
    else:
      all += 1
          
    if evtId != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(evtId)
    else:
        all += 1
    
    if prod != 'ALL':
      condition += ' and bookkeepingview.production='+str(prod)
    else:
      all += 1
    
    if all > ALLOWED_ALL:
      return S_ERROR("To many all selected")
    
    command = 'select distinct filetypes.name from filetypes,bookkeepingview,processing_pass where \
               bookkeepingview.filetypeId=fileTypes.filetypeid and bookkeepingview.production=processing_pass.production'+condition
               
    res = self.dbR_._query(command)
    return res
  
  def getProgramNameWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype):
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                    bookkeepingview.configversion=\''+configVersion+'\''
    
    all = 0
    if simcondid != 'ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcondid)
    else:
      all += 1
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())['Value'][0][0]
        totalproc += str(result)+"<"
      totalproc = totalproc[:-1]
      condition += ' and processing_pass.TOTALPROCPASS=\''+totalproc+'\''
    else:
      all += 1
          
    if evtId != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if prod != 'ALL':
      condition += ' and bookkeepingview.production='+str(prod)
    else:
      all += 1
    
    if ftype!= 'ALL':
      fType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and bookkeepingview.filetypeid='+str(ftypeId)
    else:
      all += 1
    
    if all > ALLOWED_ALL:
      return S_ERROR("To many ALL selected")
    command = 'select distinct bookkeepingview.ProgramName, bookkeepingView.ProgramVersion, 0 from filetypes,bookkeepingview,processing_pass where \
               bookkeepingview.filetypeId=fileTypes.filetypeid and bookkeepingview.production=processing_pass.production'+condition
               
    res = self.dbR_._query(command)
    return res
    
  #############################################################################
  def getFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    condition = ' and configurations.ConfigName=\''+configName+'\' and \
                    configurations.ConfigVersion=\''+configVersion+'\''
    
    all = 0;
    tables = ' jobs, files,configurations'
    if simcondid != 'ALL':
      condition += ' and jobs.production=processing_pass.production'
      condition += ' and processing_pass.simcondid='+str(simcondid)
      tables += ' ,processing_pass'
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())['Value'][0][0]
        totalproc += str(result)+"<"
      totalproc = totalproc[:-1]
      condition += ' and processing_pass.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and processing_pass.PRODUCTION=jobs.production'
      if 'processing_pass' not in tables:
         tables += ', processing_pass'
    else:
      all += 1
      
    
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if prod != 'ALL':
      condition += ' and jobs.Production='+str(prod)
    else:
      all += 1
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    else:
      all += 1
    
    if progName != 'ALL' and progVersion != 'ALL':
      condition += ' and jobs.ProgramName=\''+progName+'\''
      condition += ' and jobs.ProgramVersion=\''+progVersion+'\''
    else:
      all +=1
         
    if ftype == 'ALL':
      command =' select files.FileName, files.EventStat, files.FileSize, files.CreationDate, jobs.Generator, jobs.GeometryVersion, \
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, filetypes.Name from '+ tables+' ,filetypes \
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid and \
         files.filetypeid=filetypes.filetypeid' + condition 
      all +=1
    else:
      command =' select files.FileName, files.EventStat, files.FileSize, files.CreationDate, jobs.Generator, jobs.GeometryVersion, \
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, \''+str(ftype)+'\' from '+ tables +'\
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid' + condition 
    if all > ALLOWED_ALL:
      return S_ERROR(" TO many ALL selected")
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getLimitedFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, startitem, maxitems):
    condition = ' and configurations.ConfigName=\''+configName+'\' and \
                    configurations.ConfigVersion=\''+configVersion+'\''
    
    all = 0
    tables = ' jobs, files,configurations'
    if simcondid != 'ALL':
      condition += ' and jobs.production=processing_pass.production'
      condition += ' and processing_pass.simcondid='+str(simcondid)
      tables += ' ,processing_pass'
    else:
      all += 1
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())['Value'][0][0]
        totalproc += str(result)+"<"
      totalproc = totalproc[:-1]
      condition += ' and processing_pass.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and processing_pass.PRODUCTION=jobs.production'
      if 'processing_pass' not in tables:
         tables += ', processing_pass'
    else:
      all += 1
      
    
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if prod != 'ALL':
      condition += ' and jobs.Production='+str(prod)
    else:
      all += 1
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    else:
      all += 1
    
    if progName != 'ALL' and progVersion != 'ALL':
      condition += ' and jobs.ProgramName=\''+progName+'\''
      condition += ' and jobs.ProgramVersion=\''+progVersion+'\''
    else:
      all += 1
         
    if ftype == 'ALL':
      command = 'select rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, ftype \
      FROM \
       ( select rownum rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode,ftype \
          from( \
           select fileName fname, files.EventStat eventstat, files.FileSize fsize, files.CreationDate creation, \
            jobs.Generator gen, jobs.GeometryVersion geom, \
            jobs.JobStart jstart, jobs.JobEnd jend, jobs.WorkerNode wnode, filetypes.name ftype \
        from'+tables+'filetypes \
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid and \
         files.filetypeid=filetypes.filetypeid' + condition + ' ) where rownum <= '+str(maxitems)+ ' ) where rnum > '+ str(startitem)
      all += 1
    else:
      command = 'select rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, \''+str(ftype)+'\' from \
       ( select rownum rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode \
          from( \
           select fileName fname, files.EventStat eventstat, files.FileSize fsize, files.CreationDate creation, \
            jobs.Generator gen, jobs.GeometryVersion geom, \
            jobs.JobStart jstart, jobs.JobEnd jend, jobs.WorkerNode wnode \
        from'+ tables+'\
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid' + condition + ' ) where rownum <= ' + str(maxitems)+ ' ) where rnum > '+str(startitem)
      
    if all > ALLOWED_ALL:
      return S_ERROR("To many ALL selected")
    
    res = self.dbR_._query(command)
    return res
  
  
  def getLimitedNbOfFiles(self,configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    condition = ' and configurations.ConfigName=\''+configName+'\' and \
                    configurations.ConfigVersion=\''+configVersion+'\''
    
    all = 0
    tables = ' jobs, files,configurations'
    if simcondid != 'ALL':
      condition += ' and jobs.production=processing_pass.production'
      condition += ' and processing_pass.simcondid='+str(simcondid)
      tables += ', processing_pass' 
    else:
      all += 1
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())['Value'][0][0]
        totalproc += str(result)+"<"
      totalproc = totalproc[:-1]
      condition += ' and processing_pass.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and processing_pass.PRODUCTION=jobs.production'
      if 'processing_pass' not in tables:
         tables += ', processing_pass'
    else:
      all += 1
        
      
    
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if prod != 'ALL':
      condition += ' and jobs.Production='+str(prod)
    else:
      all += 1
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    else:
      all += 1
    
    if progName != 'ALL' and progVersion != 'ALL':
      condition += ' and jobs.ProgramName=\''+progName+'\''
      condition += ' and jobs.ProgramVersion=\''+progVersion+'\''
    else:
      all += 1
    if ftype == 'ALL':
      command =' select count(*), SUM(files.EventStat), SUM(files.FILESIZE) from '+ tables +', filetypes \
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid and \
         files.filetypeid=filetypes.filetypeid' + condition 
      all += 1
    else:
      command =' select count(*), SUM(files.EventStat), SUM(files.FILESIZE) from ' + tables +' \
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid' + condition 
    
    if all > ALLOWED_ALL:
      return S_ERROR("To many ALL selected")
    
    res = self.dbR_._query(command)
    return res
    
  #############################################################################
  def getSimCondWithEventType(self, configName, configVersion, eventType, realdata = 0):
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                    bookkeepingview.configversion=\''+configVersion+'\''
    
    if eventType != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(eventType)
    
    if realdata != 0:
      command = 'select data_taking_conditions.DAQPERIODID,data_taking_conditions.DESCRIPTION,data_taking_conditions.BEAMCOND, \
                data_taking_conditions.BEAMENERGY,data_taking_conditions.MAGNETICFIELD,data_taking_conditions.VELO,data_taking_conditions.IT, \
               data_taking_conditions.TT,data_taking_conditions.OT,data_taking_conditions.RICH1,data_taking_conditions.RICH2,data_taking_conditions.SPD_PRS, \
             data_taking_conditions.ECAL \
          from data_taking_conditions,bookkeepingView where \
               bookkeepingview.DAQPeriodId=data_taking_conditions.DAQPERIODID'+ condition
    else:
      command = 'select distinct SIMID, SIMDESCRIPTION,BEAMCOND,BEAMENERGY,GENERATOR,MAGNETICFIELD,DETECTORCOND,LUMINOSITY \
               from simulationConditions,bookkeepingView where bookkeepingview.DAQPeriodId=simulationConditions.simid' + condition
    
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getProPassWithEventType(self, configName, configVersion, eventType, simcond):
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                    bookkeepingview.configversion=\''+configVersion+'\''
    
    if eventType != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(eventType)
    
    if  simcond != 'ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcond)
  
    command = 'select distinct processing_pass.TOTALPROCPASS, pass_index.step0, pass_index.step1, pass_index.step2, pass_index.step3, pass_index.step4,pass_index.step5, pass_index.step6 \
       from bookkeepingview,processing_pass,pass_index where \
            bookkeepingview.production=processing_pass.production and \
            processing_pass.passid=pass_index.passid'+ condition
    res = self.dbR_._query(command)
    value = res['Value']
    retvalue = []
    description = ''
    for one in value:
      tmp = list(one)
      groups = tmp[0].split('<')
      description = ''
      for group in groups:
        result = self.getDescription(group)['Value'][0][0]
        description += result +' + '
      tmp[0]=description[:-3] 
      retvalue += [tuple(tmp)]
    return S_OK(retvalue)
  
  #############################################################################
  def getJobInfo(self, lfn):
    command = 'select jobid from files where filename=\''+lfn+'\''
    res = self.dbR_._query(command)
    if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
    else:
      if len( res['Value'] ) == 0:
        return S_ERROR('File not found!')
      jobid = res['Value'][0][0]
      command = 'select  DIRACJOBID, DIRACVERSION, EVENTINPUTSTAT, EXECTIME, FIRSTEVENTNUMBER, GENERATOR, \
                 GEOMETRYVERSION, GRIDJOBID, LOCALJOBID, LOCATION,  NAME, NUMBEROFEVENTS, \
                 STATISTICSREQUESTED, WNCPUPOWER, WNCPUTIME, WNCACHE, WNMEMORY, WNMODEL, WORKERNODE, jobid from jobs where jobid='+str(jobid)
      res = self.dbR_._query(command)
      return res
    return S_ERROR("Job is not found!")

  
  def getProductionFiles(self, prod, ftype):
    command = ''
    value = {}
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error(res['Message'])
        return S_ERROR('Oracle error'+res['Message'])
      else:
        if len(res['Value']) == 0:
          return S_ERROR('File Type not found:'+str(ftype)) 
        
        ftypeId = res['Value'][0][0]
        command = 'select files.filename, files.gotreplica, files.filesize,files.guid from jobs,files where jobs.jobid=files.jobid and files.filetypeid='+str(ftypeId)+' and jobs.production='+str(prod)
    else:
      command = 'select files.filename, files.gotreplica, files.filesize,files.guid from jobs,files where jobs.jobid=files.jobid and jobs.production='+str(prod)
   
    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1],'FilesSize':record[2],'GUID':record[3]} 
    else:
      return S_ERROR(res['Message'])
    return S_OK(value)
  
  #############################################################################
  def updateFileMetaData(self, filename, filesAttr):
    command = 'update files Set '
    for attribute in filesAttr.keys():
      command += str(attribute)+'='+str(filesAttr[attribute])+' ,'
    
    command = command[:-1]
    command += ' where fileName=\''+filename+'\''
    res = self.dbW_._query(command)
    return res
    

  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    command = ' update files Set  fileName = \''+newLFN+'\' where filename=\''+oldLFN+'\''
    res = self.dbW_._query(command)
    return res
  
  #############################################################################
  def getInputAndOutputJobFiles(self, jobids):
    list = {}
    for jobid in jobids:
      tmp = {}
      res = self.getInputFiles(jobid)

      if not res['OK']:
        return S_ERROR(res['Message'])
      input = res['Value']
      inputs = []
      for lfn in input:
        inputs += [lfn]
        
      res = self.getOutputFiles(jobid)
      if not res['OK']:
        return S_ERROR(res['Message'])
      output = res['Value'] 
      outputs = []
      for lfn in output:
        if lfn not in inputs:
          outputs += [lfn]
      tmp = {'InputFiles':inputs,'OutputFiles':outputs}
      list[jobid]=tmp
    return S_OK(list)  
      
  #############################################################################
  def getInputFiles(self, jobid):
    command = ' select files.filename from inputfiles,files where files.fileid=inputfiles.fileid and inputfiles.jobid='+str(jobid)
    res = self.dbR_._query(command)
    return res
      
  #############################################################################
  def getOutputFiles(self, jobid):  
    command = ' select files.filename from files where files.jobid ='+str(jobid) 
    res = self.dbR_._query(command)
    return res
  #############################################################################
  def getJobsIds(self, filelist):
    list = {}
    for file in filelist:
      res = self.getJobInfo(file)
      if not res['OK']:
        return S_ERROR(res['Message'])  
      dbResult = res['Value']
      for record in dbResult:
        jobid = str(record[19])
        value = {'DiracJobID':record[0], 'DiracVersion':record[1], 'EventInputStat':record[2], 'ExecTime':record[3], 'FirstEventNumber':record[4], 'Generator':record[5], \
                 'GeometryVersion':record[6], 'GridJobID':record[7], 'LocalJobID':record[8], 'Location':record[9], 'Name':record[10], 'NumberofEvents':record[11], \
                  'StatistivsRequested':record[12], 'WNCPUPOWER':record[13], 'WNCPUTIME':record[14], 'WNCACHE':record[15], 'WNMEMORY':record[16], 'WNMODEL':record[17], 'WORKERNODE':record[18]}  
      list[jobid]=value
    return S_OK(list)
  
  
  
  #############################################################################
  def getSpecificFiles(self,configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getSpecificFiles', [configName, configVersion, programName, programVersion, fileType, eventTypeId, production])
  
  #############################################################################
  def getPass_index(self):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getPass_index', [])
  
  #############################################################################  
  def insert_pass_index(self, groupdesc, step0, step1, step2, step3, step4, step5, step6):
    return self.dbW_.executeStoredFunctions('BKK_ORACLE.insert_pass_index', LongType, [groupdesc, step0, step1, step2, step3, step4, step5, step6])
  
  #############################################################################  
  def insertProcessing(self, production, passid, inputprod, simdesc):
    totalproc = ''
    if inputprod <> '':
      descriptions = inputprod.split('+')
      for desc in descriptions:
        result = self.getGroupId(desc.strip())['Value'][0][0]
        totalproc += str(result)+"<"
      totalproc = totalproc[:-1]
    res = self.getSimulationCondIdByDesc(simdesc)
    if not res['OK']:
      gLogger.error("Simulation conditions problem", res["Message"])
      return S_ERROR("Simulation conditions problem" + str(res["Message"]))
    elif res['Value'] != 0: 
      simid = res['Value']
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.insertProcessing', [production, passid, totalproc, simid], False)
  
  #############################################################################  
  def listProcessingPass(self, prod = None):
    condition =''
    if prod != None:
      condition = ' where production='+str(prod)
    
    
    command = 'select distinct processing_pass.TOTALPROCPASS,  \
               production from processing_pass'+condition+ ' ORDER BY production'
    
    
    res = self.dbR_._query(command)
    value = res['Value']
    retvalue = []
    description = ''
    for one in value:
      tmp = list(one)
      groups = tmp[0].split('<')
      description = ''
      for group in groups:
        result = self.getDescription(group)['Value'][0][0]
        description += result +' + '
      tmp[0]=description[:-3] 
      retvalue += [tuple(tmp)]
    return S_OK(retvalue)
  #############################################################################  
  def getProductionsWithPocessingPass(self, processingPass):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getProductions', [processingPass])
  
  #############################################################################  
  def getFilesByProduction(self, production, eventtype, filetype):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFilesByProduction', [production, eventtype, filetype])
  
  #############################################################################
  def getProductions(self, configName, configVersion, eventTypeId):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getProductions', [configName, configVersion, eventTypeId])
  
  #############################################################################
  def getNumberOfEvents(self, configName, configversion, eventTypeId, production):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getNumberOfEvents', [configName, configversion, eventTypeId, production])
  
  #############################################################################
  def getEventTyesWithProduction(self, production):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getEventTyesWithProduction', [production])
  
  #############################################################################  
  def getFileTypesWithProduction(self, production, eventType):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithProduction', [production, eventType])
  
  #############################################################################  
  def getSpecificFilesWithoutProd(self, configName, configVersion, pname, pversion, filetype, eventType):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getSpecificFilesWithoutProd',[configName,configVersion, pname, pversion, filetype, eventType])
  
  #############################################################################  
  def getFileTypes(self, configName, configVersion, eventType, prod):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileTypes',[configName, configVersion, eventType, prod]) 
  
  #############################################################################  
  def getProgramNameAndVersion(self, configName, configVersion, eventType, prod, fileType):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getProgramNameAndVersion', [configName, configVersion, eventType, prod, fileType])
  
  #############################################################################  
  def getAvailableEventTypes(self):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getAvailableEventTypes', [])
  
  #############################################################################  
  def getConfigNameAndVersion(self, eventTypeId):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getConfigNameAndVersion', [eventTypeId])
  
  #############################################################################  
  def getAvailableProcessingPass(self, configName, configVersion, eventTypeId):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getAvailableProcessingPass', [configName, configVersion, eventTypeId])

  #############################################################################
  def getFileTypesWithEventType(self, configName, configVersion, eventTypeId, production):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithEventType', [configName, configVersion, eventTypeId, production])
  
  #############################################################################
  def getFileTypesWithEventTypeALL(self, configName, configVersion, eventTypeId):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithEventType', [configName, configVersion, eventTypeId])
  
  #############################################################################
  def getFilesByEventType(self, configName, configVersion, fileType, eventTypeId, production):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFilesByEventType', [configName, configVersion, fileType, eventTypeId, production])
  
  #############################################################################
  def getFilesByEventTypeALL(self, configName, configVersion, fileType, eventTypeId):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFilesByEventType', [configName, configVersion, fileType, eventTypeId])
  
  #############################################################################
  def getProductionsWithEventTypes(self, eventType, configName,  configVersion, processingPass):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getProductionsWithEventTypes', [eventType, configName,  configVersion, processingPass])
  
  #############################################################################
  def getSimulationCondID(self, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.dbR_.executeStoredFunctions('BKK_ORACLE.getSimulationCondID', LongType, [BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity])
  
  #############################################################################
  def getSimCondIDWhenFileName(self, fileName):
    return self.dbR_.executeStoredFunctions('BKK_ORACLE.getSimCondIDWhenFileName', LongType, [fileName])

  #############################################################################  
  def getLFNsByProduction(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getLFNsByProduction',[prodid])
  
  #############################################################################  
  def getAncestors(self, lfn, depth):
    logicalFileNames={}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    jobsId = []
    job_id = -1
    if depth < 1:
      depth = 1
    for fileName in lfn:
      jobsId = []
      result = self.dbR_.executeStoredFunctions('BKK_MONITORING.getJobId',LongType,[fileName])
      if not result["OK"]:
        gLogger.error('Ancestor',result['Message'])
      else:
        job_id = int(result['Value'])
      if job_id != 0:
        jobsId = [job_id]
        files = []
        while (depth-1) and jobsId:
           for job_id in jobsId:
             command = 'select files.fileName,files.jobid from inputfiles,files where inputfiles.fileid=files.fileid and inputfiles.jobid='+str(job_id)
             jobsId=[]
             res = self.dbR_._query(command)
             if not res['OK']:
               gLogger.error('Ancestor',result["Message"])
             else:
               dbResult = res['Value']
               for record in dbResult:
                 jobsId +=[record[1]]
                 files += [record[0]]
           depth-=1 
        
        ancestorList[fileName]=files    
      else:
        logicalFileNames['Failed']+=[fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)
  
    '''
    logicalFileNames=lfn
    jobsId = []
    id = -1
    for fileName in lfn:
      result = self.db_.executeStoredFunctions('BKK_MONITORING.getJobId',LongType,[fileName])
      if not result["OK"]:
        gLogger.error('Ancestor',result['Message'])
      else:
        id = int(result['Value'])
      jobsId += [id]
     
    command=''
    while (depth-1) and jobsId:
         command = 'select files.fileName,files.jobid from inputfiles,files where '
         for job_id in jobsId:
             command +='inputfiles.fileid=files.fileid and inputfiles.jobid='+str(job_id)+' or '
         command=command[:-3]
         jobsId=[]
         res = self.db_._query(command)
         if not res['OK']:
           gLogger.error('Ancestor',result["Message"])
         else:
           dbResult = res['Value']
           for record in dbResult:
             jobsId +=[record[1]]
             logicalFileNames += [record[0]]
         depth-=1     
    return logicalFileNames
   '''
  
  #############################################################################  
  def getReverseAncestors(self, lfn, depth):
    logicalFileNames={}
    jobsId = []
    job_id = -1
    deptpTmp = depth
    for fileName in lfn:
      jobsId = []
      result = self.dbR_.executeStoredFunctions('BKK_MONITORING.getJobId',LongType,[fileName])
      if not result["OK"]:
        gLogger.error('Ancestor',result['Message'])
      else:
        job_id = int(result['Value'])
      jobsId = [job_id]
      files = []
      depthTmp = depth
      while (depthTmp-1) and jobsId:
         for job_id in jobsId:
           command = 'select files.fileName,files.jobid from inputfiles,files where inputfiles.fileid=files.fileid and files.jobid='+str(job_id)
           jobsId=[]
           res = self.dbR_._query(command)
           if not res['OK']:
             gLogger.error('Ancestor',result["Message"])
           else:
             dbResult = res['Value']
             for record in dbResult:
               jobsId +=[record[1]]
               files += [record[0]]
         depth-=1 
      logicalFileNames[fileName]=files    
    return logicalFileNames
  
  """
  data insertation into the database
  """
  #############################################################################
  def checkfile(self, fileName): #file

    result = self.dbR_.executeStoredProcedure('BKK_ORACLE.checkfile',[fileName])
    if result['OK']: 
      res = result['Value']
      if len(res)!=0:
        return S_OK(res)
      else:
        gLogger.warn("File not found! ",str(fileName))
        return S_ERROR("File not found!"+str(fileName))
    else:
      return S_ERROR(result['Message'])
    return result
  
  #############################################################################
  def checkFileTypeAndVersion(self, type, version): #fileTypeAndFileTypeVersion(self, type, version):
    result = self.dbR_.executeStoredProcedure('BKK_ORACLE.checkFileTypeAndVersion',[type, version])
    if result['OK']:
      res = result['Value']
      if len(res)!=0:
        return S_OK(res)
      else:
        gLogger.error("File type not found! ",str(type))
        return S_ERROR("File type not found!"+str(type))
    else:
      return S_ERROR(result['Message'])
    
    return result

    
  
  #############################################################################
  def checkEventType(self, eventTypeId):  #eventType(self, eventTypeId):
    
    result = self.dbR_.executeStoredProcedure('BKK_ORACLE.checkEventType',[eventTypeId])
    if result['OK']:
      res = result['Value']
      if len(res)!=0:
        return S_OK(res)
      else:
        gLogger.error("Event type not found! ",str(eventTypeId))
        return S_ERROR("Event type not found!"+str(eventTypeId))
    else:
      return S_ERROR(result['Message'])
    return result
  
  #############################################################################
  def insertJob(self, job):
    
    gLogger.info("Insert job into database!")
    attrList = {'ConfigName':None, \
                 'ConfigVersion':None, \
                 'DAQPeriodId':None, \
                 'DiracJobId':None, \
                 'DiracVersion':None, \
                 'EventInputStat':None, \
                 'ExecTime':None, \
                 'FirstEventNumber':None, \
                 'Generator':None, \
                 'GeometryVersion':None, \
                 'GridJobId':None, \
                 'JobEnd':None, \
                 'JobStart':None, \
                 'LocalJobId':None, \
                 'Location':None, \
                 'LuminosityEnd':None, \
                 'LuminosityStart':None, \
                 'Name':None, \
                 'NumberOfEvents':None, \
                 'Production':None, \
                 'ProgramName':None, \
                 'ProgramVersion':None, \
                 'StatisticsRequested':None, \
                 'WNCPUPower':None, \
                 'WNCPUTime':None, \
                 'WNCache':None, \
                 'WNMemory':None, \
                 'WNModel':None, \
                 'WorkerNode':None}
    
    
    for param in job:
      if not attrList.__contains__(param):
        gLogger.error("insert job error: "," the job table not contains "+param+" this attributte!!")
        return S_ERROR(" The job table not contains "+param+" this attributte!!")
  
      if param=='JobStart' or param=='JobEnd': # We have to convert data format
        dateAndTime = job[param].split(' ')
        date = dateAndTime[0].split('-')
        time = dateAndTime[1].split(':')
        timestamp = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), 0, 0)
        attrList[param]=timestamp
      else:
        attrList[param] = job[param]
        
      
      
    result = self.dbW_.executeStoredFunctions('BKK_ORACLE.insertJobsRow',LongType,[ attrList['ConfigName'], attrList['ConfigVersion'], \
                  attrList['DAQPeriodId'], \
                  attrList['DiracJobId'], \
                  attrList['DiracVersion'], \
                  attrList['EventInputStat'], \
                  attrList['ExecTime'], \
                  attrList['FirstEventNumber'], \
                  attrList['Generator'], \
                  attrList['GeometryVersion'], \
                  attrList['GridJobId'], \
                  attrList['JobEnd'], \
                  attrList['JobStart'], \
                  attrList['LocalJobId'], \
                  attrList['Location'], \
                  attrList['LuminosityEnd'], \
                  attrList['LuminosityStart'], \
                  attrList['Name'], \
                  attrList['NumberOfEvents'], \
                  attrList['Production'], \
                  attrList['ProgramName'], \
                  attrList['ProgramVersion'], \
                  attrList['StatisticsRequested'], \
                  attrList['WNCPUPower'], \
                  attrList['WNCPUTime'], \
                  attrList['WNCache'], \
                  attrList['WNMemory'], \
                  attrList['WNModel'], \
                  attrList['WorkerNode'] ])           
    return result
  
  #############################################################################
  def insertInputFile(self, jobID, FileId):
    result = self.dbW_.executeStoredProcedure('BKK_ORACLE.insertInputFilesRow',[FileId, jobID], False)
    return result
  #############################################################################
  def insertOutputFile(self, file):
  
      attrList = {  'Adler32':None, \
                    'CreationDate':None, \
                    'EventStat':None, \
                    'EventTypeId':None, \
                    'FileName':None,  \
                    'FileTypeId':None, \
                    'GotReplica':None, \
                    'Guid':None,  \
                    'JobId':None, \
                    'MD5Sum':None, \
                    'FileSize':None }
      
      for param in file:
        if not attrList.__contains__(param):
          gLogger.error("insert file error: "," the files table not contains "+param+" this attributte!!")
          return S_ERROR(" The files table not contains "+param+" this attributte!!")
        attrList[param] = file[param]
      
      result = self.dbW_.executeStoredFunctions('BKK_ORACLE.insertFilesRow',LongType, [  attrList['Adler32'], \
                    attrList['CreationDate'], \
                    attrList['EventStat'], \
                    attrList['EventTypeId'], \
                    attrList['FileName'],  \
                    attrList['FileTypeId'], \
                    attrList['GotReplica'], \
                    attrList['Guid'],  \
                    attrList['JobId'], \
                    attrList['MD5Sum'], \
                    attrList['FileSize'] ] ) 
      return result
      
  #############################################################################
  def updateReplicaRow(self, fileID, replica): #, name, location):
    result = self.dbW_.executeStoredProcedure('BKK_ORACLE.updateReplicaRow',[int(fileID), replica],False)
    return result
  
  #############################################################################
  def deleteJob(self, jobID):
    result = self.dbW_.executeStoredProcedure('BKK_ORACLE.deleteJob',[jobID], False)
    return result
  
  #############################################################################
  def deleteInputFiles(self, jobid):
    result = self.dbW_.executeStoredProcedure('BKK_ORACLE.deleteInputFiles',[jobid], False)
    return result
  
  #############################################################################
  def deleteFile(self, fileid):
    result = self.dbW_.executeStoredProcedure('BKK_ORACLE.deletefile',[fileid], False)
    return result
  
  #############################################################################
  def deleteFiles(self, lfns):
    return S_ERROR('Not Implemented !!')
    '''
    result = {}
    for file in lfns:
      res = self.checkfile(file)
      if res['OK']:
        fileID = long(res['Value'][0][0])
        jobID =  long(res['Value'][0][1])
        res = self.deleteFile(fileID)
        if res['OK']:
          res = self.deleteInputFiles(jobID)
          res = self.deleteJob(jobID)
          if res['OK']:        
            result['Succesfull']=file
          else:
            result['Failed']=file
      else:
        result['Failed']=file
      
    return S_OK(result)    
  '''
  
  #############################################################################
  def insertQuality(self, fileID, group, flag ):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQualityParam(self, fileID, qualityID, name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def modifyReplica(self, fileID , name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.dbW_.executeStoredFunctions('BKK_ORACLE.insertSimConditions', LongType, [simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity])
  
  #############################################################################
  def getSimulationCondIdByDesc(self, desc):
    return self.dbR_.executeStoredFunctions('BKK_ORACLE.getSimulationCondIdByDesc', LongType, [desc])

  #############################################################################
  def getDataTakingCondId(self, condition):
    command = 'select DaqPeriodId from data_taking_conditions where ' 
    for param in condition:
      command +=  str(param)+'=\''+condition[param]+'\' and '
    command = command[:-4]
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getSimConditions(self):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getSimConditions',[])
  
  #############################################################################
  def insertDataTakingCond(self, conditions): 
    datataking = {  'BeamCond':None, \
                    'BeamEnergy':None, \
                    'MagneticField':None, \
                    'VELO':None, \
                    'IT':None,  \
                    'TT':None, \
                    'OT':None, \
                    'RICH1':None,  \
                    'RICH2':None, \
                    'SPD_PRS':None,\
                    'ECAL':None, \
                    'HCAL':None, \
                    'MUON':None, \
                    'L0':None, \
                    'HLT':None}
        
    for param in conditions:
      if not datataking.__contains__(param):
        gLogger.error("insert datataking error: "," the files table not contains "+param+" this attributte!!")
        return S_ERROR(" The datatakingconditions table not contains "+param+" this attributte!!")
      datataking[param] = conditions[param]
        
    res = self.dbW_.executeStoredFunctions('BKK_ORACLE.insertDataTakingCond', LongType, [datataking['BeamCond'], datataking['BeamEnergy'], \
                                                                                  datataking['MagneticField'], datataking['VELO'], \
                                                                                  datataking['IT'], datataking['TT'], datataking['OT'], \
                                                                                  datataking['RICH1'], datataking['RICH2'], \
                                                                                  datataking['SPD_PRS'], datataking['ECAL'], \
                                                                                  datataking['HCAL'], datataking['MUON'], datataking['L0'], datataking['HLT'] ])
    return res
  
  #############################################################################
  def getAvailableFileTypes(self):
    command = ' select distinct Name from filetypes'
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def removeReplica(self, fileName):
    result = self.checkfile(fileName) 
    if result['OK']:
      fileID = long(result['Value'][0][0])
      res = self.updateReplicaRow(fileID, 'No')
      if res['OK']:
        return S_OK("Replica has ben removed!!!")
      else:
        return S_ERROR(res['Message'])      
    else:
      return S_ERROR('The file '+fileName+'not exist in the BKK database!!!')
  
  #############################################################################
  def getFileMetadata(self, lfns):
    result = {}
    for file in lfns:
      res = self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileMetaData',[file])
      if not res['OK']:
        result[file]= res['Message']
      else:
        records = res['Value']  
        for record in records:
          row = {'ADLER32':record[1],'CreationDate':record[2],'EventStat':record[3],'EventTypeId':record[4],'FileType':record[5],'GotReplica':record[6],'GUID':record[7],'MD5SUM':record[8],'FileSize':record[9]}
          result[file]= row
    return S_OK(result)
  
  #############################################################################
  def getFileMetaDataForUsers(self, lfns):
    totalrecords = len(lfns)
    parametersNames = ['Name', 'FileSize','FileType','CreationDate','EventTypeId','EventStat','GotReplica']
    records = []
    for file in lfns:
      res = self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileMetaData',[file])
      if not res['OK']:
        records = [str(res['Message'])]
      else:
        values = res['Value']  
        for record in values:
          row = [file, record[9],record[5], record[2], record[4], record[3], record[6]]
          records += [row]
    return S_OK({'TotalRecords':totalrecords,'ParameterNames':parametersNames,'Records':records}) 
  
  #############################################################################
  def __getProductionStatisticsForUsers(self, prod):
    command = 'select count(*), SUM(files.EventStat), SUM(files.FILESIZE) from files ,jobs where jobs.jobid=files.jobid and jobs.production='+str(prod)
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getProductionFilesForUsers(self, prod, ftypeDict, SortDict, StartItem, Maxitems):
    command = ''
    parametersNames = ['Name', 'FileSize','FileType','CreationDate','EventTypeId','EventStat','GotReplica']
    records = []
    
    totalrecords = 0
    nbOfEvents = 0
    filesSize = 0
    ftype = ftypeDict['type']
    if len(SortDict) > 0:
      res = self.db_.__getProductionStatisticsForUsers(prod)
      if not res['OK']:
        gLogger.error(res['Message'])
      else:
        totalrecords = res['Value'][0][0]
        nbOfEvents = res['Value'][0][1]
        filesSize = res['Value'][0][2]
        
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error(res['Message'])
        return S_ERROR('Oracle error'+res['Message'])
      else:
        if len(res['Value']) == 0:
          return S_ERROR('File Type not found:'+str(ftype)) 
        
        ftypeId = res['Value'][0][0]
        
        command = 'select rnum, filename, filesize, \''+str(ftype)+'\' , creationdate, eventtypeId, eventstat,gotreplica from \
                ( select rownum rnum, filename, filesize, \''+str(ftype)+'\' , creationdate, eventtypeId, eventstat, gotreplica \
                from ( select files.filename, files.filesize, \''+str(ftype)+'\' , files.creationdate, files.eventtypeId, files.eventstat,files.gotreplica \
                           from jobs,files,filetypes where \
                           jobs.jobid=files.jobid and \
                           files.filetypeid='+str(ftypeId)+' and \
                           jobs.production='+str(prod)+' Order by files.filename) where rownum <='+str(Maxitems)+ ') where rnum >'+str(StartItem) 
    else:
      
      command = 'select rnum, filename, filesize, name, creationdate, eventtypeId, eventstat,gotreplica from \
                ( select rownum rnum, filename, filesize, name, creationdate, eventtypeId, eventstat, gotreplica \
                from ( select files.filename, files.filesize, filetypes.name, files.creationdate, files.eventtypeId, files.eventstat,files.gotreplica \
                           from jobs,files,filetypes where \
                           jobs.jobid=files.jobid and \
                           files.filetypeid=filetypes.filetypeid and \
                           jobs.production='+str(prod)+' Order by files.filename) where rownum <='+str(Maxitems)+ ') where rnum >'+str(StartItem) 
   
    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        row = [record[1],record[2],record[3],record[4],record[5],record[6],record[7]]
        records += [row]
        totalrecords += 1
    else:
      return S_ERROR(res['Message'])
    return S_OK({'TotalRecords':totalrecords,'ParameterNames':parametersNames,'Records':records,'Extras': {'GlobalStatistics':{'Number of Events':nbOfEvents, 'Files Size':filesSize }}}) 
  
  #############################################################################
  def exists(self, lfns):
    result ={}
    for file in lfns:
      res = self.dbR_.executeStoredFunctions('BKK_ORACLE.fileExists', LongType, [file])
      if not res['OK']:
        return S_ERROR(res['Message'])
      if res['Value'] ==0:
        result[file] = False
      else:
        result[file] = True
    return S_OK(result)
   
  #############################################################################
  def addReplica(self, fileName):
    result = self.checkfile(fileName) 
    if result['OK']:
      fileID = long(result['Value'][0][0])
      res = self.updateReplicaRow(fileID, 'Yes')
      if res['OK']:
        return S_OK("Replica has ben added!!!")
      else:
        return S_ERROR(res['Message'])      
    else:
      return S_ERROR('The file '+fileName+'not exist in the BKK database!!!')

  #############################################################################
  def getPassIndexID(self, programName, programVersion):
    condition = programName+'-'+programVersion
    command = 'select passid from pass_index where step0=\''+condition+'\''
    res = self.dbR_._query(command)
    returnValue = None
    if not res['OK']:
      returnValue = S_ERROR('Message')
    else:
      if len(res['Value']) == 0:
        retVal = self.insert_pass_index('UNKNOWN', condition, None, None, None, None, None, None)
        if not retVal['OK']:
          returnValue = S_ERROR(retVal['Message'])
        else:
          returnValue = S_OK(retVal['Value'])
      else:
        returnValue = S_OK(res['Value'][0][0])
      
    return returnValue
  
  #############################################################################
  def insertProcessing_pass(self, passid, simcond):
    return self.dbW_.executeStoredFunctions('BKK_ORACLE.insertProcessing_PASS', LongType, [passid, simcond])
  
  #############################################################################
  def getProcessingPassGroups(self):
     return self.dbR_.executeStoredProcedure('BKK_ORACLE.getProcessingPassGroups',[])
  
  #############################################################################
  def insert_pass_group(self, gropupdesc):
    return self.dbW_.executeStoredFunctions('BKK_ORACLE.insert_pass_group', LongType, [gropupdesc])
  
  #############################################################################
  def insertEventTypes(self, evid, desc, primary):
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.insertEventTypes',[desc, evid, primary], False)
  
  #############################################################################
  def updateEventType(self, evid, desc, primary):
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.updateEventTypes',[desc, evid, primary], False)
  
  #############################################################################
  def checkProcessingPassAndSimCond(self, production):
    command = ' select count(*) from processing_pass where production='+ str(production)
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  #
  #          MONITORING
  #############################################################################
  def getJobsNb(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getJobsNb', [prodid])
  
  #############################################################################
  def getNumberOfEvents(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getNumberOfEvents', [prodid])
  
  #############################################################################
  def getSizeOfFiles(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getSizeOfFiles', [prodid])
  
  #############################################################################
  def getNbOfFiles(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getNbOfFiles', [prodid])
  
  #############################################################################
  def getProductionInformation(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getProductionInformation', [prodid])
  
  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getJobsbySites', [prodid])
    
  