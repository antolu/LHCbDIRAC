########################################################################
# $Id: OracleBookkeepingDB.py,v 1.57 2009/02/09 16:46:50 zmathe Exp $
########################################################################
"""

"""

__RCSID__ = "$Id: OracleBookkeepingDB.py,v 1.57 2009/02/09 16:46:50 zmathe Exp $"

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
             data_taking_conditions.ECAL, data_taking_conditions.HCAL, data_taking_conditions.MUON, data_taking_conditions.L0, data_taking_conditions.HLT\
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
                
    command = 'select distinct productions.TOTALPROCPASS,  \
               pass_index.step0,pass_index.step1,pass_index.step2,pass_index.step3,pass_index.step4,pass_index.step5,pass_index.step6 from bookkeepingview,productions,pass_index,applications where \
               bookkeepingview.production=productions.production and \
               productions.passid=pass_index.passid'+condition
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
      for appid in range(1,7):
        appname = ''
        if tmp[appid]!= None:
          retVal = self._getApplication(tmp[appid])
          if retVal['OK']:
            value = retVal['Value']
            appname = value[0][1]+' '+value[0][2]
            gLogger.info('Application name:',appname)
            infos = '/'+str(value[0][3])+'/'+str(value[0][4])+'/'+str(value[0][5])
            tmp[appid]=appname+infos
          else:
            return S_ERROR(retVal['Message'])
      
      retvalue += [tuple(tmp)]
    return S_OK(retvalue)
  
  #############################################################################
  def _getApplication(self, applid):
    command = 'select * from applications where applicationid='+str(applid)
    res = self.dbR_._query(command)
    return res
  
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
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
    
    command ='select distinct eventTypes.EventTypeId, eventTypes.Description from eventtypes,bookkeepingview,productions where \
         bookkeepingview.production=productions.production and \
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
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
    else:
      all += 1
    
    if evtId != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if all > ALLOWED_ALL:
      return S_ERROR('To many ALL selected')
    
    command = 'select distinct bookkeepingview.production from bookkeepingview,productions where \
         bookkeepingview.production=productions.production'+condition
  
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
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
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
    
    command = 'select distinct filetypes.name from filetypes,bookkeepingview,productions where \
               bookkeepingview.filetypeId=fileTypes.filetypeid and bookkeepingview.production=productions.production'+condition
               
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
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
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
    command = 'select distinct bookkeepingview.ProgramName, bookkeepingView.ProgramVersion, 0 from filetypes,bookkeepingview,productions where \
               bookkeepingview.filetypeId=fileTypes.filetypeid and bookkeepingview.production=productions.production'+condition
               
    res = self.dbR_._query(command)
    return res
    
  #############################################################################
  def getFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    condition = ' and configurations.ConfigName=\''+configName+'\' and \
                    configurations.ConfigVersion=\''+configVersion+'\''
    
    all = 0;
    tables = ' jobs, files,configurations'
    if simcondid != 'ALL':
      condition += ' and jobs.production=productions.production'
      condition += ' and productions.simcondid='+str(simcondid)
      tables += ' ,productions'
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
      if 'productions' not in tables:
         tables += ', productions'
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
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, filetypes.Name, jobs.runnumber, jobs.fillnumber, files.physicStat, dataquality.dataqualityflag from '+ tables+' ,filetypes, dataquality \
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid and \
         files.gotReplica=\'Yes\' and \
         files.filetypeid=filetypes.filetypeid and \
         files.qualityid= dataquality.qualityid' + condition 
      all +=1
    else:
      command =' select files.FileName, files.EventStat, files.FileSize, files.CreationDate, jobs.Generator, jobs.GeometryVersion, \
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, \''+str(ftype)+'\' , jobs.runnumber, jobs.fillnumber, files.physicStat, dataquality.dataqualityflag from '+ tables +' ,dataquality\
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         jobs.configurationid=configurations.configurationid and \
         files.qualityid= dataquality.qualityid' + condition 
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
      condition += ' and jobs.production=productions.production'
      condition += ' and productions.simcondid='+str(simcondid)
      tables += ' ,productions'
    else:
      all += 1
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
      if 'productions' not in tables:
         tables += ', productions'
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
      command = 'select rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, ftype, runnb,fillnb,physt,quality \
      FROM \
       ( select rownum rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode,ftype,runnb,fillnb,physt,quality \
          from( \
           select fileName fname, files.EventStat eventstat, files.FileSize fsize, files.CreationDate creation, \
            jobs.Generator gen, jobs.GeometryVersion geom, \
            jobs.JobStart jstart, jobs.JobEnd jend, jobs.WorkerNode wnode, filetypes.name ftype, \
        jobs.runnumber runnb, jobs.fillnumber fillnb, files.physicStat physt, dataquality.dataqualityflag quality\
        from'+tables+',filetypes, dataquality \
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid and \
         files.gotReplica=\'Yes\' and \
         files.qualityid=dataquality.qualityid \
         files.filetypeid=filetypes.filetypeid' + condition + ' ) where rownum <= '+str(maxitems)+ ' ) where rnum > '+ str(startitem)
      all += 1
    else:
      command = 'select rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, \''+str(ftype)+'\' , runnb,fillnb,physt,quality from \
       ( select rownum rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, runnb,fillnb,physt,quality \
          from( \
           select fileName fname, files.EventStat eventstat, files.FileSize fsize, files.CreationDate creation, \
            jobs.Generator gen, jobs.GeometryVersion geom, \
            jobs.JobStart jstart, jobs.JobEnd jend, jobs.WorkerNode wnode, jobs.runnumber runnb, jobs.fillnumber fillnb, files.physicStat physt, dataquality.dataqualityflag quality \
        from'+ tables+', dataquality\
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         files.qualityid=dataquality.qualityid and \
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
      condition += ' and jobs.production=productions.production'
      condition += ' and productions.simcondid='+str(simcondid)
      tables += ', productions' 
    else:
      all += 1
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
      if 'productions' not in tables:
         tables += ', productions'
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
         files.gotReplica=\'Yes\' and \
         files.filetypeid=filetypes.filetypeid' + condition 
      all += 1
    else:
      command =' select count(*), SUM(files.EventStat), SUM(files.FILESIZE) from ' + tables +' \
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
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
  def getFilesWithGivenDataSets(self, simdesc, procPass, ftype, evt, configName='ALL', configVersion='ALL', production='ALL'):
    
    configid = None
    condition = ''
    
    if configName != 'ALL' and configVersion != 'ALL':
      command = ' select configurationid from configurations where configurations.ConfigName=\''+configName+'\' and \
                    configurations.ConfigVersion=\''+configVersion+'\''
      res = self.dbR_._query(command)
      if not res['OK']:
        return S_ERROR(res['Message'])
      else:
        configid = res['Value'][0][0]
        if configid != 0:
          condition = ' and jobs.configurationid='+str(configid)
        else:
          return S_ERROR('Wrong configuration name and version!')
                    
    if production != 'ALL':
      condition += ' and jobs.production='+str(production)
    
    descriptions = procPass.split('+')
    totalproc = ''
    for desc in descriptions:
      result = self.getGroupId(desc.strip())
      if not result['OK']:
        return S_ERROR(result['Message'])
      elif len(result['Value']) == 0:
        return S_ERROR('Data taking or Simulation Conditions is missing in the BKK database!')
      val = result['Value'][0][0]
      totalproc += str(val)+"<"
    totalproc = totalproc[:-1]
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    
    condition +=  ' and files.eventtypeid='+str(evt)
  
    cond = simdesc.split('*')
    if cond[0] == 'S':
      command = ' select filename from files,jobs where files.jobid= jobs.jobid and files.gotreplica=\'Yes\''+condition+' \
                 and jobs.production in ( select production from  productions, simulationconditions where  \
                 simulationconditions.simdescription=\''+cond[1]+'\' and \
                 productions.simcondid= simulationconditions.simid and \
                 productions.totalprocpass=\''+totalproc+'\')'
    else:
      command = ' select filename from files,jobs where files.jobid= jobs.jobid and files.gotreplica=\'Yes\''+condition+' \
                 and jobs.production in ( select production from  productions, data_Taking_conditions where  \
                 data_Taking_conditions.description=\''+cond[1]+'\' and \
                 productions.simcondid= data_Taking_conditions.Daqperiodid and \
                 productions.totalprocpass=\''+totalproc+'\')'
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
  
    command = 'select distinct productions.TOTALPROCPASS, pass_index.step0, pass_index.step1, pass_index.step2, pass_index.step3, pass_index.step4,pass_index.step5, pass_index.step6 \
       from bookkeepingview,productions,pass_index where \
            bookkeepingview.production=productions.production and \
            productions.passid=pass_index.passid'+ condition
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

  #############################################################################
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
  def insert_aplications(self, appName, appVersion, option, dddb, condb):
    
    retVal = self.check_applications(appName, appVersion, option, dddb, condb)
    if retVal['OK']:
      id = retVal['Value']
      if id == 0:
        command = ' SELECT applications_index_seq.nextval FROM dual'
        retVal = self.dbR_._query(command)
        if retVal['OK']:
          appid = retVal['Value'][0][0]
          values = str(appid) + ','
          values += '\''+str(appName)+'\','
          values += '\''+str(appVersion)+'\','
          values += '\''+str(option)+'\','
          values += '\''+str(dddb)+'\','
          values += '\''+str(condb)+'\''
          command = ' insert into applications (ApplicationID, ApplicationName, ApplicationVersion, OptionFiles, DDDb, condDb) values ('+values+')'
          retVal = self.dbW_._query(command)
          if not retVal['OK']:
            return S_ERROR(retVal['Message'])
        else:
          return S_ERROR(retVal['Message'])
        return S_OK(appid)
      else:
        return S_OK(id)
    else:
      return S_ERROR(retVal['Message'])
  
  #############################################################################
  def check_applications(self,appName, appVersion, option, dddb, condb):
    command = ''
    condition = ''
    if dddb != '':
      condition += ' and dddb=\''+str(dddb)+'\' '
    
    if option != '':
      condition += ' and optionfiles=\''+str(option)+'\' '
    
    if condb != '':
      condition += ' and conddb=\''+str(condb)+'\' '
    
    command = 'select applicationid from applications where applicationname=\''+str(appName)+'\' and applicationversion=\''+str(appVersion)+'\''+condition
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      id = retVal['Value']
      if len(id) == 0:
        return S_OK(0)
      else:
        return S_OK(id[0][0])
    else:
      return S_ERROR(retVal['Message'])
  
  #############################################################################
  def insert_pass_index_migration(self, passid, descr, groupid, step0,step1, step2,step3,step4,step5,step6):
    values = str(passid)+','
    values += '\''+descr+'\','
    values += str(groupid) +','
    values += str(step0) +','
    values += str(step1) +','
    values += str(step2) +','
    values += str(step3) +','
    values += str(step4) +','
    values += str(step5) +','
    values += str(step6)
    command = 'insert into pass_index (PASSID, DESCRIPTION, GROUPID, STEP0, STEP1, STEP2, STEP3, STEP4, STEP5, STEP6) values('+values+')'
    res = self.dbW_._query(command)
    print res
    return res


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
  def checkAddProduction(self, steps, groupdesc, simcond, inputProdTotalProcessingPass, production):
    keys = steps.keys()
    keys.sort()
    s = [None, None, None, None, None, None, None]
    i = 0
    result = ''
    for step in keys:
      appName = steps[step]['ApplicationName']
      appVersion = steps[step]['ApplicationVersion'] 
      optfiles = steps[step]['OptionFiles']
      if optfiles == None:
        optfiles = ''
        result += 'Option files are missing! \n'
      dddb = steps[step]['DDDb']
      if dddb == None: 
        dddb = ''
        result += 'DDDb tag is missing! \n'
      condb = steps[step]['CondDb']
      if condb == None:
        condb = ''
        result += 'ConDDb tag is missing! \n'
      
      res = self.check_applications(appName, appVersion, optfiles, dddb, condb)
      if not res['OK']:
        return S_ERROR(res['Message'])
      else:
        value = res['Value']
        if value == 0:
          result += 'Application Name:'+str(appName)+' Application Version:'+str(appVersion)+' Optionfiles:'+str(optfiles)+ 'DDDb:'+str(dddb) +'CondDb:'+str(condb)+' are missing in the BKKDB! \n'
        else:
          result += 'Application Name:'+str(appName)+' Application Version:'+str(appVersion)+' Optionfiles:'+str(optfiles)+ 'DDDb:'+str(dddb) +'CondDb:'+str(condb)+' are in the BKKDB! \n'
          s[i] = res['Value']
          i += 1
    

    command = ' select groupId from PASS_GROUP where GROUPDESCRIPTION=\''+str(groupdesc)+'\''
    retVal = self.dbR_._query(command)
    groupid = None
    
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      if len(retVal['Value']) == 0:
        result += 'Group description is not exist in the BKKDB! \n'
      else: 
        groupid = retVal['Value'][0][0]
      
    
      retVal = self.check_pass_index(groupid, s[0], s[1], s[2], s[3], s[4], s[5],s[6])
      if not retVal['OK']:
        return S_ERROR(retVal['Message'])
      elif retVal['Value']==None:
        result += 'Pass index not exists! \n'
        result += '     pass index:'+str(steps)+'\n'
      else:
        result += 'Pass index exists!\n'
        result += '     pass index:'+str(steps)+'\n'
    
    res = self.getSimulationCondIdByDesc(simcond['SimDescription'])
    if not res['OK']:
      result += res['Message'] +'\n'
    elif res['Value'] == 0: 
      result += ' Simulation conditions not exists!\n'
    else:
      result += ' Simulation conditions exists!\n'
    
    return S_OK(result)

    
  
  #############################################################################  
  def insert_procressing_pass(self, steps, groupdesc, simcond, inputProdTotalProcessingPass, production):
    keys = steps.keys()
    keys.sort()
    s = [None, None, None, None, None, None, None]
    i = 0
    for step in keys:
      appName = steps[step]['ApplicationName']
      appVersion = steps[step]['ApplicationVersion'] 
      optfiles = steps[step]['OptionFiles']
      if optfiles == None:
        optfiles = ''
      dddb = steps[step]['DDDb']
      if dddb == None: 
        dddb = ''
      condb = steps[step]['CondDb']
      if condb == None:
        condb = ''
      res = self.insert_aplications(appName, appVersion, optfiles, dddb, condb)
      if not res['OK']:
        return S_ERROR(res['Message'])
      else:
        s[i] = res['Value']
        i += 1
    res = self.insert_pass_index_new(groupdesc, s[0], s[1], s[2], s[3], s[4], s[5],s[6])
    passid = None
    if not res['OK']:
      print res['Message']
    else:
      passid = res['Value']
    
    simdesc = ''
    retVal = self.__getSimDesc(simcond)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      simdesc = retVal['Value']
    
    gLogger.info('Insert productions:')
    gLogger.info('Passid: '+str(passid))
    gLogger.info('Simulation description: '+str(simdesc))
    gLogger.info('Production: '+str(production))
    gLogger.info('Input Production total processing pass: '+str(inputProdTotalProcessingPass))
    
    retVal = self.insertProcessing(production, passid, inputProdTotalProcessingPass, simdesc)
    if retVal['OK']:
      return S_OK('The processing pass added successfully!')
    else:
      return S_ERROR('Error discovered!'+ str(retVal['Message']))
  
  #############################################################################  
  def insert_procressing_passRealData(self, steps, groupdesc, daqdesc, inputProdTotalProcessingPass, production):
    keys = steps.keys()
    keys.sort()
    s = [None, None, None, None, None, None, None]
    i = 0
    for step in keys:
      appName = steps[step]['ApplicationName']
      appVersion = steps[step]['ApplicationVersion'] 
      optfiles = steps[step]['OptionFiles']
      if optfiles == None:
        optfiles = ''
      dddb = steps[step]['DDDb']
      if dddb == None: 
        dddb = ''
      condb = steps[step]['CondDb']
      if condb == None:
        condb = ''
      res = self.insert_aplications(appName, appVersion, optfiles, dddb, condb)
      if not res['OK']:
        return S_ERROR(res['Message'])
      else:
        s[i] = res['Value']
        i += 1
    res = self.insert_pass_index_new(groupdesc, s[0], s[1], s[2], s[3], s[4], s[5],s[6])
    passid = None
    if not res['OK']:
      print res['Message']
    else:
      passid = res['Value']
    
    gLogger.info('Insert productions:')
    gLogger.info('Passid: '+str(passid))
    gLogger.info('Data taking description: '+str(daqdesc))
    gLogger.info('Production: '+str(production))
    gLogger.info('Input Production total processing pass: '+str(inputProdTotalProcessingPass))
    
    retVal = self.insertProcessingRealData(production, passid, inputProdTotalProcessingPass, daqdesc)
    if retVal['OK']:
      return S_OK('The processing pass added successfully!')
    else:
      return S_ERROR('Error discovered!'+ str(retVal['Message']))
  
  #############################################################################  
  def __getSimDesc(self, simcond):
    res = self.getSimulationCondIdByDesc(simcond['SimDescription'])
    if not res['OK']:
      gLogger.error("Simulation conditions problem", res["Message"])
      return S_ERROR("Simulation conditions problem" + str(res["Message"]))
    elif res['Value'] == 0: 
      retVal = self.insertSimConditions(simcond['SimDescription'], simcond['BeamCond'], simcond['BeamEnergy'], simcond['Generator'], simcond['MagneticField'], simcond['DetectorCond'], simcond['Luminosity'])
      if not retVal['OK']:
         return S_ERROR(retVal['Message'])
    simdesc = simcond['SimDescription'] 
    return S_OK(simdesc)
       
    
  
  #############################################################################  
  def __equalsPrograms(self, existing, new):
    ok = True
    if self.__countSteps(existing) != len(new):
      ok = False
    else:
      i = 3
      keys = new.keys()
      keys.sort()
      for key in keys:
        newProgramName = new[key].split('-')[0]
        newProgramVersion = new[key].split('-')[1]
        tags = self.__getTags(existing[i])
        name = self.__getProgramName(existing[i])
        i += 1
        ok = (newProgramName == name and newProgramVersion == tags[0])
        if not ok:
          break;
    return ok
  
  #############################################################################  
  def __countSteps(self, value):
    stepNb = 0
    for i in range(3, len(value)):
      if value[i] != None:
        stepNb += 1
      i += 1
    return stepNb
  #############################################################################  
  
  def __getTags(self, value):
    tmp = value.split('&')
    tags = []
    for i in range(len(tmp)):
      tags  += tmp[i].split('-')[1].split('/')
    return tags
  
  #############################################################################  
  def __getProgramName(self, value):
    return value.split('-')[0]
  
  #############################################################################  
  def insertProcessing(self, production, passid, inputprod, simdesc):
    totalproc = ''
    simid = None
    if inputprod <> '':
      descriptions = inputprod.split('+')
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
    else:
      inputprod = None
    res = self.getSimulationCondIdByDesc(simdesc)
    print res['Value']
    if not res['OK']:
      gLogger.error("Simulation conditions problem", res["Message"])
      return S_ERROR("Simulation conditions problem" + str(res["Message"]))
    elif res['Value'] != 0: 
      simid = res['Value']
    else:
      return S_ERROR('Simulation condition is not found in BKK!')
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.insertProcessing', [production, passid, totalproc, simid], False)
  
  #############################################################################  
  def insertProcessingRealData(self, production, passid, inputprod, datataking):
    totalproc = ''
    daqid = None
    if inputprod <> '':
      descriptions = inputprod.split('+')
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions is missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
    else:
      inputprod = None
    daq = {'Description':datataking}
    res = self.getDataTakingCondId(daq)
    if not res['OK']:
      gLogger.error("Data takin conditions problem", res["Message"])
      return S_ERROR("Data takin conditions problem" + str(res["Message"]))
    elif len(res['Value']) != 0: 
      daqid = res['Value'][0][0]
    else:
      return S_ERROR('Data takin conditions is not found in BKK!')
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.insertProcessing', [production, passid, totalproc, daqid], False)
  
  
  #############################################################################  
  def listProcessingPass(self, prod = None):
    condition =''
    if prod != None:
      condition = ' where production='+str(prod)
    
    
    command = 'select distinct productions.TOTALPROCPASS,  \
               production from productions'+condition+ ' ORDER BY production'
    
    
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
  def setQuality(self, lfns, flag):
    command = ' select qualityid from dataquality where dataqualityflag=\''+str(flag)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    elif len(retVal['Value']) == 0:
      return S_ERROR('Data quality flag is missing in the DB')
    qid = retVal['Value'][0][0]
  
    for lfn in lfns:
      retVal = self.__updateQualityFlag(lfn, qid)
      if not retVal['OK']:
        return S_ERROR(retVal['Message'])
          
    return S_OK('Quality flag updated!')
  
  #############################################################################  
  def setQualityRun(self, runNb, dataTaking, flag):
    command = ' select qualityid from dataquality where dataqualityflag=\''+str(flag)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    elif len(retVal['Value']) == 0:
      return S_ERROR('Data quality flag is missing in the DB')
    qid = retVal['Value'][0][0]
    
    command = ' update files set qualityId='+str(qid)+' where fileid in ( select files.fileid from jobs, files, data_taking_conditions,productions where jobs.jobid=files.jobid and \
      jobs.runnumber='+str(runNb)+' and \
      jobs.production=productions.production and \
      data_taking_conditions.daqperiodid=productions.simcondid and \
      data_taking_conditions.description=\''+str(dataTaking)+'\')'
    retVal = self.dbW_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    
    return S_OK('Quality flag has been updated!')
    
  #############################################################################  
  def __updateQualityFlag(self, lfn, qid):
    command = 'update files set qualityId='+str(qid)+' where filename=\''+str(lfn)+'\''
    retVal = self.dbW_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      return S_OK('Quality flag has been updated!')
    
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
    odepth = depth 
    gLogger.debug('original',lfn)
    for fileName in lfn:
      depth = odepth
      jobsId = []
      gLogger.debug('filename',fileName)
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
             command = 'select files.fileName,files.jobid, files.gotreplica from inputfiles,files where inputfiles.fileid=files.fileid and inputfiles.jobid='+str(job_id)
             jobsId=[]
             res = self.dbR_._query(command)
             if not res['OK']:
               gLogger.error('Ancestor',result["Message"])
             else:
               dbResult = res['Value']
               for record in dbResult:
                 jobsId +=[record[1]]
                 if record[2] != 'No':
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
                 'WorkerNode':None, \
                 'RunNumber':None,
                 'FillNumber':None}
    
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
                  attrList['WorkerNode'], \
                  attrList['RunNumber'], \
                  attrList['FillNumber'] ])           
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
                    'FileSize':None, \
                    'PhysicStat':None }
      
      for param in file:
        if not attrList.__contains__(param):
          gLogger.error("insert file error: "," the files table not contains "+param+" this attributte!!")
          return S_ERROR(" The files table not contains "+param+" this attributte!!")
        
        if param=='CreationDate': # We have to convert data format
          dateAndTime = file[param].split(' ')
          date = dateAndTime[0].split('-')
          time = dateAndTime[1].split(':')
          timestamp = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), 0, 0)
          attrList[param]=timestamp
        else:
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
                    attrList['FileSize'],
                    attrList['PhysicStat'] ] ) 
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
      res = self.__getProductionStatisticsForUsers(prod)
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
                           from jobs,files where \
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
    returnValue = None
    res = self.insert_aplications(programName, programVersion, '', '', '')
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      step0 = res['Value']
      retVal = self.insert_pass_index_new('Real Data', step0, None, None, None, None, None, None)
      if not retVal['OK']:
        returnValue = S_ERROR(retVal['Message'])
      else:
        returnValue = S_OK(retVal['Value'])    
      return returnValue 
  
  #############################################################################
  def insert_pass_index_new(self, groupdesc, step0, step1, step2, step3, step4, step5,step6):
    command = ' select groupId from PASS_GROUP where GROUPDESCRIPTION=\''+str(groupdesc)+'\''
    retVal = self.dbR_._query(command)
    groupid = None
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      if len(retVal['Value']) == 0:
        command = ' SELECT GROUPID_SEQ.nextval from dual'
        retVal = self.dbR_._query(command)
        if not retVal['OK']:
          return S_ERROR(retVal['Message'])
        else:
          groupid = retVal['Value'][0][0]
          command = ' insert /* +APPEND */ into PASS_GROUP (GROUPID, GROUPDESCRIPTION) values ('+str(groupid)+',\''+str(groupdesc)+'\')'
          retVal = self.dbW_._query(command)
          if not retVal['OK']:
            return S_ERROR(retVal['Message'])
      else:  
        groupid = retVal['Value'][0][0]
      
      retVal = self.check_pass_index(groupid, step0, step1, step2, step3, step4, step5,step6)
      if not retVal['OK']:
        return S_ERROR(retVal['Message'])
      elif retVal['Value']==None:
        command = 'select PASS_INDEX_SEQ.nextval from dual'
        retVal = self.dbR_._query(command)
        if not retVal['OK']:
          return S_ERROR(retVal['Message'])
        else:
          passid = retVal['Value'][0][0]
          descr = 'Pass'+str(passid)
          values = str(passid)+','
          values += '\''+descr+'\','
          values += str(groupid) +','
          if step0 != None:
            values += str(step0) +','
          else:
            values += 'NULL,'
          
          if step1 != None:
            values += str(step1) +','
          else:
            values += 'NULL,'
          
          if step2 != None:
            values += str(step2) +','
          else:
            values += 'NULL,'
            
          if step3 != None:
            values += str(step3) +','
          else:
            values += 'NULL,'
            
          if step4 != None:
            values += str(step4) +','
          else:
            values += 'NULL,'
            
          if step5 != None:
            values += str(step5) +','
          else:
            values += 'NULL,'
            
          if step6 != None:
            values += str(step6)
          else:
            values += 'NULL'
            
          command = 'insert /* +APPEND */ into pass_index (PASSID, DESCRIPTION, GROUPID, STEP0, STEP1, STEP2, STEP3, STEP4, STEP5, STEP6) values('+values+')'
          retVal = self.dbW_._query(command)
          if not retVal['OK']:
            return S_ERROR(retVal['Message'])
          else:
            return S_OK(passid)
      else:
          return S_OK(retVal['Value'])
          
    return S_ERROR()  
  #############################################################################
  def check_pass_index(self, groupid, step0, step1, step2, step3, step4, step5,step6):
    conditions = ''
    if step0 != None:
      conditions += ' and step0='+str(step0)
    if step1 != None:
      conditions += ' and step1='+str(step1)
    if step2 != None:
      conditions += ' and step2='+str(step2)
    if step3 != None:
      conditions += ' and step3='+str(step3)
    if step4 != None:
      conditions += ' and step4='+str(step4)
    if step5 != None:
      conditions += ' and step5='+str(step5)
    if step6 != None:
      conditions += ' and step6='+str(step6)
    command = ' select passid from pass_index where groupid='+str(groupid)+conditions
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      if len(retVal['Value']) == 0:
        return S_OK(None)
      else:
        return S_OK(retVal['Value'][0][0])
    return S_ERROR()
  
  #############################################################################
  def getLogfile(self, lfn):
    command = 'select files.jobid from files where files.filename=\''+str(lfn)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    if len(retVal['Value']) == 0:
      return S_ERROR('Job not in the DB')
    jobid = retVal['Value'][0][0]
    command = 'select filename from files where (files.filetypeid=17 or files.filetypeid=9) and files.jobid='+str(jobid)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      if len(retVal['Value']) == 0:
        return S_ERROR('Log file is not exist!')
      return S_OK(retVal['Value'][0][0])
    return S_ERROR('getLogfile error!')
  
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
    command = ' select count(*) from productions where production='+ str(production)
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
  def getSteps(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getSteps', [prodid])
  
  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getJobsbySites', [prodid])
  
  #############################################################################
  def getConfigsAndEvtType(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getConfigsAndEvtType',[prodid])
  
  