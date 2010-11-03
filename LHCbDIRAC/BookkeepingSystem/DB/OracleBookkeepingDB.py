########################################################################
# $Id$
########################################################################
"""

"""

__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.DB.IBookkeepingDB                   import IBookkeepingDB
from types                                                           import *
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                         import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder                     import getDatabaseSection
from DIRAC.Core.Utilities.OracleDB                                   import OracleDB
import datetime
import types
global ALLOWED_ALL 
ALLOWED_ALL = 2

global default 
default = 'ALL'

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
  def getAvailableSteps(self, dict = {}):      
    condition = ''
    selection = 'stepid,stepname, applicationname,applicationversion,optionfiles,DDDB,CONDDB, extrapackages,Visible'
    if len(dict) > 0:
      tables = 'steps'
      if dict.has_key('StartDate'):
        condition += ' steps.inserttimestamps >= TO_TIMESTAMP (\''+dict['StartDate']+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      if dict.has_key('StepId'): 
        if len(condition) > 0:
          condition += ' and '
        condition += ' stepid='+str(dict['StepId'])
      if dict.has_key('InputFileTypes'):
        return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getAvailebleSteps',[],True,dict['InputFileTypes'])
      command = 'select '+selection+' from '+tables+' where '+condition+'order by inserttimestamps desc'
      return self.dbR_._query(command)
    else:
      command = 'select '+selection+' from steps order by inserttimestamps desc'
      return self.dbR_._query(command)
  
  #############################################################################
  def getStepInputFiles(self, StepId):
    command = 'select inputFiletypes.name,inputFiletypes.visible from steps, table(steps.InputFileTypes) inputFiletypes where  steps.stepid='+str(StepId)
    return self.dbR_._query(command)
  
  #############################################################################
  def setStepInputFiles(self, stepid, files):
    values = 'filetypesARRAY('
    for i in files:
      if i.has_key('Name') and i.has_key('Visible'):
        values += 'ftype(\''+i['Name']+'\',\''+i['Visible']+'\'),'
    values = values[:-1]
    values +=')'
    command = 'update steps set inputfiletypes='+values+' where stepid='+str(stepid)
    return self.dbW_._query(command)
      
  #############################################################################
  def setStepOutputFiles(self, stepid, files):
    values = 'filetypesARRAY('
    for i in files:
      if i.has_key('Name') and i.has_key('Visible'):
        values += 'ftype(\''+i['Name']+'\',\''+i['Visible']+'\'),'
    values = values[:-1]
    values +=')'
    command = 'update steps set Outputfiletypes='+values+' where stepid='+str(stepid)
    return self.dbW_._query(command)
  
  #############################################################################
  def getStepOutputFiles(self, StepId):
    command = 'select outputfiletypes.name,outputfiletypes.visible from steps, table(steps.outputfiletypes) outputfiletypes where  steps.stepid='+str(StepId)
    return self.dbR_._query(command)
        
  #############################################################################
  def getAvailableFileTypes(self):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getAvailableFileTypes',[])
  
  #############################################################################
  def insertFileTypes(self, ftype, desc):
    return self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.insertFileTypes', LongType, [ftype,desc,'ROOT_All'])
  
  #############################################################################
  def insertStep(self, dict):
    values = ''
    selection = 'insert into steps(stepname,applicationname,applicationversion,OptionFiles,dddb,conddb,extrapackages,visible '
    if dict.has_key('InputFileTypes'):
      values = ',filetypesARRAY('
      selection += ',InputFileTypes'
      for i in dict['InputFileTypes']:
        if i.has_key('Name') and i.has_key('Visible'):
          values += 'ftype(\''+i['Name']+'\',\''+i['Visible']+'\'),'
      values = values[:-1]
      values +=')'
    if dict.has_key('OutputFileTypes'):
      values +=' , filetypesARRAY('
      selection += ',OutputFileTypes'
      for i in dict['OutputFileTypes']:
        if i.has_key('Name') and i.has_key('Visible'):
          values += 'ftype(\''+i['Name']+'\',\''+i['Visible']+'\'),'
      values = values[:-1]
      values +=')'
    
    if dict.has_key('Step'):
      step = dict['Step']
      command =  selection+')values(\''+str(step['StepName'])+ \
      '\',\''+str(step['ApplicationName'])+'\',\''+str(step['ApplicationVersion'])+'\',\''+str(step['OptionFiles'])+'\',\''+str(step['DDDB'])+ \
      '\',\'' +str(step['CondDB'])+'\',\''+str(step['ExtraPackages'])+'\',\''+str(step['Visible'])+'\''+values+')'
      return self.dbW_._query(command)

    return S_ERROR('The Filetypes and Step are missing!')
  
  #############################################################################
  def deleteStep(self, stepid):
    command = 'delete steps where stepid='+str(stepid)
    return self.dbW_._query(command)
  
  #############################################################################
  def updateStep(self, dict):
    if dict.has_key('StepId'):
      stepid = dict.pop('StepId')
      condition = ' where stepid='+str(stepid)
      command = 'update steps set '
      for i in dict:
        if type(dict[i]) == types.StringType:
          command += i+ '=\''+str(dict[i])+'\','
        else:
          values = 'filetypesARRAY('
          for j in dict[i]:
            if j.has_key('Name') and j.has_key('Visible'):
              values += 'ftype(\''+j['Name']+'\',\''+j['Visible']+'\'),'
          values = values[:-1]
          values +=')'
          command += i+ '='+values+','
      command = command[:-1]
      command += condition
      return self.dbW_._query(command)
      
    else:
      return S_ERROR('Please give a StepId!')
    return S_ERROR()  
  
  #############################################################################
  def getAvailableConfigNames(self):
    command = ' select distinct Configname from newbookkeepingview'
    return self.dbR_._query(command)
  
  #############################################################################
  def getConfigVersions(self, configname):
    command = ' select distinct configversion from newbookkeepingview where configname=\''+configname+'\''
    return self.dbR_._query(command)
    
  #############################################################################
  def getConditions(self, configName, configVersion):
    command = 'select distinct simulationConditions.SIMID,data_taking_conditions.DAQPERIODID,simulationConditions.SIMDESCRIPTION, simulationConditions.BEAMCOND, \
    simulationConditions.BEAMENERGY, simulationConditions.GENERATOR,simulationConditions.MAGNETICFIELD,simulationConditions.DETECTORCOND, simulationConditions.LUMINOSITY, \
    data_taking_conditions.DESCRIPTION,data_taking_conditions.BEAMCOND, data_taking_conditions.BEAMENERGY,data_taking_conditions.MAGNETICFIELD, \
    data_taking_conditions.VELO,data_taking_conditions.IT, data_taking_conditions.TT,data_taking_conditions.OT,data_taking_conditions.RICH1,data_taking_conditions.RICH2, \
    data_taking_conditions.SPD_PRS, data_taking_conditions.ECAL, data_taking_conditions.HCAL, data_taking_conditions.MUON, data_taking_conditions.L0, data_taking_conditions.HLT,\
     data_taking_conditions.VeloPosition from simulationConditions,data_taking_conditions,newbookkeepingView where \
      newbookkeepingview.simid=simulationConditions.simid(+) and \
      newbookkeepingview.DAQPERIODID=data_taking_conditions.DAQPERIODID(+) \
      and newbookkeepingview.configname=\''+configName+'\' and \
      newbookkeepingview.configversion=\''+configVersion+'\''
    return self.dbR_._query(command)
  
  #############################################################################
  def getProcessingPass(self, configName, configVersion, conddescription, path='/'):
    
    erecords = []
    eparameters =  []
    precords = []
    pparameters =  []
    
    condition = ' and newbookkeepingview.configname=\''+str(configName)+'\' and \
      newbookkeepingview.configversion=\''+str(configVersion)+'\''
      
    retVal = self._getConditionString(conddescription)
    if retVal['OK']:
      condition = retVal['Value']
    else:
      return retVal
    
    proc = path.split('/')[len(path.split('/'))-1]
    if proc != '':
      command = 'select distinct eventTypes.EventTypeId, eventTypes.Description from eventtypes,newbookkeepingview,productionscontainer,processing where \
        newbookkeepingview.production=productionscontainer.production and \
        eventTypes.EventTypeId=newbookkeepingview.eventtypeid and \
        productionscontainer.processingid=processing.id and \
        processing.name=\''+str(proc)+'\''+condition
      retVal = self.dbR_._query(command)
      if retVal['OK']:
        eparameters = ['EventTypeId','Description']
        for record in retVal['Value']:
          erecords += [[record[0],record[1]]]
      else:
        return retVal
      command = 'SELECT distinct id, name \
      FROM processing   where parentid in (select id from  processing where name=\''+str(proc)+'\') START WITH id in (select distinct productionscontainer.processingid from productionscontainer,newbookkeepingview where \
      productionscontainer.production=newbookkeepingview.production ' + condition +')  CONNECT BY NOCYCLE PRIOR  parentid=id order by name desc'
    else:
      command = 'SELECT distinct id, name \
      FROM processing   where parentid is null START WITH id in (select distinct productionscontainer.processingid from productionscontainer,newbookkeepingview where \
      productionscontainer.production=newbookkeepingview.production' + condition +') CONNECT BY NOCYCLE PRIOR  parentid=id order by name desc'
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      precords = []
      pparameters = ['Id','Name']
      for record in retVal['Value']:
        precords += [[record[0],record[1]]]
    else:
      return retVal
    
    return S_OK([{'ParameterNames':pparameters,'Records':precords,'TotalRecords':len(precords)},{'ParameterNames':eparameters,'Records':erecords,'TotalRecords':len(erecords)}])
     
  
  ############################################################################# 
  def _getConditionString(self, conddescription, table ='productionscontainer'):  
    condition = ''
    retVal = self._getDataTakingConditionId(conddescription)
    if retVal['OK']:
      if retVal['Value'] != -1:
        condition += ' and '+table+'.DAQPERIODID ='+str(retVal['Value'])
      else:
        retVal = self._getSimulationConditioId(conddescription)
        if retVal['OK']:
          if retVal['Value'] != -1:
           condition += ' and '+table+'.simid ='+str(retVal['Value'])
          else:
            return S_ERROR('Condition does not exists!') 
        else:
          return retVal
    else:
      return retVal
    return S_OK(condition)
    
  #############################################################################
  def _getDataTakingConditionId(self, desc):
    command = 'select DAQPERIODID from data_taking_conditions where DESCRIPTION=\''+str(desc)+'\''
    retVal = self.dbR_._query(command)           
    if retVal['OK']:
      if len(retVal['Value']) >0:
        return S_OK(retVal['Value'][0][0])
      else:
        return S_OK(-1)
    else:
      return retVal
  
  #############################################################################
  def _getSimulationConditioId(self, desc):
    command = 'select simid from simulationconditions where simdescription=\''+str(desc)+'\''
    retVal = self.dbR_._query(command)           
    if retVal['OK']:
      if len(retVal['Value']) >0:
        return S_OK(retVal['Value'][0][0])
      else:
        return S_OK(-1)
    else:
      return retVal
  
  #############################################################################
  def getProductions(self, configName, configVersion, conddescription=default, processing=default, evt=default):
    condition = ' and bview.configname=\''+configName+'\' and \
                  bview.configversion=\''+configVersion+'\''
    
    if conddescription != default:
      retVal = self._getConditionString(conddescription, 'pcont')
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal
    
    if evt != default:
      condition += ' and bview.eventtypeid=90000000'
    
    if processing != default:
      command = "select distinct pcont.production from \
                 productionscontainer pcont,newbookkeepingview bview \
                 where pcont.processingid in \
                    (select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='"+str(processing.split('/')[1])+"') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='"+processing+"') \
                  and bview.production=pcont.production "+condition
    else:
      command = "select distinct pcont.production from productionscontainer pcont,newbookkeepingview bview where \
                 bview.production=pcont.production "+condition
    return self.dbR_._query(command)
  
  #############################################################################
  def getFileTypes(self, configName, configVersion, conddescription=default, processing=default, evt=default, production=default):
    condition = ' and bview.configname=\''+configName+'\' and \
                  bview.configversion=\''+configVersion+'\' '
    
    if conddescription != default:
      retVal = self._getConditionString(conddescription, 'pcont')
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal
    
    if evt != default:
      condition += ' and bview.eventtypeid='+str(evt)
    
    if production != default:
      condition += ' and bview.production='+str(production)
    if processing != default:
      command = "select distinct ftypes.name from \
                 productionscontainer pcont,newbookkeepingview bview, filetypes ftypes  \
                 where pcont.processingid in \
                    (select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='"+str(processing.split('/')[1])+"') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='"+processing+"') \
                  and bview.production=pcont.production and bview.filetypeId=ftypes.filetypeid"+condition
    else:
      command = "select distinct ftypes.name  from productionscontainer pcont, newbookkeepingview bview,  filetypes ftypes where \
                 bview.production=pcont.production and bview.filetypeId=ftypes.filetypeid"+condition
    return self.dbR_._query(command)
  
  #############################################################################
  def getFiles(self, configName, configVersion, conddescription=default, processing=default, evt=default, production=default, filetype=default, quality = default):
    condition = ' and c.ConfigName=\''+configName+'\' and \
                  c.configversion=\''+configVersion+'\' '
    
    if conddescription != default:
      retVal = self._getConditionString(conddescription, 'prod')
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal
    
    if evt != default:
      condition += ' and f.eventtypeid='+str(evt)
    
    if production != default:
      condition += ' and j.production='+str(production)
    
    if filetype != default:
      condition += "  and ftypes.name='"+str(filetype)+"'"
    
    if quality != 'ALL':
      if type(quality) == types.StringType:
        command = "select QualityId from dataquality where dataqualityflag='"+str(i)+"'"
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error('Data quality problem:',res['Message'])
        elif len(res['Value']) == 0:
            return S_ERROR('Dataquality is missing!')
        else:
          quality = res['Value'][0][0]
        condition += ' and f.qualityid='+str(quality)
      else:
        conds = ' ('
        for i in quality:
          quality = None
          command = "select QualityId from dataquality where dataqualityflag='"+str(i)+"'"
          res = self.dbR_._query(command)
          if not res['OK']:
            gLogger.error('Data quality problem:',res['Message'])
          elif len(res['Value']) == 0:
              return S_ERROR('Dataquality is missing!')
          else:
            quality = res['Value'][0][0]
          conds += ' f.qualityid='+str(quality)+' or'
        condition += 'and'+conds[:-3] + ')'
      
    if processing != default:
      condition += " and prod.processingid=(\
      select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
      FROM processing v   START WITH id in (select distinct id from processing where name='"+str(processing.split('/')[1])+"') \
      CONNECT BY NOCYCLE PRIOR  id=parentid) v\
      where v.path='"+processing+"')"
    
    command = "select distinct f.FileName, f.EventStat, f.FileSize, f.CreationDate, j.JobStart, j.JobEnd, j.WorkerNode, ftypes.Name, j.runnumber, j.fillnumber, f.fullstat, d.dataqualityflag, \
    j.eventinputstat, j.totalluminosity, f.luminosity, f.instLuminosity from files f, jobs j, productionscontainer prod, configurations c, dataquality d, filetypes ftypes  where \
    j.jobid=f.jobid and \
    ftypes.filetypeid=f.filetypeid and \
    d.qualityid=f.qualityid and \
    f.gotreplica='Yes' and \
    j.configurationid=c.configurationid and \
    j.production=prod.production"+condition
    return self.dbR_._query(command)
  
  #############################################################################  
  def getAvailableDataQuality(self):
    command = ' select dataqualityflag from dataquality'
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    flags = retVal['Value']
    result = []
    for i in flags:
      result += [i[0]]
    return S_OK(result)
  
  #############################################################################
  def getAvailableProductions(self):
    command = ' select distinct production from newbookkeepingview where production > 0'
    res = self.dbR_._query(command)
    return res
  
  def getAvailableRuns(self):
    command = ' select distinct production from newbookkeepingview where production < 0'
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getAvailableEventTypes(self):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getAvailableEventTypes', [])
  
  
  #############################################################################
  def getMoreProductionInformations(self, prodid):
    command = 'select bookkeepingview.configname, bookkeepingview.configversion, bookkeepingview.ProgramName, bookkeepingview.programversion from bookkeepingview where bookkeepingview.production='+str(prodid)
    res = self.dbR_._query(command)
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      record = res['Value']
      cname = record[0][0]
      cversion = record[0][1]
      pname = record[0][2]
      pversion = record[0][3]
    
    command = 'select totalprocpass, simcondid from productions where production='+str(prodid)
    res = self.dbR_._query(command)
    simid = 0
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      record = res['Value']
      p = record[0][0]
      simid = record[0][1]
      description = ''
      groups = p.split('<')
      procdescription = ''
      for group in groups:
        result = self.getDescription(group)['Value'][0][0]
        procdescription += result +' + '
      procdescription = procdescription[:-3]
    simdesc = ''
    daqdesc = ''
    command = 'select simdescription from simulationconditions where simid='+str(simid)
    res = self.dbR_._query(command)
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      value = res['Value']
      if len(value) != 0:
        simdesc = value[0][0]
      else:
        command = 'select description from data_taking_conditions where daqperiodid='+str(simid)
        res = self.dbR_._query(command)
        if not res['OK']:
          return S_ERROR(res['Message'])
        else:
          value = res['Value']
          if len(value) != 0:
            daqdesc = value[0][0]
          else:
            return S_ERROR('Simulation condition or data taking condition not exist!')
    if simdesc != '':
      return S_OK({'ConfigName':cname,'ConfigVersion':cversion,'ProgramName':pname,'ProgramVersion':pversion,'Processing pass':procdescription,'Simulation conditions':simdesc})
    else:
      return S_OK({'ConfigName':cname,'ConfigVersion':cversion,'ProgramName':pname,'ProgramVersion':pversion,'Processing pass':procdescription,'Data taking conditions':daqdesc})
    
    
  #############################################################################
  def getJobInfo(self, lfn):
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.getJobInfo', [lfn])
  
  #############################################################################
  def getRunNumber(self, lfn):
    command = 'select jobs.runnumber from jobs,files where files.jobid=jobs.jobid and files.filename=\''+str(lfn)+'\''
    res = self.dbR_._query(command)
    return res
         
  #############################################################################
  def getProductionFiles(self, prod, ftype, gotreplica='ALL'):
    command = ''
    value = {}
    condition = ''
    if gotreplica != 'ALL':
      condition += 'and files.gotreplica=\''+str(gotreplica)+'\''
      
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
        command = 'select files.filename, files.gotreplica, files.filesize,files.guid, \''+ftype+'\', files.inserttimestamp from jobs,files where jobs.jobid=files.jobid and files.filetypeid='+str(ftypeId)+' and jobs.production='+str(prod)+condition
    else:
      command = 'select files.filename, files.gotreplica, files.filesize,files.guid, filetypes.name, files.inserttimestamp from jobs,files,filetypes where jobs.jobid=files.jobid and files.filetypeid=filetypes.filetypeid and jobs.production='+str(prod)+condition
   
    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1],'FileSize':record[2],'GUID':record[3], 'FileType':record[4]} 
    else:
      return S_ERROR(res['Message'])
    return S_OK(value)
  
  #############################################################################
  def getFilesForAGivenProduction(self, dict):
    condition = ''
    if dict.has_key('Production'):
      prod = dict['Production']
      condition = ' and jobs.production='+str(prod)
    else:
      return S_ERROR('You need to give a production number!')
    if dict.has_key('FileType'):
      ftype = dict['FileType']
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error(res['Message'])
        return S_ERROR('Oracle error'+res['Message'])
      else:
        if len(res['Value']) == 0:
          return S_ERROR('File Type not found:'+str(ftype)) 
        
        ftypeId = res['Value'][0][0]
        condition += ' and files.filetypeid='+str(ftypeId)
   
    if dict.has_key('Replica'):
      gotreplica = dict['Replica']
      condition += 'and files.gotreplica=\''+str(gotreplica)+'\''
    
    command = ''
    tables = ''
    if dict.has_key('DataQuality'):
      tables = ', dataquality'
      quality = dict['DataQuality']
      if type(quality) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in quality:
          cond += 'dataquality.dataqualityflag=\''+str(i)+'\' and files.qualityId=dataquality.qualityid or '
        cond = cond[:-3] + ')'
        condition += cond
      else:
       condition += ' and dataquality.dataqualityflag=\''+str(quality)+'\' and files.qualityId=dataquality.qualityid '
       
    value = {}
    command = 'select files.filename, files.gotreplica, files.filesize,files.guid, filetypes.name from jobs,files,filetypes'+tables+' where jobs.jobid=files.jobid and files.filetypeid=filetypes.filetypeid ' +condition
   
    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1],'FileSize':record[2],'GUID':record[3], 'FileType':record[4]} 
    else:
      return S_ERROR(res['Message'])
    return S_OK(value)
  
  #############################################################################
  def getAvailableRunNumbers(self):
    command = 'select distinct runnumber from bookkeepingview'
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getRunFiles(self, runid):
    value = {}
    command = 'select files.filename, files.gotreplica, files.filesize,files.guid from jobs,files where jobs.jobid=files.jobid and files.filetypeid=23 and jobs.runnumber='+str(runid)
    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1],'FileSize':record[2],'GUID':record[3]} 
    else:
      return S_ERROR(res['Message'])
    return S_OK(value)
  
  #############################################################################
  def updateFileMetaData(self, filename, filesAttr):
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = 'update files Set inserttimestamp=TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\') ,'
    for attribute in filesAttr.keys():
      if type(filesAttr[attribute]) == types.StringType:
        command += str(attribute)+'=\''+str(filesAttr[attribute])+'\' ,'
      else:
        command += str(attribute)+'='+str(filesAttr[attribute])+' ,'
    
    command = command[:-1]
    command += ' where fileName=\''+filename+'\''
    res = self.dbW_._query(command)
    return res
    

  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = ' update files Set inserttimestamp=TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\'), fileName = \''+newLFN+'\' where filename=\''+oldLFN+'\''
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
  def getJobsIds(self, filelist):
    list = {}
    for file in filelist:
      res = self.getJobInfo(file)
      if not res['OK']:
        return S_ERROR(res['Message'])  
      dbResult = res['Value']
      for record in dbResult:
        jobid = str(record[19])
        value = {'DiracJobID':record[0], 'DiracVersion':record[1], 'EventInputStat':record[2], 'ExecTime':record[3], 'FirstEventNumber':record[4], \
                  'Location':record[5], 'Name':record[6], 'NumberofEvents':record[7], \
                  'StatistivsRequested':record[8], 'WNCPUPOWER':record[9], 'CPUTIME':record[10], 'WNCACHE':record[11], 'WNMEMORY':record[12], 'WNMODEL':record[13], 'WORKERNODE':record[14],'TotalLuminosity':record[20]}  
      list[jobid]=value
    return S_OK(list)
  
  #############################################################################
  def insertTag(self, name, tag):
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.insertTag', [name, tag], False)
  
  #############################################################################
  def existsTag(self, name, value): #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    command = 'select count(*) from tags where name=\''+str(name)+'\' and tag=\''+str(value)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    elif retVal['Value'][0][0] > 0:
      return S_OK(True)
    return S_OK(False)
  
  #############################################################################  
  def setQuality(self, lfns, flag):
    result = {}
    command = ' select qualityid from dataquality where dataqualityflag=\''+str(flag)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    elif len(retVal['Value']) == 0:
      return S_ERROR('Data quality flag is missing in the DB')
    qid = retVal['Value'][0][0]
    
    failed = []
    succ = []
    for lfn in lfns:
      retVal = self.__updateQualityFlag(lfn, qid)
      if not retVal['OK']:
        failed += [lfn] 
        gLogger.error(retVal['Message'])
      else:
        succ += [lfn]
    result['Successful'] = succ
    result['Failed'] = failed
    return S_OK(result)
  
  #############################################################################
  def setRunQualityWithProcessing(self, runNB, procpass, flag):
    totalproc = ''
    descriptions = procpass.split('+')
    for desc in descriptions:
      result = self.getGroupId(desc.strip())
      if not result['OK']:
        return S_ERROR(result['Message'])
      elif len(result['Value']) == 0:
        return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
      val = result['Value'][0][0]
      totalproc += str(val)+"<"
    totalproc = totalproc[:-1]
    command = 'insert into runquality(runnumber,procpass,qualityid) values('+str(runNB)+',\''+totalproc+'\',\''+flag+'\')'
    return self.dbW_._query(command)
  
  #############################################################################  
  def setQualityRun(self, runNb, flag):
    command = 'select distinct jobs.fillnumber, configurations.configname, configurations.configversion, data_taking_conditions.description, pass_group.groupdescription from \
          jobs, configurations,data_taking_conditions,productions, pass_group, pass_index \
         where jobs.configurationid=configurations.configurationid and data_taking_conditions.daqperiodid=productions.simcondid and \
         productions.passid=pass_index.passid and pass_index.groupid=pass_group.groupid and \
         jobs.production=productions.production and jobs.runnumber='+str(runNb)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    value = retVal['Value']
    if len(value) == 0:
      return S_ERROR('This '+str(runNb)+' run is missing in the BKK DB!')
    command = ' select qualityid from dataquality where dataqualityflag=\''+str(flag)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    elif len(retVal['Value']) == 0:
      return S_ERROR('Data quality flag is missing in the DB')
    qid = retVal['Value'][0][0]
    
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = ' update files set inserttimestamp=TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\'), qualityId='+str(qid)+' where fileid in ( select files.fileid from jobs, files where jobs.jobid=files.jobid and \
      jobs.runnumber='+str(runNb)+')'
    retVal = self.dbW_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    
    command = 'select files.filename from jobs, files where jobs.jobid=files.jobid and \
      jobs.runnumber='+str(runNb)
    
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    
    succ = []
    records = retVal['Value']
    for record in records:
      succ += [record[0]]
    result = {}
    result['Successful'] = succ
    result['Failed'] = []
    return S_OK(result)
  
  def setQualityProduction(self, prod, flag):
    command = 'select distinct jobs.production  from jobs where jobs.production='+str(prod)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    value = retVal['Value']
    if len(value) == 0:
      return S_ERROR('This '+str(prod)+' production is missing in the BKK DB!')
    command = ' select qualityid from dataquality where dataqualityflag=\''+str(flag)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    elif len(retVal['Value']) == 0:
      return S_ERROR('Data quality flag is missing in the DB')
    qid = retVal['Value'][0][0]
    
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = ' update files set inserttimestamp=TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\'), qualityId='+str(qid)+' where fileid in ( select files.fileid from jobs, files where jobs.jobid=files.jobid and \
      jobs.production='+str(prod)+')'
    retVal = self.dbW_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    
    command = 'select files.filename from jobs, files where jobs.jobid=files.jobid and \
      jobs.production='+str(prod)
    
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    
    succ = []
    records = retVal['Value']
    for record in records:
      succ += [record[0]]
    result = {}
    result['Successful'] = succ
    result['Failed'] = []
    return S_OK(result)
  
  #############################################################################  
  def __updateQualityFlag(self, lfn, qid):
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = 'update files set inserttimestamp=TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\'), qualityId='+str(qid)+' where filename=\''+str(lfn)+'\''
    retVal = self.dbW_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      return S_OK('Quality flag has been updated!')
  
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
      return S_OK({'Failed:':lfn})
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
  
  #############################################################################  
  def getAllAncestors(self, lfn, depth):
    logicalFileNames={}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    jobsId = []
    job_id = -1
    if depth > 10:
      depth = 10
    elif depth < 1:
      depth = 1
    odepth = depth 
    gLogger.debug('original',lfn)
    for fileName in lfn:
      depth = odepth
      jobsId = []
      gLogger.debug('filename',fileName)
      jobsId = []
      result = self.dbR_.executeStoredFunctions('BKK_MONITORING.getJobIdWithoutReplicaCheck',LongType,[fileName])
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
                 files += [record[0]]
           depth-=1 
        
        ancestorList[fileName]=files    
      else:
        logicalFileNames['Failed']+=[fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)
  
  #############################################################################  
  def getAllAncestorsWithFileMetaData(self, lfn, depth):
    logicalFileNames={}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    jobsId = []
    job_id = -1
    if depth > 10:
      depth = 10
    elif depth < 1:
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
            command = 'select files.fileName,files.jobid, files.gotreplica, files.eventstat, files.eventtypeid, files.luminosity, files.instLuminosity from inputfiles,files where inputfiles.fileid=files.fileid and inputfiles.jobid='+str(job_id)
            jobsId=[]
            res = self.dbR_._query(command)
            if not res['OK']:
              gLogger.error('Ancestor',result["Message"])
            else:
              dbResult = res['Value']
              for record in dbResult:
                jobsId +=[record[1]]
                files += [{'FileName':record[0],'GotReplica':record[2],'EventStat':record[3],'EventType':record[4],'Luminosity':record[5],'InstLuminosity':record[6]}]
          depth-=1 
        
        ancestorList[fileName]=files    
      else:
        logicalFileNames['Failed']+=[fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)
  
  #############################################################################  
  def getAllDescendents(self, lfn, depth = 0, production=0, checkreplica=False):
    logicalFileNames = {}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    logicalFileNames['NotProcessed'] = []
    file_id = -1
    fileids = []
    odepth = -1
    if depth > 10:
      depth = 10
    elif depth < 1:
      depth = 1
      odepth = depth
    else:
      odepth = depth + 1
    
    tables = ''
    condition = ''
    gLogger.debug('original',lfn)
    for fileName in lfn:
      depth = odepth
      gLogger.debug('filename',fileName)
      fileids = []       
      res= self.dbW_.executeStoredFunctions('BKK_ORACLE.getFileID',LongType,[fileName])
      if not res["OK"]:
        gLogger.error('Ancestor',res['Message'])
      elif res['Value'] == None:
        logicalFileNames['Failed']+=[fileName]
      else:
        file_id = res['Value']
      if file_id != 0:
        fileids += [file_id]
        files = []
        while (depth-1) and fileids:
          for file_id in fileids:
            res= self.dbW_.executeStoredProcedure('BKK_ORACLE.getJobIdFromInputFiles',[file_id])
            fileids.remove(file_id)
            if not res["OK"]:
              gLogger.error('Ancestor',res['Message'])
              if not fileName in logicalFileNames['Failed']:
                logicalFileNames['Failed']+=[fileName]
            elif  len(res['Value']) != 0:
              job_ids = res['Value']
              for i in job_ids:
                job_id = i[0]
                command = 'select files.fileName,files.fileid,files.gotreplica, jobs.production from files, jobs where jobs.jobid=files.jobid and files.jobid='+str(job_id)
                res = self.dbW_._query(command)
                if not res["OK"]:
                  gLogger.error('Ancestor',res['Message'])
                  if not fileName in logicalFileNames['Failed']:
                    logicalFileNames['Failed']+=[fileName]
                elif len(res['Value']) == 0:
                  logicalFileNames['NotProcessed']+=[fileName]
                else:
                  dbResult = res['Value']
                  for record in dbResult:
                    fileids +=[record[1]]
                    if checkreplica and (record[2] == 'No'):
                      continue
                    if production and (int(record[3]) != int(production)):
                      continue
                    files += [record[0]]
          depth-=1

        ancestorList[fileName]=files
      else:
        logicalFileNames['Failed']+=[fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)
 
  #############################################################################  
  def getDescendents(self, lfn, depth = 0):
    logicalFileNames = {}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    logicalFileNames['NotProcessed'] = []
    file_id = -1
    fileids = []
    odepth = -1
    if depth > 10:
      depth = 10
    elif depth < 1:
      depth = 1
      odepth = depth
    else:
      odepth = depth + 1
    
    gLogger.debug('original',lfn)
    for fileName in lfn:
      depth = odepth
      gLogger.debug('filename',fileName)
      fileids = []
      res= self.dbW_.executeStoredFunctions('BKK_ORACLE.getFileID',LongType,[fileName])
      if not res["OK"]:
        gLogger.error('Ancestor',res['Message'])
      elif res['Value'] == None:
        logicalFileNames['Failed']+=[fileName]
      else:
        file_id = res['Value']
      if file_id != 0:
        fileids += [file_id]
        files = []
        while (depth-1) and fileids:
          for file_id in fileids:
            res= self.dbW_.executeStoredProcedure('BKK_ORACLE.getJobIdFromInputFiles',[file_id])
            fileids.remove(file_id)
            if not res["OK"]:
              gLogger.error('Ancestor',res['Message'])
            elif len(res['Value']) != 0:
              job_ids = res['Value']              
              for i in job_ids:
                job_id = i[0]
                res = self.dbW_.executeStoredProcedure('BKK_ORACLE.getFNameFiDRepWithJID',[job_id])
                if not res["OK"]:
                  gLogger.error('Ancestor',res['Message'])
                elif len(res['Value']) == 0:
                  logicalFileNames['NotProcessed']+=[fileName]
                else:
                  dbResult = res['Value']
                  for record in dbResult:
                    if record[2] != 'No':
                      files += [record[0]]
                      fileids +=[record[1]]
          depth-=1 
        
        ancestorList[fileName]=files    
      else:
        logicalFileNames['Failed']+=[fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)
    
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
                 'JobEnd':None, \
                 'JobStart':None, \
                 'Location':None, \
                 'Name':None, \
                 'NumberOfEvents':None, \
                 'Production':None, \
                 'ProgramName':None, \
                 'ProgramVersion':None, \
                 'StatisticsRequested':None, \
                 'WNCPUPOWER':None, \
                 'CPUTIME':None, \
                 'WNCACHE':None, \
                 'WNMEMORY':None, \
                 'WNMODEL':None, \
                 'WorkerNode':None, \
                 'RunNumber':None, \
                 'FillNumber':None, \
                 'WNCPUHS06': 0, \
                 'TotalLuminosity':0}
    
    for param in job:
      if not attrList.__contains__(param):
        gLogger.error("insert job error: "," the job table not contains "+param+" this attributte!!")
        return S_ERROR(" The job table not contains "+param+" this attributte!!")
  
      if param=='JobStart' or param=='JobEnd': # We have to convert data format
        dateAndTime = job[param].split(' ')
        date = dateAndTime[0].split('-')
        time = dateAndTime[1].split(':')
        if len(time) > 2:
          timestamp = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), int(time[2]), 0)
        else:
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
                  attrList['JobEnd'], \
                  attrList['JobStart'], \
                  attrList['Location'], \
                  attrList['Name'], \
                  attrList['NumberOfEvents'], \
                  attrList['Production'], \
                  attrList['ProgramName'], \
                  attrList['ProgramVersion'], \
                  attrList['StatisticsRequested'], \
                  attrList['WNCPUPOWER'], \
                  attrList['CPUTIME'], \
                  attrList['WNCACHE'], \
                  attrList['WNMEMORY'], \
                  attrList['WNMODEL'], \
                  attrList['WorkerNode'], \
                  attrList['RunNumber'], \
                  attrList['FillNumber'], \
                  attrList['WNCPUHS06'], \
                  attrList['TotalLuminosity'] ])           
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
                    'FileSize':0, \
                    'FullStat':None, \
                    'QualityId': 'UNCHECKED', \
                    'Luminosity':0, \
                    'InstLuminosity':0}
      
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
      utctime = datetime.datetime.utcnow()
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
                    attrList['FileSize'], \
                    attrList['FullStat'], utctime,\
                    attrList['QualityId'], \
                    attrList['Luminosity'], \
                    attrList['InstLuminosity'] ] ) 
      return result
      
  #############################################################################
  def updateReplicaRow(self, fileID, replica): #, name, location):  
    result = self.dbW_.executeStoredProcedure('BKK_ORACLE.updateReplicaRow',[fileID, replica], False)
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
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.dbW_.executeStoredFunctions('BKK_ORACLE.insertSimConditions', LongType, [simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity])
  
  #############################################################################
  def getSimConditions(self):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getSimConditions',[])
  
  #############################################################################
  def insertDataTakingCond(self, conditions): 
    datataking = {  'Description':None,\
                    'BeamCond':None, \
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
                    'HLT':None,
                    'VeloPosition':None}
        
    for param in conditions:
      if not datataking.__contains__(param):
        gLogger.error("insert datataking error: "," the files table not contains "+param+" this attributte!!")
        return S_ERROR(" The datatakingconditions table not contains "+param+" this attributte!!")
      datataking[param] = conditions[param]
        
    res = self.dbW_.executeStoredFunctions('BKK_ORACLE.insertDataTakingCond', LongType, [datataking['Description'], datataking['BeamCond'], datataking['BeamEnergy'], \
                                                                                  datataking['MagneticField'], datataking['VELO'], \
                                                                                  datataking['IT'], datataking['TT'], datataking['OT'], \
                                                                                  datataking['RICH1'], datataking['RICH2'], \
                                                                                  datataking['SPD_PRS'], datataking['ECAL'], \
                                                                                  datataking['HCAL'], datataking['MUON'], datataking['L0'], datataking['HLT'], datataking['VeloPosition'] ])
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
          row = {'ADLER32':record[1],'CreationDate':record[2],'EventStat':record[3],'FullStat':record[10],'EventTypeId':record[4],'FileType':record[5],'GotReplica':record[6],'GUID':record[7],'MD5SUM':record[8],'FileSize':record[9],'DQFlag':record[11],'JobId':record[12],'RunNumber':record[13],'InsertTimeStamp':record[14],'Luminosity':record[15],'InstLuminosity':record[16]}
          result[file]= row
    return S_OK(result)
  
  #############################################################################
  def getFilesInformations(self,lfns):
    result = {}
    res = self.getFileMetadata(lfns)
    if not res['OK']:
      result = res
    else:
      records = res['Value']  
      for file in records:
        value = records[file]
        command = 'select jobs.runnumber,jobs.fillnumber, configurations.configname,configurations.configversion from jobs,configurations where configurations.configurationid= jobs.configurationid and jobs.jobid='+str(value['JobId'])
        res = self.dbR_._query(command)
        if not res['OK']:
          result[file]= res['Message']
        else:
            info = res['Value']
            if len(info)!=0:
              row = {'RunNumber':info[0][0],'FillNumber':info[0][1],'ConfigName':info[0][2],'ConfigVersion':info[0][3]}
              value.pop('ADLER32')
              value.pop('GUID')
              value.pop('MD5SUM')
              value.pop('JobId')
              row.update(value)
              result[file]= row
            else:
              result[file]= {}
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
    command = 'select count(*), SUM(files.EventStat), SUM(files.FILESIZE), sum(files.Luminosity), sum(files.instLuminosity) from files ,jobs where jobs.jobid=files.jobid and jobs.production='+str(prod)
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getProductionFilesForUsers(self, prod, ftypeDict, SortDict, StartItem, Maxitems):
    command = ''
    parametersNames = ['Name', 'FileSize','FileType','CreationDate','EventTypeId','EventStat','GotReplica', 'InsertTimeStamp', 'Luminosity', 'InstLuminosity']
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
        
        command = 'select rnum, filename, filesize, \''+str(ftype)+'\' , creationdate, eventtypeId, eventstat,gotreplica, inserttimestamp , luminosity ,instLuminosity from \
                ( select rownum rnum, filename, filesize, \''+str(ftype)+'\' , creationdate, eventtypeId, eventstat, gotreplica, inserttimestamp, luminosity,instLuminosity \
                from ( select files.filename, files.filesize, \''+str(ftype)+'\' , files.creationdate, files.eventtypeId, files.eventstat,files.gotreplica, files.inserttimestamp, files.luminosity, files.instLuminosity \
                           from jobs,files where \
                           jobs.jobid=files.jobid and \
                           files.filetypeid='+str(ftypeId)+' and \
                           jobs.production='+str(prod)+' Order by files.filename) where rownum <='+str(Maxitems)+ ') where rnum >'+str(StartItem) 
    else:
      
      command = 'select rnum, filename, filesize, name, creationdate, eventtypeId, eventstat,gotreplica, inserttimestamp, luminosity, instLuminosity from \
                ( select rownum rnum, filename, filesize, name, creationdate, eventtypeId, eventstat, gotreplica, inserttimestamp, luminosity, instLuminosity \
                from ( select files.filename, files.filesize, filetypes.name, files.creationdate, files.eventtypeId, files.eventstat,files.gotreplica, files.inserttimestamp, files.luminosity, files.instLuminosity  \
                           from jobs,files,filetypes where \
                           jobs.jobid=files.jobid and \
                           files.filetypeid=filetypes.filetypeid and \
                           jobs.production='+str(prod)+' Order by files.filename) where rownum <='+str(Maxitems)+ ') where rnum >'+str(StartItem) 
   
    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        row = [record[1],record[2],record[3],record[4],record[5],record[6],record[7], record[8]]
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
  def getRunInformations(self, runnb):
    command = 'select distinct jobs.fillnumber, configurations.configname, configurations.configversion, data_taking_conditions.description, pass_group.groupdescription, jobs.jobstart, jobs.jobend from \
          jobs, configurations,data_taking_conditions,productions, pass_group, pass_index \
         where jobs.configurationid=configurations.configurationid and data_taking_conditions.daqperiodid=productions.simcondid and \
         productions.passid=pass_index.passid and pass_index.groupid=pass_group.groupid and \
         jobs.production=productions.production and jobs.production<0 and jobs.runnumber='+str(runnb)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    value = retVal['Value']
    if len(value) == 0:
      return S_ERROR('This run is missing in the BKK DB!')
    result = {'Configuration Name':value[0][1],'Configuration Version':value[0][2],'FillNumber':value[0][0]}
    result['DataTakingDescription']=value[0][3]
    result['ProcessingPass']=value[0][4]
    result['RunStart'] = value[0][5]
    result['RunEnd'] = value[0][6]
    
    command = ' select count(*), SUM(files.EventStat), SUM(files.FILESIZE), sum(files.fullstat), files.eventtypeid , sum(files.luminosity), sum(files.instLuminosity)  from files,jobs \
         where files.JobId=jobs.JobId and  \
         files.gotReplica=\'Yes\' and \
         jobs.production<0 and \
         jobs.runnumber='+str(runnb)+' Group by files.eventtypeid'
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    value = retVal['Value']
    if len(value) == 0:
      return S_ERROR('Replica flag is not set!')
    nbfile = [] 
    nbevent = [] 
    fsize = [] 
    fstat = []
    stream = []
    luminosity = []
    ilumi = []
    for i in value:
      nbfile += [i[0]]
      nbevent += [i[1]]
      fsize += [i[2]]
      fstat += [i[3]]
      stream += [i[4]]
      luminosity += [i[5]]
      ilumi += [i[6]]
          
    result['Number of file'] = nbfile
    result['Number of events'] = nbevent
    result['File size'] = fsize
    result['FullStat'] = fstat
    result['Stream'] = stream
    result['luminosity'] = luminosity
    result['InstLuminosity'] = ilumi
    return S_OK(result)
  
  #############################################################################
  def checkProductionStatus(self, productionid = None, lfns = []):
    result = {}
    missing = []
    replicas = []
    noreplicas = []
    if productionid != None:
      command = 'select files.filename, files.gotreplica from files,jobs where \
                 files.jobid=jobs.jobid and \
                 jobs.production='+str(productionid)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return S_ERROR(retVal['Message'])
      files = retVal['Value']
      for file in files:
        if file[1] == 'Yes':
          replicas +=  [file[0]]
        else:
          noreplicas += [file[0]]
      result['replica'] = replicas
      result['noreplica'] = noreplicas
    elif len(lfns) != 0:
      for file in lfns:
        command = ' select files.filename, files.gotreplica from files where filename=\''+str(file)+'\''
        retVal = self.dbR_._query(command)
        if not retVal['OK']:
          return S_ERROR(retVal['Message'])
        value = retVal['Value']
        if len(value) == 0:
          missing += [file]
        else:
          for i in value:
            if i[1] == 'Yes':
              replicas +=  [i[0]]
            else:
              noreplicas += [i[0]]
      result['replica'] = replicas
      result['noreplica'] = noreplicas
      result['missing'] = missing
    
    return S_OK(result)
  
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
  def insertEventTypes(self, evid, desc, primary):
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.insertEventTypes',[desc, evid, primary], False)
  
  #############################################################################
  def updateEventType(self, evid, desc, primary):
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.updateEventTypes',[desc, evid, primary], False)
  
    
  #############################################################################
  def getProductionSummary(self, cName, cVersion, simdesc='ALL', pgroup='ALL', production='ALL', ftype='ALL', evttype='ALL'):
    conditions = ''
    
    if cName != 'ALL':
      conditions += ' and bookkeepingview.configname=\''+cName+'\'' 
        
    if cVersion != 'ALL':
      conditions += ' and bookkeepingview.configversion=\''+cVersion+'\''
        
    if ftype != 'ALL':
      fType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fType)
      if not res['OK']:
        gLogger.error(res['Message'])
        return S_ERROR(res['Message'])
      else:
        value = res['Value']
        if len(value) == 0:
          return S_ERROR('File type is not found!')
        ftypeId = res['Value'][0][0]
        conditions += ' and files.filetypeid='+str(ftypeId)
    else:
      conditions += ' and bookkeepingview.filetypeid= files.filetypeid  \
                      and bookkeepingview.filetypeid= filetypes.filetypeid '
    if evttype != 'ALL':
      conditions += ' and bookkeepingview.eventtypeid='+str(evttype)
    else:
      conditions += ' and bookkeepingview.eventtypeid=files.eventtypeid '
    
    if production != 'ALL':
      conditions += ' and jobs.production='+str(production)
      conditions += ' and jobs.production= bookkeepingview.production '
    else:
       conditions += ' and jobs.production= bookkeepingview.production '
  
    if simdesc != 'ALL':
      conditions += 'and productions.simcondid = simulationconditions.simid '
      conditions += 'and simulationconditions.simdescription = \''+str(simdesc)+'\''
    else:
      conditions += 'and productions.simcondid = simulationconditions.simid '    
    
    command = ' select bookkeepingview.configname, bookkeepingview.configversion, simulationconditions.simdescription, \
       pass_group.groupdescription, bookkeepingview.eventtypeid, \
       bookkeepingview.description, bookkeepingview.production, filetypes.name, sum(files.eventstat) \
  from jobs, bookkeepingview,files, filetypes, productions, simulationconditions, pass_index, pass_group \
        where jobs.jobid= files.jobid and \
        files.gotreplica=\'Yes\' and \
        bookkeepingview.programname= jobs.programname and \
        bookkeepingview.programversion= jobs.programversion and \
        bookkeepingview.production = productions.production and \
        productions.passid= pass_index.passid and \
        pass_index.groupid=pass_group.groupid' + conditions 
    
    command += ' Group by  bookkeepingview.configname, bookkeepingview.configversion, \
            simulationconditions.simdescription, pass_group.groupdescription,\
            bookkeepingview.eventtypeid, bookkeepingview.description,\
            bookkeepingview.production, filetypes.name'
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    
    parameters = ['ConfigurationName', 'ConfigurationVersion', 'SimulationDescription', 'Processing pass group', 'EventTypeId', 'EventType description','Production', 'FileType','Number of events']
    dbResult = retVal['Value']
    records = []
    nbRecords = 0
    for record in dbResult:
      row = [record[0],record[1],record[2],record[3],record[4],record[5],record[6], record[7],record[8]]
      records += [row]
      nbRecords += 1
    
    return S_OK({'TotalRecords':nbRecords,'ParameterNames':parameters,'Records':records,'Extras': {}})
            
  #############################################################################
  def getProductionSimulationCond(self, prod):
    command ='select simulationconditions.simdescription  FROM simulationconditions, productions where \
         productions.simcondid= simulationconditions.simid and productions.production='+str(prod)
     
    res = self.dbR_._query(command)
    simcond = ''
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      value = res['Value']
      if len(value) == 0:
        command = 'select DESCRIPTION from data_taking_conditions, productions where productions.simcondid=data_taking_conditions.daqperiodid and productions.production='+str(prod) 
        res = self.dbR_._query(command)
        if not res['OK']:
          return S_ERROR(res['Message'])
        else:
          value = res['Value']
          if len(value) != 0:
            daqdesc = value[0][0]
            return S_OK(daqdesc)
          else:
            return S_ERROR('The data taking or simulation condition is missing!')
      else:
        simcond = value[0][0]
    return S_OK(simcond)
  
  #############################################################################
  def getProductionProcessing(self, prod):
    command = 'select pass_group.groupdescription from pass_index, pass_group, productions where \
               productions.passid= pass_index.passid and pass_index.groupid= pass_group.groupid and productions.production='+str(prod)
    res = self.dbR_._query(command)
    procpass = ''
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      value = res['Value']
      procpass = value[0][0]
    return S_OK(procpass)
  
  #############################################################################
  def getFileHistory(self, lfn):
    command = 'select  files.fileid, files.filename,files.adler32,files.creationdate,files.eventstat,files.eventtypeid,files.gotreplica, \
files.guid,files.jobid,files.md5sum, files.filesize,files.fullstat, dataquality.dataqualityflag, files.inserttimestamp, files.luminosity, files.instLuminosity from files, dataquality \
where files.fileid in ( select inputfiles.fileid from files,inputfiles where files.jobid= inputfiles.jobid and files.filename=\''+str(lfn)+'\')\
and files.qualityid= dataquality.qualityid'
 
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    command = 'select * from productioninformations where production='+str(prodid)
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
  
  #############################################################################
  def getAvailableTags(self):
    command = 'select name, tag from tags order by inserttimestamp desc'
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      parameters = ['TagName','TagValue']
      dbResult = retVal['Value']
      records = []
      nbRecords = 0
      for record in dbResult:
        row = [record[0],record[1]]
        records += [row]
        nbRecords += 1
      return S_OK({'TotalRecords':nbRecords,'ParameterNames':parameters,'Records':records,'Extras': {}})        
    else:
      return retVal
  
  #############################################################################
  def getProcessedEvents(self, prodid):
    command = 'select sum(jobs.numberofevents) from jobs where jobs.production='+str(prodid)
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getRunsWithAGivenDates(self, dict):
    condition = ''
    if dict.has_key('AllowOutsideRuns') and dict['AllowOutsideRuns']:
      if not dict.has_key('StartDate') and not dict.has_key('EndDate'):
        return S_ERROR('The Start and End date must be given!')
      else:
        if dict.has_key('StartDate'):
          condition += ' and jobs.jobstart >= TO_TIMESTAMP (\''+str(dict['StartDate'])+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      
        if dict.has_key('EndDate'):
          condition += ' and jobs.jobstart <= TO_TIMESTAMP (\''+str(dict['EndDate'])+'\',\'YYYY-MM-DD HH24:MI:SS\')'
    else:
      if dict.has_key('StartDate'):
        condition += ' and jobs.jobstart >= TO_TIMESTAMP (\''+str(dict['StartDate'])+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      
      if dict.has_key('EndDate'):
        condition += ' and jobs.jobend <= TO_TIMESTAMP (\''+str(dict['EndDate'])+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      elif dict.has_key('StartDate') and not dict.has_key('EndDate'):
        d = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S') 
        condition += ' and jobs.jobend <= TO_TIMESTAMP (\''+str(d)+'\',\'YYYY-MM-DD HH24:MI:SS\')'
    
    command = ' select jobs.runnumber from jobs where jobs.production < 0'+ condition
    retVal = self.dbR_._query(command)
    runIds = []
    if retVal['OK']:
      records = retVal['Value'] 
      for record in records:
        if record[0] != None:
          runIds += [record[0]]
    else:
      return S_ERROR(retVal['Message'])
    
    if dict.has_key('CheckRunStatus') and dict['CheckRunStatus']:
      processedRuns = []
      notProcessedRuns = []
      for i in runIds:
        command = 'select files.filename from files,jobs where jobs.jobid=files.jobid and files.gotreplica=\'Yes\' and jobs.production<0 and jobs.runnumber='+str(i)
        retVal = self.dbR_._query(command)
        if retVal['OK']:
          files = retVal['Value']
          ok = True
          for file in files:
            name = file[0]
            retVal = self.getDescendents([name],1)
            files = retVal['Value']
            successful = files['Successful']
            failed = files['Failed']
            if len(successful[successful.keys()[0]]) == 0: 
              ok = False
          if ok:
            processedRuns += [i]
          else:
            notProcessedRuns += [i]  
      
      return S_OK({'Runs':runIds,'ProcessedRuns':processedRuns,'NotProcessedRuns':notProcessedRuns})
    else:
      return S_OK({'Runs':runIds})
    return S_ERROR()
    
  #############################################################################
  def getProductiosWithAGivenRunAndProcessing(self, dict):
    
    if dict.has_key('Runnumber'):
      run = dict['Runnumber']
    else:
      return S_ERROR('The run number is missing!')
    if dict.has_key('ProcPass'):
      proc = dict['ProcPass']
    else:
      return S_ERROR('The processing pass is missing!')
    
    command = 'select distinct bookkeepingview.production  from bookkeepingview, productions where bookkeepingview.runnumber='+str(run)+' and bookkeepingview.production>0 and bookkeepingview.production=productions.production ' 
    
    descriptions = proc.split('+')
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
    command += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
    retVal = self.dbR_._query(command)
    return retVal
  
  #############################################################################
  def getDataQualityForRuns(self, runs):
    command = ' select distinct jobs.runnumber,dataquality.dataqualityflag from files, jobs,dataquality where files.jobid=jobs.jobid and files.qualityid=dataquality.qualityid  and jobs.production<0 and ('
    conditions = ''
    for i in runs:
      conditions += ' jobs.runnumber='+str(i)+'or'
    conditions += conditions[:-2]+')'
    command += conditions
    retVal = self.dbR_._query(command)
    return retVal
  
  #############################################################################
  def setProductionVisible(self, prodid, Value):
    if Value:
      command = 'update productions set visible=\'1\' where production='+str(prodid) 
    else:
      command = 'update productions set visible=\'0\' where production='+str(prodid)
    retVal = self.dbW_._query(command)
    return retVal
  
  #############################################################################
  def getTotalProcessingPass(self, prod):
    command = 'select totalprocpass from productions where production='+str(prod)
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      value = retVal['Value']
      if len(value) == 0:
        return S_ERROR('The production doesnt exist in the production table!')
      else:
        proc = value[0][0]
        return S_OK(proc)
    else:
      return S_ERROR(retVal['Message'])
  
  #############################################################################
  def getRunFlag(self, runnb, processing):
    command = 'select qualityid from runquality where runnumber='+str(runnb)+' and procpass=\''+processing+'\''
    retVal = self.dbR_._query(command)
    if retVal["OK"]:
      value = retVal['Value']
      if len(value) == 0:
        return S_ERROR('The run is not flagged!')
      else:
        return S_OK(value[0][0])
    else:
      return S_ERROR(retVal['Message'])
  
  #############################################################################
  def setFilesInvisible(self, lfns):
    for i in lfns:
      res = self.dbW_.executeStoredProcedure('BKK_ORACLE.setFileInvisible', [i], False)
      if not res['OK']:
        return S_ERROR(res['Message'])
    return S_OK('The files are invisible!')
    
  
      
  