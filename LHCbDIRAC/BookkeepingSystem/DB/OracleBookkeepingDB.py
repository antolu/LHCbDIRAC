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
      condition += ' and bview.eventtypeid=90000000'
    
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