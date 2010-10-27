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
    selection = ' s.stepid, s.stepname, s.applicationname, s.applicationversion,s.optionfiles, s.dddb,s.conddb,s.extrapackages, s.visible,i.name,i.visible,o.name,o.visible '
    if len(dict) > 0:
      tables = 'steps s, table(s.inputfiletypes)(+) i, table(s.outputfiletypes)(+) o'
      if dict.has_key('StartDate'):
        condition += ' s.inserttimestamps >= TO_TIMESTAMP (\''+dict['StartDate']+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      if dict.has_key('StepId'):
        if len(condition) > 0:
          condition += ' and '
        condition += ' s.stepid='+str(dict['StepId'])
      
      command = 'select '+selection+' from '+tables+' where '+condition+'order by s.inserttimestamps desc'
      return self.dbR_._query(command)
    else:
      command = 'select '+selection+' from steps s, table(s.inputfiletypes)(+) i, table(s.outputfiletypes)(+) o order by s.inserttimestamps desc'
      return self.dbR_._query(command)
  
  #############################################################################
  def getStepInputFiles(self, StepId):
    command = 'select inputFiletypes.name,inputFiletypes.visible from steps, table(steps.InputFileTypes) inputFiletypes where  steps.stepid='+str(StepId)
    return self.dbR_._query(command)
          
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
      
    retVal = self._getDataTakingConditionId(conddescription)
    if retVal['OK']:
      if retVal['Value'] != -1:
        condition += ' and productionscontainer.DAQPERIODID ='+str(retVal['Value'])
      else:
        retVal = self._getSimulationConditioId(conddescription)
        if retVal['OK']:
          if retVal['Value'] != -1:
           condition += ' and productionscontainer.simid ='+str(retVal['Value'])
          else:
            return S_ERROR('Condition does not exists!') 
        else:
          return retVal
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