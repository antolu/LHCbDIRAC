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
    selection = 'stepid,stepname, applicationname,applicationversion,optionfiles,DDDB,CONDDB, extrapackages,VisibilityFlag'
    if len(dict) > 0:
      tables = 'steps'
      if dict.has_key('StartDate'):
        condition += ' steps.inserttimestamps >= TO_TIMESTAMP (\''+dict['StartDate']+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      if dict.has_key('StepId'):
        command = 'select '+selection+' from '+tables+' where stepid='+str(dict['StepId'])+' order by inserttimestamps desc'
        return self.dbR_._query(command)
    else:
      command = 'select '+selection+' from steps order by inserttimestamps desc'
      return self.dbR_._query(command)
  
  #############################################################################
  def getStepInputFiles(self, StepId):
    command = 'select inputFiletypes.name,inputFiletypes.visibilityflag from steps, table(steps.InputFileTypes) inputFiletypes where  steps.stepid='+str(StepId)
    return self.dbR_._query(command)
          
  #############################################################################
  def getStepOutputFiles(self, StepId):
    command = 'select outputfiletypes.name,outputfiletypes.visibilityflag from steps, table(steps.outputfiletypes) outputfiletypes where  steps.stepid='+str(StepId)
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
    selection = 'insert into steps(stepname,applicationname,applicationversion,OptionFiles,dddb,conddb,extrapackages,visibilityflag'
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
    