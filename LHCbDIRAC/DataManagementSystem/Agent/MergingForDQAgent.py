__RCSID__ = "$Id: $"
'''
MergingForDQAgent automatize some operations done by the data quality crew. It looks for Runs that have not been flagged 
(i.e. that are UNCHECKED). If it finds out some of them he performes a merging of the BRUNELHIST and DAVINCIHIST root files.
The main steps are the following:

- Retrieving the UNCHECKED DAVINCIHISTs and BRUNELHISTs by run. The BKquery the perform this will be changed in the new release.

- The files are saved locally and merged in three steps. The first step consists in merging separatly the two kinds of histograms in groups of 50.
  This grouping is made because the merging is basically CPU consuming. The output files of this first step are then merged as well.
  Finally DAVINCI and BRUNEL histograms are then put together in a single file.
 
- As it is no result is uploaded everything is left in the homeDir.

- The initialize method gather informations from etc/dirac.cfg  
          
'''

import DIRAC
from DIRAC                                                     import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule                               import AgentModule
from LHCbDIRAC.DataManagementSystem.Utilities.MergeForDQ       import *
from DIRAC.Core.Base                                           import Script
from DIRAC.Interfaces.API.Dirac                                import Dirac
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient   import BookkeepingClient
from DIRAC import gConfig
import re,os,string
import subprocess
from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getProjectEnvironment

__RCSID__ = "$Id:"


AGENT_NAME = 'DataManagement/MergingForDQAgent'

class MergingForDQAgent( AgentModule ):

  def initialize( self ):
    self.am_setOption( 'shifterProxy', 'DataManager' )
    self.systemConfiguration = 'x86_64-slc5-gcc43-opt' 
    Configuration = {'ExeDir' : False,
                     'homeDir' : False, 
                     'senderAddress' : False,
                     'mailAddress' : False,
                     'applicationName' : False,
                     'eventType' : False,
                     'cfgName' : False,
                     'cfgVersion' : False,
                     'dqFlag' : False,
                     'evtTypeDict' : False,
                     'histTypeList' : False,
                     'testMode' : False,
                     'specialMode' : False,
                     'specialRuns' : False}

    gLogger.info('=====Gathering information from dirac.cfg=====')    
    self.applicationName = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/applicationName" )
    if self.applicationName: Configuration['applicationName']=True     
    self.homeDir = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/homeDir" )
    if self.homeDir: 
      Configuration['homeDir']=True
      '''
      Local directory creation 
      '''
      d = os.path.dirname(self.homeDir)
      gLogger.info( 'Checking temp dir %s' % ( d ) )
      if not os.path.exists(d):
        gLogger.info( '%s not found creating' % ( d ) )
        os.makedirs(d)

    
    '''
    Compiled C++ root macros for the three Merging steps. 
    '''
    self.mergeExeDir = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/ExeDir" )

    if self.mergeExeDir: 
      Configuration['ExeDir']=True
      self.mergeStep1Command = self.mergeExeDir + '/Merge'
      self.mergeStep2Command = self.mergeExeDir + '/Merge2'
      self.mergeStep3Command = self.mergeExeDir + '/Merge3'

    self.senderAddress = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/senderAddress" )
    if self.senderAddress: Configuration['senderAddress']=True
    self.mailAddress = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/mailAddress" )
    if self.mailAddress:Configuration['mailAddress']=True
    self.thisEventType = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/eventType" )
    if self.thisEventType: Configuration['eventType']=True
    self.cfgName = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/cfgName" )
    if self.cfgName: Configuration['cfgName']=True
    self.cfgVersion = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/cfgVersion" )
    if self.cfgVersion: Configuration['cfgVersion']=True
    self.dqFlag = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/dqFlag" )
    if self.dqFlag: Configuration['dqFlag']=True
    self.dqFlag = 'UNCHECKED'
    self.evtTypeList = {'EXPRESS' : '91000000', 'FULL'    : '90000000'}
    self.histTypeList = ['BRUNELHIST', 'DAVINCIHIST']
    self.brunelCount = 0
    self.daVinciCount = 0

    TypeDict = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/evtTypedict" )
    if TypeDict:
      l = string.join(TypeDict.split(),"")
      ll = l.split(",")
      self.evtTypeDict = {}
      for t in ll:
        s =  t.split(":")  
        self.evtTypeDict[s[0]]=s[1]
    Configuration['evtTypeDict']=True
    
    List = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/histTypeList" ) 
    if List:
      l = string.join(List.split(),"")
      self.histTypeList = l.split(",")
      Configuration['histTypeList']=True

    self.testMode = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/testMode" )
    if self.testMode: Configuration['testMode']=True
    self.specialMode = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/specialMode" )
    if self.specialMode: Configuration['specialMode']=True
    Runs = gConfig.getValue( "Systems/DataManagement/Development/Agents/MergingForDQAgent/specialRuns" )
    if Runs: 
      l = string.join(Runs.split(),"")
      self.specialRuns = l.split(",")
    Configuration['specialRuns']=True
   
    nConf=False
    for confVar in Configuration:
      if not Configuration[confVar]:
        gLogger.error('%s not specified in dirac.cfg'% confVar)
        nConf=True
    if nConf: DIRAC.exit(2) 

    gLogger.info('=====All informations from dirac.cfg retrieved=====')     
    self.exitStatus = 0
    env = dict(os.environ)
    env['USER']='dirac'
    res = getProjectEnvironment(self.systemConfiguration, self.applicationName, applicationVersion = '', extraPackages = '',
                                runTimeProject = '', site = '', directory = '', generatorName = '',
                                poolXMLCatalogName = 'pool_xml_catalog.xml', env = env )

    if not res['OK']:
      gLogger.error('===== Cannot create the environment check LocalArea or SharedArea in dirac.cfg =====')
      DIRAC.exit( 2 )
    
    self.environment = res['Value']

    self.logFileName = '%s/Merge_%s_histograms.log' % (self.homeDir , self.thisEventType)    

    self.logFile = open( self.logFileName, 'a' )
    
    self.dirac = Dirac()
    self.bkClient = BookkeepingClient()
 
    evtTypeId = int( self.evtTypeDict[self.thisEventType] )
    
    '''

    Here the list of run is built

    '''
    
    gLogger.info('=====Retrieving the list of runs=====')
    
    bkTree = {self.cfgName : {}}
    bkDict = {'ConfigName' : self.cfgName}
    allConfig = self.bkClient.getAvailableConfigurations()
    for i in range( len( allConfig['Value'] ) ):
      if allConfig['Value'][i][0] == self.cfgName:
        if re.search( self.cfgVersion, allConfig['Value'][i][1] ):
          self.cfgVersion = allConfig['Value'][i][1]
          if not bkTree[self.cfgName].has_key( self.cfgVersion ):
            bkTree[self.cfgName][self.cfgVersion] = {}

    bkTree, res = GetRunningConditions( bkTree ,self.bkClient )
    if not res['OK']:
      gLogger.error( 'Running Conditions not found' )
      DIRAC.exit( 2 )

    bkTree, res = GetProcessingPasses( bkTree , self.bkClient )
    if not res['OK']:
      DIRAC.exit( 2 )

    self.bkDict , res = GetRuns( bkTree, self.bkClient, self.thisEventType,self.evtTypeDict, self.dqFlag )

    if not res['OK']:
      gLogger.error( 'Cannot load the run list for version %s' % ( self.cfgVersion ) )
      gLogger.error( res['Message'] )
      DIRAC.exit( 2 )

    return S_OK()

  def execute( self ):
    
    res = MergeRun( self.bkDict, self.thisEventType , self.histTypeList , self.bkClient , self.homeDir , self.testMode, self.specialMode , 
                    self.mergeExeDir , self.mergeStep1Command, self.mergeStep2Command,self.mergeStep3Command, 
                    self.brunelCount ,self.daVinciCount , self.logFile , self.logFileName , self.dirac , 
                    self.senderAddress,self.mailAddress,self.environment)
    
    return S_OK()
