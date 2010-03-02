########################################################################
# $Id: ProtocolAccessTest.py 22010 2010-02-22 16:31:05Z paterson $
########################################################################

__RCSID__ = "$Id: ProtocolAccessTest.py 22010 2010-02-22 16:31:05Z paterson $"

""" The Protocol Access Test module opens connections to the supplied files for the supplied protocols.
    It measures the times taken to open, close and read events from the files.
    It produced statistics on the observed performance of each of the protocols.
"""

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities.ModuleFactory                             import ModuleFactory
from DIRAC.Core.Utilities.List                                      import sortList
from DIRAC.Core.Utilities.Statistics                                import getMean,getMedian,getVariance,getStandardDeviation
from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager

from LHCbDIRAC.Workflow.Modules.ModuleBase                          import ModuleBase
from LHCbDIRAC.Core.Utilities.ClientTools                           import readFileEvents

import os,sys,re,string

COMPONENT_NAME = 'ProtocolAccessTest'

class ProtocolAccessTest(ModuleBase):

  #############################################################################
  def __init__(self):
    """ Standard constructor """
    ModuleBase.__init__(self)    
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger(self.name)
    self.version = __RCSID__
    self.inputData=''
    self.systemConfig=''
    self.applicationLog=''
    self.applicationVersion=''
    self.protocolsList=''
    self.rootVersion = ''

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the module parameters are resolved here.
    """
    self.log.info(self.workflow_commons)
    self.log.info(self.step_commons)
    result = S_OK()

    if self.step_commons.has_key('inputData'):
      self.inputData = self.step_commons['inputData']
    elif self.workflow_commons.has_key('InputData'):
      self.inputData = self.workflow_commons['InputData']
    else:
      result = S_ERROR('No Input Data Defined')    

    if type(self.inputData) != type([]):
      self.inputData = self.inputData.split(';')
    self.inputData = [x.replace('LFN:','') for x in self.inputData]
    
    if self.step_commons.has_key('applicationLog'):
      self.applicationLog = self.step_commons['applicationLog']
    
    if not self.applicationLog:
      if self.step_commons.has_key('STEP_NUMBER'):
        self.applicationLog='TimingResults_%s.log' %(self.step_commons['STEP_NUMBER'])
     
    if self.step_commons.has_key('protocols'):
      self.protocolsList = self.step_commons['protocols']
      if type(self.protocolsList) != type([]):
        self.protocolsList = self.protocolsList.split(';')
      self.protocolsList = [x.lower() for x in self.protocolsList]
    else:
      result = S_ERROR('No protocols list defined')
    
    if self.step_commons.has_key('applicationVersion'):
      self.applicationVersion = self.step_commons['applicationVersion']
    else:
      result = S_ERROR('No application version specified')
    
    if self.workflow_commons.has_key('SystemConfig'):
      self.systemConfig = self.workflow_commons['SystemConfig']
    
    if self.step_commons.has_key('rootVersion'):
      self.rootVersion = self.step_commons['rootVersion']
    
    return result
    
  #############################################################################
  def execute(self):
    """ The main execution method of the protocol access test module.
    """
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    self.log.info('Initializing %s' %self.version)
    rm = ReplicaManager()
    self.log.info('Attempting to get replica and metadata information for:\n%s' %(string.join(self.inputData,'\n')))

    replicaRes = rm.getReplicas(self.inputData)
    if not replicaRes['OK']:
      self.log.error(replicaRes)
      return S_ERROR('Could not obtain replica information')
    if replicaRes['Value']['Failed']:
      self.log.error(replicaRes)
      return S_ERROR('Could not obtain replica information')
    
    metadataRes = rm.getCatalogFileMetadata(self.inputData)
    if not metadataRes['OK']:
      self.log.error(metadataRes)
      return S_ERROR('Could not obtain metadata information')
    if metadataRes['Value']['Failed']:
      self.log.error(metadataRes)
      return S_ERROR('Could not obtain metadata information')
    
    for lfn,metadata in metadataRes['Value']['Successful'].items():
      replicaRes['Value']['Successful'][lfn].update(metadata)

    catalogResult = replicaRes
    
    localSE = gConfig.getValue('/LocalSite/LocalSE',[])
    if not localSE:
      return S_ERROR('Could not determine local SE list')
    
    seConfig =  {'LocalSEList':localSE,'DiskSEList' :localSE,'TapeSEList' :localSE}
    argumentsDict = {'InputData':self.inputData,'Configuration':seConfig,'FileCatalog': catalogResult,'Protocols' : self.protocolsList}
    
    if self.systemConfig:
      self.log.info('Setting system configuration (CMTCONFIG) to %s' %self.systemConfig)
      os.environ['CMTCONFIG']=self.systemConfig
      
    if self.rootVersion:
      self.log.info('Requested ROOT version %s corresponding to DaVinci version %s' %(self.rootVersion,self.applicationVersion))
    
    fileLocations = {}
    failedInitialise = {}
    # Obtain the turls for accessing remote files via protocol
    for protocol in self.protocolsList:
      protocolArguments = argumentsDict.copy()
      protocolArguments['Configuration']['Protocol'] = protocol
      res = self.__getProtocolLocations(protocolArguments)
      if not res['OK']:
        self.log.error(res)
        continue
      for failed in res['Value']['Failed']:
        if not failedInitialise.has_key(failed):
          failedInitialise[failed] = []
        failedInitialise[failed].append(protocol)
      for lfn in sortList(res['Value']['Successful'].keys()):
        if not fileLocations.has_key(lfn):
          fileLocations[lfn] = {}
        turl = res['Value']['Successful'][lfn]
        fileLocations[lfn][protocol] = turl

    # Obtain a local copy of the data for benchmarking
    res = self.__downloadInputData(argumentsDict)
    if not res['OK']:
      return S_ERROR("Failed to get local copy of data")
    for failed in res['Value']['Failed']:
      if not failedInitialise.has_key(failed):
          failedInitialise[failed] = []
      failedInitialise[failed].append('local')
    for lfn in sortList(res['Value']['Successful'].keys()):
      if not fileLocations.has_key(lfn):
        fileLocations[lfn] = {}
      turl = res['Value']['Successful'][lfn]
      fileLocations[lfn]['local'] = turl

    # For any files that that failed to initialise
    timingResults = open(self.applicationLog,'w')
    for lfn in sortList(failedInitialise.keys()):
      for protocol in sortList(failedInitialise[lfn]):
        statsString = "%s %s %s %s %s %s %s" % (lfn.ljust(70),protocol.ljust(10),'I'.ljust(10),str(0.0).ljust(10),str(0.0).ljust(10),str(0.0).ljust(10),str(0.0).ljust(10))
        timingResults.write('%s\n' % statsString)
    timingResults.close()

    gLogger.info("Will test the following files:")
    for lfn in sortList(fileLocations.keys()):
      for protocol in sortList(fileLocations[lfn].keys()):
        turl = fileLocations[lfn][protocol]
        self.log.info("%s %s" % (lfn,turl))
 
    statsStrings = []
    for lfn in sortList(fileLocations.keys()):
      protocolDict = fileLocations[lfn]
      for protocol in sortList(protocolDict.keys()):
        turl = protocolDict[protocol]
        res = readFileEvents(turl,self.applicationVersion)
        if not res['OK']:
          self.log.info("Failed to read events for protocol %s: %s" % (protocol,res))
          openTime = 'F'
          readTimes = [0.0]
        else:
          openTime = "%.4f" % res['Value']['OpenTime'] 
          readTimes = res['Value']['ReadTimes']
        statsDict = self.__generateStats(readTimes)
        events = "%d" % statsDict['Elements']
        mean = "%.7f" % statsDict['Mean']
        stdDev = "%.7f" % statsDict['StdDev']
        median = "%.7f" % statsDict['Median']
        statsString = "%s %s %s %s %s %s %s" % (lfn.ljust(70),protocol.ljust(10),str(openTime).ljust(10),str(events).ljust(10),str(mean).ljust(10),str(stdDev).ljust(10),str(median).ljust(10))
        statsStrings.append(statsString)
        timingResults = open(self.applicationLog,'a')
        timingResults.write('%s\n' % statsString)
        timingResults.close()
    self.log.info("%s %s %s %s %s %s %s" % ('lfn'.ljust(70),'protocol'.ljust(10),'opening'.ljust(10),'events'.ljust(10),'mean'.ljust(10),'stdev'.ljust(10),'median'.ljust(10)))
    for statString in statsStrings:
      self.log.info(statString)
    return S_OK()
  
  #############################################################################
  def __getProtocolLocations(self,argumentsDict):
    protocol = argumentsDict['Configuration']['Protocol']
    moduleFactory = ModuleFactory()
    moduleInstance = moduleFactory.getModule('DIRAC.WorkloadManagementSystem.Client.InputDataByProtocol',argumentsDict)
    if not moduleInstance['OK']:
      return moduleInstance
    module = moduleInstance['Value']
    res = module.execute()
    failed = []
    if not res['OK']:
      self.log.error("Failed to get turl for files",res['Message'])
      failed = argumentsDict['InputData']
    for lfn in sortList(res['Failed']):
      self.log.error("Failed to get turl for protocol", "%s %s" % (protocol , lfn))
      failed.append(lfn)
    if not res['Successful']:
      self.log.error("Failed to obtain turl for any data")
      failed =  argumentsDict['InputData']
    successful = {}
    if res['Successful']:
      self.log.info("Successfully obtained turls for %s at:" % protocol)
      for lfn in sortList(res['Successful'].keys()):
        turl = res['Successful'][lfn]['turl']
        successful[lfn] = turl
        self.log.info("%s : %s" % (lfn.ljust(50),turl.ljust(50)))
    return S_OK({'Successful':successful,'Failed':failed})

  #############################################################################
  def __downloadInputData(self,argumentsDict):
    # Prepare the files to be tested locally so that we have a bench mark of performance
    moduleFactory = ModuleFactory()
    moduleInstance = moduleFactory.getModule('DIRAC.WorkloadManagementSystem.Client.DownloadInputData',argumentsDict)
    if not moduleInstance['OK']:
      return moduleInstance
    module = moduleInstance['Value']
    res = module.execute()
    failed = []
    if not res['OK']:
      self.log.error("Failed to download file locally",res['Message'])
      failed = argumentsDict['InputData']
    for lfn in sortList(res['Failed']):
      self.log.error("Failed to download %s locally", "%s %s" % (lfn,error)) 
      failed.append(lfn)
    if not res['Successful']:
      self.log.error("Failed to download any data locally")   
      failed = argumentsDict['InputData']
    successful = {}
    if res['Successful']:
      self.log.info("Successfully obtained local copies of files at:")
      for lfn in sortList(res['Successful'].keys()):
        path = res['Successful'][lfn]['path']
        successful[lfn] = path
        self.log.info("%s : %s" % (lfn.ljust(50),path.ljust(50)))
    return S_OK({'Successful':successful,'Failed':failed})
  
  #############################################################################
  def __generateStats(self,readTimes):
    resDict = {}
    resDict['Elements'] = len(readTimes)
    resDict['Median'] = getMedian(readTimes)
    resDict['Mean'] = getMean(readTimes)
    resDict['Variance'] = getVariance(readTimes,posMean=resDict['Mean'])
    resDict['StdDev'] = getStandardDeviation(readTimes,variance=resDict['Variance'],mean=resDict['Mean'])
    return resDict
