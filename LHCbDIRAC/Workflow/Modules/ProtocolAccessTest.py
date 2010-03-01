# $Id: InputDataByProtocol.py 18161 2009-11-11 12:07:09Z acasajus $
__RCSID__ = "$Id: InputDataByProtocol.py 18161 2009-11-11 12:07:09Z acasajus $"

""" The Protocol Access Test module opens connections to the supplied files for the supplied protocols.
    It measures the times taken to open, close and read events from the files.
    It produced statistics on the observed performance of each of the protocols.
"""

from DIRAC                                                          import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.Utilities.ModuleFactory                             import ModuleFactory
from DIRAC.Core.Utilities.List                                      import sortList
from DIRAC.Core.Utilities.Statistics                                import getMean,getMedian,getVariance,getStandardDeviation
from DIRAC.DataManagementSystem.Client.ReplicaManager               import ReplicaManager
from LHCbDIRAC.Core.Utilities.ClientTools                           import readFileEvents
import os,sys,re,string

COMPONENT_NAME = 'ProtocolAccessTest'

class ProtocolAccessTest:

  #############################################################################
  def __init__(self,argumentsDict):
    """ Standard constructor """
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger(self.name)
    self.argumentsDict = argumentsDict

  #############################################################################
  def execute(self):
    
    fileLocations = {}
    failedInitialise = {}

    # Obtain the turls for accessing remote files via protocol
    protocols = self.argumentsDict['Protocols']
    for protocol in protocols:
      protocolArguments = self.argumentsDict.copy()
      protocolArguments['Configuration']['Protocol'] = protocol
      res = self.__getProtocolLocations(protocolArguments)
      if not res['OK']:
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
    res = self.__downloadInputData(self.argumentsDict)
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
    timingResults = open('TimingResults.txt','w')
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
        res = readFileEvents(turl)
        if not res['OK']:
          self.log.info("Failed to read events for protocol %s: %s" % (protocol,res['Value']))
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
        timingResults = open('TimingResults.txt','a')
        timingResults.write('%s\n' % statsString)
        timingResults.close()
    self.log.info("%s %s %s %s %s %s %s" % ('lfn'.ljust(70),'protocol'.ljust(10),'opening'.ljust(10),'events'.ljust(10),'mean'.ljust(10),'stdev'.ljust(10),'median'.ljust(10)))
    for statString in statsStrings:
      self.log.info(statString)
    return S_OK()

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

  def __generateStats(self,readTimes):
    resDict = {}
    resDict['Elements'] = len(readTimes)
    resDict['Median'] = getMedian(readTimes)
    resDict['Mean'] = getMean(readTimes)
    resDict['Variance'] = getVariance(readTimes,posMean=resDict['Mean'])
    resDict['StdDev'] = getStandardDeviation(readTimes,variance=resDict['Variance'],mean=resDict['Mean'])
    return resDict

  #############################################################################
  def __setJobParam(self,name,value):
    """Wraps around setJobParameter of state update client """
    if not self.jobID:
      return S_ERROR('JobID not defined')
    jobReport = RPCClient('WorkloadManagement/JobStateUpdate',timeout=120)
    jobParam = jobReport.setJobParameter(int(self.jobID),str(name),str(value))
    self.log.verbose('setJobParameter(%s,%s,%s)' %(self.jobID,name,value))
    if not jobParam['OK']:
      self.log.warn(jobParam['Message'])
    return jobParam
