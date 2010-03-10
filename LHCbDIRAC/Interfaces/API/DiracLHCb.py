########################################################################
# $HeadURL$
# Author: Andrew C. Smith, Stuart Paterson
########################################################################

"""
   LHCb API Class

   The LHCb API exposes LHCb specific functionality in addition to the standard DIRAC API.

"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

__RCSID__ = "$Id$"

from DIRAC                                          import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.List                      import breakListIntoChunks, sortList
from DIRAC.Interfaces.API.Dirac                     import Dirac

from LHCbDIRAC.Core.Utilities.ClientTools           import mergeRootFiles,getRootFileGUID
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

import os,glob,fnmatch,string,time

COMPONENT_NAME='DiracLHCb'

class DiracLHCb(Dirac):

  #############################################################################
  def __init__(self, WithRepo=False, RepoLocation=''):
    """Internal initialization of the DIRAC API.
    """
    Dirac.__init__(self,WithRepo=WithRepo, RepoLocation=RepoLocation)
    self.rootSection = '/Operations/SoftwareDistribution/LHCbRoot'
    self.softwareSection = '/Operations/SoftwareDistribution'
    self.bkQueryTemplate = { 'SimulationConditions'     : 'All',
                             'DataTakingConditions'     : 'All',
                             'ProcessingPass'           : 'All',
                             'FileType'                 : 'All',
                             'EventType'                : 'All',
                             'ConfigName'               : 'All',
                             'ConfigVersion'            : 'All',
                             'ProductionID'             : 'All',
                             'DataQualityFlag'          : 'All'}
    
  #############################################################################
  def addRootFile(self,lfn,fullPath,diracSE,printOutput=False):
    """ Add a Root file to Grid storage, an attempt is made to retrieve the 
        POOL GUID of the file prior to upload. 
        
       Example Usage:

       >>> print dirac.addFile('/lhcb/user/p/paterson/myRootFile.tar.gz','myFile.tar.gz','CERN-USER')
       {'OK': True, 'Value':{'Failed': {},
        'Successful': {'/lhcb/user/p/paterson/test/myRootFile.tar.gz': {'put': 64.246301889419556,
                                                                    'register': 1.1102778911590576}}}}

       @param lfn: Logical File Name (LFN)
       @type lfn: string
       @param diracSE: DIRAC SE name e.g. CERN-USER
       @type diracSE: strin
       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR        
    """
    res = getRootFileGUID(fullPath)
    if not res['OK']:
      return self.__errorReport(res['Message'],"Failed to obtain root file GUID.")
    res = self.addFile(lfn,fullPath,diracSE,fileGuid=res['Value'],printOutput=printOutput)
    return res

  #############################################################################
  def rootMergeRepository(self,outputFileName,inputFileMask='*.root',location='Sandbox',requestedStates=['Done']):
    """ Create a merged ROOT file using root files retrived in the sandbox or output data
    
       Example Usage:

       >>> print dirac.rootMergeRepository('MyMergedRootFile.root',inputFileMask='DVHistos.root',location='Sandbox', requestedStates = ['Done'])
       {'OK': True, 'Value': ''}
     
       @param outputFileName: The target merged file
       @type outputFileName: string
       @param inputFileMask: Mask to be used when locating input files. Can support wildcards like 'Tuple*.root'
       @type inputFileMask: string
       @param location: The input files present either in the 'Sandbox' (retrieved with getOutputSandbox) or 'OutputFiles' (getJobOutputData)
       @type location: string
       @param requestedStates: List of jobs states to be considered
       @type requestedStates: list of strings
       @return: S_OK,S_ERROR
    """
    if not self.jobRepo:
      self.log.warn("No repository is initialised") 
      return S_OK()  

    # Get the input files to be used
    jobs = self.jobRepo.readRepository()['Value']
    inputFiles = []
    for jobID in sortList(jobs.keys()):
      jobDict = jobs[jobID]
      if jobDict.has_key('State') and jobDict['State'] in requestedStates:
        if location == 'OutputFiles':
          jobFiles = eval(jobDict[location])
          for jobFile in jobFiles:
            fileName = os.path.basename(jobFile)
            if fnmatch.fnmatch(fileName,inputFileMask):
              if os.path.exists(jobFile):
                inputFiles.append(jobFile)
              else:
                self.log.warn("Repository output file does not exist locally",jobFile)
        elif location == 'Sandbox':
          globStr = "%s/%s" % (jobDict[location],inputFileMask)
          print glob.glob(globStr)
          inputFiles.extend(glob.glob(globStr))
        else:
          return self.__errorReport("Location of .root should be 'Sandbox' or 'OutputFiles'.")

    # Perform the root merger
    res = mergeRootFiles(outputFileName,inputFiles,daVinciVersion='')
    if not res['OK']:
      return self.__errorReport(res['Message'],"Failed to perform final ROOT merger")
    return S_OK()

  #############################################################################
  def getRootVersions(self,printOutput=False):
    """ Return the list of currently supported LHCb Root versions.
    
        Example Usage:
        
        >>> print dirac.getRootVersions()
        {'OK': True, 'Value': {'5.26.00b': 'DaVinci.v25r1', '5.22.00d': 'DaVinci.v24r3p2', '5.22.00c': 'DaVinci.v24r2p3', '5.22.00b': 'DaVinci.v23r2p1', '5.22.00a': 'DaVinci.v23r0p1', '5.14.00h': 'DaVinci.v19r8', '5.14.00i': 'DaVinci.v19r9', '5.18.00d': 'DaVinci.v20r3', '5.18.00f': 'DaVinci.v21r0', '5.24.00b': 'DaVinci.v24r7p3', '5.18.00a': 'DaVinci.v19r12', '4.04.02': 'DaVinci.v14r5', '3.10.02': 'DaVinci.v12r18', '5.18.00': 'DaVinci.v19r10', '5.14.00f': 'DaVinci.v19r5'}}
        
       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR           
    """
    rootVersions = gConfig.getOptionsDict(self.rootSection)
    if not rootVersions['OK']:
      return self.__errorReport(rootVersions,'Could not contact DIRAC Configuration Service for supported ROOT version list')

    if printOutput:
      rootList = []
      rootDict = rootVersions['Value']
      for r,d in rootDict.items():
        rootList.append('%s = %s' %(r,d))
      self.log.info('Supported versions of ROOT (and corresponding DaVinci versions) in LHCb are:\n%s' %(string.join(rootList,'\n')))
      
    return rootVersions

  #############################################################################
  def getSoftwareVersions(self,printOutput=False):
    """ Return and optionally print a list of all currently supported LHCb 
        software versions.  This includes all software packages that DIRAC 
        is asked to specifically install for given system configurations.
        
        Example Usage:
        
        >>> print dirac.getSoftwareVersions()
        {'OK': True, 'Value': {'Compat': {'v1r3': ['slc4_ia32_gcc34', 'x86_64-slc5-gcc43-opt']}, 'LHCbGrid': {'v1r7': ['slc4_amd64_gcc34', 'slc4_ia32_gcc34']}}

       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR            
    """
    softwareDistribution = gConfig.getOptionsDict(self.softwareSection)
    if not softwareDistribution['OK']:
      return self.__errorReport(rootVersions,'Could not contact DIRAC Configuration Service for supported software version list')

    software = softwareDistribution['Value']
    systemConfigs = software.keys()
    systemConfigs.remove('Active')
    systemConfigs.remove('Deprecated')
    
    active = software['Active'].replace(' ','').split(',')
    active.sort()
    deprecated = software['Deprecated'].replace(' ','').split(',')
    deprecated.sort()
    
    if printOutput:
      print '=========> Active LHCb Software For All System Configurations' 

    result = {}
    for package in active:
      packageArch = []
      for systemConfig in systemConfigs:
        if package in software[systemConfig]:
          packageArch.append(systemConfig)
      name = package.split('.')[0]
      version = package.split('.')[1]
      if packageArch:
        adj = 30
        if printOutput:
          print name.ljust(adj)+version.ljust(adj)+string.join(packageArch,',').ljust(adj)
        if not result.has_key(name):
          result[name]={}
        result[name][version]=packageArch
      else:
        self.log.warn('%s %s is not defined for any system configurations' %(name,version))

    return S_OK(result)

  #############################################################################
  def bookkeepingQuery(self,SimulationConditions='All',DataTakingConditions='All',ProcessingPass='All',FileType='All',EventType='All',ConfigName='All',ConfigVersion='All',ProductionID=0,DataQualityFlag='ALL'):
    """ This function will create and perform a BK query using the supplied arguments 
        and return a list of LFNs.
        
        Example Usage:
        
        >>> dirac.bookkeepingQuery(ConfigName='LHCb',ConfigVersion='Beam1',EventType='90000000',ProcessingPass='Real Data')
        {'OK':True,'Value':<files>}
        
       @param  ConfigName: BK ConfigName
       @type ConfigName: string
       @param  EventType: BK EventType
       @type EventType: string
       @param  FileType: BK FileType
       @type FileType: string
       @param  ProcessingPass: BK ProcessingPass
       @type ProcessingPass: string
       @param  ProductionID: BK ProductionID
       @type ProductionID: integer
       @param  DataQualityFlag: BK DataQualityFlag
       @type DataQualityFlag: string
       @param  ConfigVersion: BK ConfigVersion
       @type ConfigVersion: string
       @param  DataTakingConditions: BK DataTakingConditions
       @type DataTakingConditions: string
       @param  SimulationConditions: BK SimulationConditions
       @type SimulationConditions: string        
    """
    query = self.bkQueryTemplate
    query['SimulationConditions']=SimulationConditions
    query['DataTakingConditions']=DataTakingConditions
    query['ProcessingPass']=ProcessingPass
    query['FileType']=FileType
    query['EventType']=EventType
    query['ConfigName']=ConfigName
    query['ConfigVersion']=ConfigVersion
    query['ProductionID']=ProductionID
    query['DataQualityFlag']=DataQualityFlag 
    return self.bkQuery(query)

  #############################################################################
  def bkQuery(self,bkQueryDict):
    """ Developer function. Perform a query to the LHCb Bookkeeping to return 
        a list of LFN(s).
        
        Example Usage:
        
        >>> print dirac.bkQuery(query)
        {'OK':True,'Value':<files>}

       @param bkQueryDict: BK query 
       @type bkQueryDict: dictionary (see bookkeepingQuery() for keys)
       @return: S_OK,S_ERROR            
    """
    problematicFields = []
    for name,value in bkQueryDict.items():
      if not name in self.bkQueryTemplate.keys():
        problematicFields.append(name)

    if problematicFields:
      msg = 'The following fields are not valid for a BK query: %s\nValid fields include: %s' %(string.join(problematicFields,', '),string.join(self.bkQueryTemplate.keys(),', '))
      return S_ERROR(msg)

    for name,value in bkQueryDict.items():
      if name == "ProductionID" or name == "EventType":
        if value == 0:
          del bkQueryDict[name]
        else:
          bkQueryDict[name] = str(value)
      elif name=="FileType":
        if value.lower()=="all":
          bkQueryDict[name]='ALL'
      else:
        if value.lower() == "all":
          del bkQueryDict[name]
    
    if not bkQueryDict.has_key('SimulationConditions') and not bkQueryDict.has_key('DataTakingConditions'):
      return S_ERROR('A Simulation or DataTaking Condition must be specified for a BK query.')
    
    if not bkQueryDict.has_key('EventType') or not bkQueryDict.has_key('ConfigName') or not bkQueryDict.has_key('ConfigVersion'):
      return S_ERROR('The minimal set of BK fields for a query is: EventType, ConfigName and ConfigVersion in addition to a Simulation or DataTaking Condition')
    
    self.log.info('Final BK query dictionary is:')
    for n,v in bkQueryDict.items():
      self.log.info('%s : %s' %(n,v))

    start = time.time()
    bk = BookkeepingClient()                      
    result = bk.getFilesWithGivenDataSets(bkQueryDict)
    rtime = time.time()-start    
    self.log.info('BK query time: %.2f sec' %rtime)
    
    if not result['OK']:
      return S_ERROR('BK query returned an error: %s' %(result['Message']))
    
    if not result['Value']:
      return self.__errorReport('No BK files selected')
    
    returnedFiles = len(result['Value'])
    self.log.info('%s files selected from the BK' %(returnedFiles))
    return result

  #############################################################################
  def __errorReport(self,error,message=None):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error
    self.log.warn(error)
    return S_ERROR(message)
