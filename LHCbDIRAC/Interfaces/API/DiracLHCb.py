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

from LHCbDIRAC.Core.Utilities.ClientTools                 import mergeRootFiles,getRootFileGUID
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.BookkeepingSystem.Client.AncestorFiles     import getAncestorFiles

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
                             'ProductionID'             :     0,
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
  def getBKAncestors(self,lfns,depth):
    """ This function allows to retrieve ancestor files from the Bookkeeping.
    
        Example Usage:
        
        >>> dirac.getBKAncestors('/lhcb/data/2009/DST/00005727/0000/00005727_00000042_1.dst',2)
        {'OK': True, 'Value': ['/lhcb/data/2009/DST/00005727/0000/00005727_00000042_1.dst', '/lhcb/data/2009/RAW/FULL/LHCb/COLLISION09/63807/063807_0000000004.raw']}
        
       @param lfn: Logical File Name (LFN)
       @type lfn: string or list
       @param depth: Ancestor depth
       @type depth: integer      
    """
    return getAncestorFiles(lfns,depth)

  #############################################################################
  def bkQueryPath(self,bkPath,dqFlag='All'):
    """ This function allows to create and perform a BK query given a supplied
        BK path. The following BK path convention is expected:
       
       /<ConfigurationName>/<Configuration Version>/<Sim or Data Taking Condition>/<Processing Pass>/<Event Type>/<File Type>
       
       so an example for 2009 collsions data would be:
       
       /LHCb/Collision09/Beam450GeV-VeloOpen-MagDown/Real Data + RecoToDST-07/90000000/DST
       
       or for MC09 simulated data:
       
       /MC/2010/Beam3500GeV-VeloClosed-MagDown-Nu1/2010-Sim01Reco01-withTruth/27163001/DST
       
       a data quality flag can also optionally be provided, the full list of these is available 
       via the getAllDQFlags() method.
       
       Example Usage:
       
       >>> dirac.bkQueryPath('/MC/2010/Beam3500GeV-VeloClosed-MagDown-Nu1/2010-Sim01Reco01-withTruth/27163001/DST')
       {'OK': True, 'Value': [<LFN1>,<LFN2>]}
       
       @param bkPath: BK path as described above
       @type bkPath: string        
       @param dqFlag: Optional Data Quality flag 
       @type dqFlag: string 
       @return: S_OK,S_ERROR                   
    """
    if not type(bkPath)==type(' '):
      return S_ERROR('Expected string for bkPath')
    
    #remove any double slashes, spaces must be preserved 
    bkPath = string.split(string.replace(bkPath,'//','/'),'/')
    #remove any empty components from leading and trailing slashes
    tmp = []
    for i in bkPath:
      if i:
        tmp.append(i)
    bkPath = tmp
    
    if not len(bkPath)==6:
      return S_ERROR('Expected 6 components to the BK path: /<ConfigurationName>/<Configuration Version>/<Sim or Data Taking Condition>/<Processing Pass>/<Event Type>/<File Type>')
    
    query = self.bkQueryTemplate.copy()
    query['ConfigName']=bkPath[0]
    query['ConfigVersion']=bkPath[1]    
    query['ProcessingPass']=bkPath[3]
    query['EventType']=bkPath[4]
    query['FileType']=bkPath[5]

    if dqFlag:
      if dqFlag.lower()=='all':
        query['DataQualityFlag']=dqFlag
      else:
        dqFlag = dqFlag.upper()
        flags = self.getAllDQFlags()
        if not flags['OK']:
          return flags
        if not dqFlag in flags['Value']:
          msg = 'Specified DQ flag "%s" is not in allowed list: %s' %(dqFlag,string.join(flags['Value'],', '))
          self.log.error(msg)
          return S_ERROR(msg) 
          
    #The problem here is that we don't know if it's a sim or data taking condition, assume that if configName=MC this is simulation
    if bkPath[0].lower()=='mc':
      query['SimulationConditions']=bkPath[2]
    else:
      query['DataTakingConditions']=bkPath[2]

    result = self.bkQuery(query)
    self.log.verbose(result)
    return result
    
  #############################################################################
  def bookkeepingQuery(self,SimulationConditions='All',DataTakingConditions='All',ProcessingPass='All',FileType='All',EventType='All',ConfigName='All',ConfigVersion='All',ProductionID=0,DataQualityFlag='ALL'):
    """ This function will create and perform a BK query using the supplied arguments 
        and return a list of LFNs.
        
        Example Usage:
        
        >>> dirac.bookkeepingQuery(ConfigName='LHCb',ConfigVersion='Collision09',EventType='90000000',ProcessingPass='Real Data',DataTakingConditions='Beam450GeV-VeloOpen-MagDown')
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
       @return: S_OK,S_ERROR                          
    """
    query = self.bkQueryTemplate.copy()
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
    
    self.log.verbose('Final BK query dictionary is:')
    for n,v in bkQueryDict.items():
      self.log.verbose('%s : %s' %(n,v))

    start = time.time()
    bk = BookkeepingClient()                      
    result = bk.getFilesWithGivenDataSets(bkQueryDict)
    rtime = time.time()-start    
    self.log.info('BK query time: %.2f sec' %rtime)
    
    if not result['OK']:
      return S_ERROR('BK query returned an error: "%s"' %(result['Message']))
    
    if not result['Value']:
      return self.__errorReport('No BK files selected')
    
    returnedFiles = len(result['Value'])
    self.log.info('%s files selected from the BK' %(returnedFiles))
    return result

  #############################################################################
  def getAllDQFlags(self,printOutput=False):
    """ Helper function.  Returns the list of possible DQ flag statuses
        from the Bookkeeping.
        
        Example Usage:
        
        >>> print dirac.getAllDQFlags()
        {'OK':True,'Value':<flags>}

       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR   
    """
    bk = BookkeepingClient()                          
    result = bk.getAvailableDataQuality()
    if not result['OK']:
      self.log.error('Could not obtain possible DQ flags from BK with result:\n%s' %(result))
      return result
    
    if printOutput:
      flags = result['Value']
      self.log.info('Possible DQ flags from BK are: %s' %(string.join(flags,', ')))
    
    return result

  #############################################################################
  def __errorReport(self,error,message=None):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error
    self.log.warn(error)
    return S_ERROR(message)
