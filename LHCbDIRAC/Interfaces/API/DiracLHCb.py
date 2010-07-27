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
from DIRAC.Core.Utilities.List                      import breakListIntoChunks, sortList, removeEmptyElements
from DIRAC.Core.Utilities.SiteSEMapping             import getSEsForSite
from DIRAC.Interfaces.API.Dirac                     import Dirac
from DIRAC.Interfaces.API.DiracAdmin                import DiracAdmin

from LHCbDIRAC.Core.Utilities.ClientTools                 import mergeRootFiles,getRootFileGUID
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.BookkeepingSystem.Client.AncestorFiles     import getAncestorFiles

import os,glob,fnmatch,string,time,re

COMPONENT_NAME='DiracLHCb'

class DiracLHCb(Dirac):

  #############################################################################
  def __init__(self, WithRepo=False, RepoLocation=''):
    """Internal initialization of the DIRAC API.
    """
    Dirac.__init__(self,WithRepo=WithRepo, RepoLocation=RepoLocation)
    self.tier1s=gConfig.getValue('/Operations/Defaults/Tier1s',['LCG.CERN.ch','LCG.CNAF.it','LCG.NIKHEF.nl','LCG.PIC.es','LCG.RAL.uk','LCG.GRIDKA.de','LCG.IN2P3.fr','LCG.SARA.nl'])    
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
                             'StartRun'                 :     0,
                             'EndRun'                   :     0,
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
  def bkQueryRunsByDate(self,bkPath,startDate,endDate,dqFlag='All',selection='Runs'):
    """ This function allows to create and perform a BK query given a supplied
        BK path. The following BK path convention is expected:
        
        /<ConfigurationName>/<Configuration Version>/<Processing Pass>/<Event Type>/<File Type>

        so an example for 2010 collisions data would be:
        
        /LHCb/Collision09/Real Data + RecoToDST-07/90000000/DST

        The startDate and endDate must be specified as yyyy-mm-dd.
        
        Runs can be selected based on their status e.g. the selection parameter
        has the following possible attributes:
         - Runs - data for all runs in the range are queried (default)
         - ProcessedRuns - data is retrieved for runs that are processed
         - NotProcessed - data is retrieved for runs that are not yet processed. 

       Example Usage:
       
       >>> dirac.bkQueryRunsByDate('/LHCb/Collision10/Real Data/90000000/RAW','2010-05-18','2010-05-20',dqFlag='EXPRESS_OK',selection='ProcessedRuns')
       {'OK': True, 'Value': [<LFN1>,<LFN2>]}
       
       @param bkPath: BK path as described above
       @type bkPath: string        
       @param dqFlag: Optional Data Quality flag 
       @type dqFlag: string 
       @param startDate: Start date  yyyy-mm-dd
       @param startDate: string
       @param endDate: End date  yyyy-mm-dd
       @param endDate: string
       @param selection: Either Runs, ProcessedRuns or NotProcessed 
       @param selection: string     
       @return: S_OK,S_ERROR                  
    """
    runSelection = ['Runs','ProcessedRuns','NotProcessed']
    if not selection in runSelection:
      return S_ERROR('Expected one of %s not "%s" for selection' %(string.join(runSelection,', '),selection))
    
    if not type(bkPath)==type(' '):
      return S_ERROR('Expected string for bkPath')
    
    #remove any double slashes, spaces must be preserved 
    #remove any empty components from leading and trailing slashes
    bkPath = removeEmptyElements(string.split(string.replace(bkPath,'//','/'),'/'))
    if not len(bkPath)==5:
      return S_ERROR('Expected 5 components to the BK path: /<ConfigurationName>/<Configuration Version>/<Processing Pass>/<Event Type>/<File Type>')
    
    if not startDate or not endDate:
      return S_ERROR('Expected both start and end dates to be defined in format: yyyy-mm-dd')
    
    if not type(startDate)==type(' ') or not type(endDate)==type(' '):
      return S_ERROR('Expected yyyy-mm-dd string for start and end dates')
    
    if not len(startDate.split('-'))==3 or not len(endDate.split('-'))==3:
      return S_ERROR('Expected yyyy-mm-dd string for start and end dates')

    start = time.time()
    bk = BookkeepingClient()                      
    result = bk.getRunsWithAGivenDates({'StartDate':startDate,'EndDate':endDate})
    rtime = time.time()-start    
    self.log.info('BK query time: %.2f sec' %rtime)
    if not result['OK']:
      self.log.info('Could not get runs with given dates from BK with result: "%s"' %result)
      return result
    
    if not result['Value']:
      self.log.info('No runs selected from BK for specified dates')
      return result
    
    if not result['Value'].has_key(selection):
      return S_ERROR('No %s runs for specified dates' %(selection))
    
    runs = result['Value'][selection]    
    self.log.info('Found the following %s runs:\n%s' %(len(runs),string.join([str(i) for i in runs],', ')))
    #temporary until we can query for a discrete list of runs
    
    selectedData = []
    for run in runs:    
      query = self.bkQueryTemplate.copy()
      query['StartRun']=run
      query['EndRun']=run
      query['ConfigName']=bkPath[0]
      query['ConfigVersion']=bkPath[1]    
      query['ProcessingPass']=bkPath[2]
      query['EventType']=bkPath[3]
      query['FileType']=bkPath[4]
      if dqFlag:
        check = self.__checkDQFlags(dqFlag)
        if not check['OK']:
          return check
        dqFlag = check['Value']
        query['DataQualityFlag']=dqFlag      
      result = self.bkQuery(query)
      self.log.verbose(result)
      if not result['OK']:
        return result
      self.log.info('Selected %s files for run %s' %(len(result['Value']),run))
      selectedData+=result['Value']
    
    self.log.info('Total files selected = %s' %(len(selectedData)))
    return S_OK(selectedData)

  #############################################################################
  def bkQueryRun(self,bkPath,dqFlag='All'):
    """ This function allows to create and perform a BK query given a supplied
        BK path. The following BK path convention is expected:
        
        /<Run Number>/<Processing Pass>/<Event Type>/<File Type>
        
        so an example for 2009 collisions data would be:

       /63566/Real Data + RecoToDST-07/90000000/DST
       
       In addition users can specify a range of runs using the following convention:
       
       /<Run Number 1> - <Run Number 2>/<Processing Pass>/<Event Type>/<File Type>

       so extending the above example this would look like:

       /63566-63600/Real Data + RecoToDST-07/90000000/DST

       Example Usage:
       
       >>> dirac.bkQueryRun('/63566/Real Data + RecoToDST-07/90000000/DST')
       {'OK':True,'Value': ['/lhcb/data/2009/DST/00005842/0000/00005842_00000008_1.dst']}
       
       @param bkPath: BK path as described above
       @type bkPath: string        
       @param dqFlag: Optional Data Quality flag 
       @type dqFlag: string 
       @return: S_OK,S_ERROR        
    """
    if not type(bkPath)==type(' '):
      return S_ERROR('Expected string for bkPath')
    
    #remove any double slashes, spaces must be preserved 
    #remove any empty components from leading and trailing slashes
    bkPath = removeEmptyElements(string.split(string.replace(bkPath,'//','/'),'/'))
    if not len(bkPath)==4:
      return S_ERROR('Expected 4 components to the BK path: /<Run Number>/<Processing Pass>/<Event Type>/<File Type>')
    
    runNumberString = bkPath[0].replace('--','-').replace(' ','')
    startRun = 0
    endRun = 0
    if re.search('-',runNumberString):
      if not len(runNumberString.split('-'))==2:
        return S_ERROR('Could not determine run range from "%s", try "<Run 1> - <Run2>"' %(runNumberString))      
      start = int(runNumberString.split('-')[0])
      end = int(runNumberString.split('-')[1])
      if int(start)<int(end):
        startRun=start
        endRun = end
      else:
        startRun=end
        endRun = start
    else:
      startRun = int(runNumberString)
      endRun = int(runNumberString)
    
    query = self.bkQueryTemplate.copy()
    query['StartRun']=startRun
    query['EndRun']=endRun
    query['ProcessingPass']=bkPath[1]
    query['EventType']=bkPath[2]
    query['FileType']=bkPath[3]

    if dqFlag:
      check = self.__checkDQFlags(dqFlag)
      if not check['OK']:
        return check
      dqFlag = check['Value']
      query['DataQualityFlag']=dqFlag

    result = self.bkQuery(query)
    self.log.verbose(result)
    return result
  
  #############################################################################
  def bkQueryProduction(self,bkPath,dqFlag='All'):
    """ This function allows to create and perform a BK query given a supplied
        BK path. The following BK path convention is expected:
        
        /<ProductionID>/<Processing Pass>/<Event Type>/<File Type>        
        
        so an example for 2009 collisions data would be:
        
       /5842/Real Data + RecoToDST-07/90000000/DST
       
       a data quality flag can also optionally be provided, the full list of these is available 
       via the getAllDQFlags() method.
       
       Example Usage:
       
       >>> dirac.bkQueryProduction('/5842/Real Data + RecoToDST-07/90000000/DST')
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
    #remove any empty components from leading and trailing slashes
    bkPath = removeEmptyElements(string.split(string.replace(bkPath,'//','/'),'/'))
    if not len(bkPath)==4:
      return S_ERROR('Expected 4 components to the BK path: /<ProductionID>/<Processing Pass>/<Event Type>/<File Type>')
    
    query = self.bkQueryTemplate.copy()
    query['ProductionID']=int(bkPath[0])
    query['ProcessingPass']=bkPath[1]
    query['EventType']=bkPath[2]
    query['FileType']=bkPath[3]

    if dqFlag:
      check = self.__checkDQFlags(dqFlag)
      if not check['OK']:
        return check
      dqFlag = check['Value']
      query['DataQualityFlag']=dqFlag

    result = self.bkQuery(query)
    self.log.verbose(result)
    return result    

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
    #remove any empty components from leading and trailing slashes
    bkPath = removeEmptyElements(string.split(string.replace(bkPath,'//','/'),'/'))
    if not len(bkPath)==6:
      return S_ERROR('Expected 6 components to the BK path: /<ConfigurationName>/<Configuration Version>/<Sim or Data Taking Condition>/<Processing Pass>/<Event Type>/<File Type>')
    
    query = self.bkQueryTemplate.copy()
    query['ConfigName']=bkPath[0]
    query['ConfigVersion']=bkPath[1]    
    query['ProcessingPass']=bkPath[3]
    query['EventType']=bkPath[4]
    query['FileType']=bkPath[5]

    if dqFlag:
      check = self.__checkDQFlags(dqFlag)
      if not check['OK']:
        return check
      dqFlag = check['Value']
      query['DataQualityFlag']=dqFlag

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
        a list of LFN(s). This method takes a BK query dictionary.
        
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
      if name == "ProductionID" or name == "EventType" or name=="StartRun" or name=="EndRun":
        if value == 0:
          del bkQueryDict[name]
        else:
          bkQueryDict[name] = str(value)            
      elif name=="FileType":
        if value.lower()=="all":
          bkQueryDict[name]='ALL'
      else:
        if str(value).lower() == "all":
          del bkQueryDict[name]
    
    if bkQueryDict.has_key('ProductionID') or bkQueryDict.has_key('StartRun') or bkQueryDict.has_key('EndRun'):
      self.log.verbose('Found a specific query so loosening some restrictions to prevent BK overloading')
    else:
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
    self.log.verbose('%s files selected from the BK' %(returnedFiles))
    return result

  #############################################################################
  def __checkDQFlags(self,flags):
    """ Internal function.  Checks the provided flags against the list of 
        possible DQ flag statuses from the Bookkeeping.        
    """
    dqFlags = []
    if type(flags)==type([]):
      dqFlags = flags
    else:
      dqFlags = [flags]

    bkFlags = self.getAllDQFlags()
    if not bkFlags['OK']:
      return bkFlags

    final = []
    for flag in dqFlags:     
      if flag.lower()=='all':
        final.append(flag.upper())
      else:
        flag = flag.upper()
        if not flag in bkFlags['Value']:
          msg = 'Specified DQ flag "%s" is not in allowed list: %s' %(flag,string.join(bkFlags['Value'],', '))
          self.log.error(msg)
          return S_ERROR(msg)
        else:
          final.append(flag) 

    #when first coding this it was not possible to use a list ;)    
    if len(final)==1:
      final=final[0]
    
    return S_OK(final)

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
  def getDataByRun( self, lfns, printOutput = False ):
    """Sort the supplied lfn list by run. An S_OK object will be returned 
       containing a dictionary of runs and the corresponding list of LFN(s)
       associated with them.  

       Example usage:

       >>> print dirac.getDataByRun(lfns)
       {'OK': True, 'Value': {<RUN>:['<LFN>','<LFN>',...], <RUN>:['<LFN>',..]}}


       @param lfns: Logical File Name(s) 
       @type lfns: list
       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """
    if type( lfns ) == type( " " ):
      lfns = [lfns.replace( 'LFN:', '' )]
    elif type( lfns ) == type( [] ):
      try:
        lfns = [str( lfn.replace( 'LFN:', '' ) ) for lfn in lfns]
      except Exception, x:
        return self.__errorReport( str( x ), 'Expected strings for LFNs' )
    else:
      return self.__errorReport( 'Expected single string or list of strings for LFN(s)' )

    runDict = {}
    bk = BookkeepingClient()
    start = time.time()
    result = bk.getFileMetadata(lfns)
    self.log.verbose("Obtained BK file metadata in %.2f seconds" % (time.time()-start))
    if not result['OK']: 
      self.log.error('Failed to get bookkeeping metadata with result "%s"' %(result['Message']))
      return result
    
    for lfn,metadata in result['Value'].items():
      if metadata.has_key('RunNumber'):
        runNumber = metadata['RunNumber']        
        if not runDict.has_key(runNumber):
          runDict[runNumber] = [lfn]
        else:
          runDict[runNumber].append(lfn)
      else:
        self.log.warn('Could not find run number from BK for %s' %(lfn))

    if printOutput:
      print self.pPrint.pformat( runDict )
      
    return S_OK( runDict )

  #############################################################################
  def bkMetadata( self, lfns, printOutput = False ):
    """Return metadata for the supplied lfn list. An S_OK object will be returned 
       containing a dictionary of LFN(s) and the corresponding metadata associated 
       with them.  

       Example usage:

       >>> print dirac.bkMetadata(lfns)
       {'OK': True, 'Value': {<LFN>:{'<Name>':'<Value>',...},...}}

       @param lfns: Logical File Name(s) 
       @type lfns: list
       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """
    if type( lfns ) == type( " " ):
      lfns = [lfns.replace( 'LFN:', '' )]
    elif type( lfns ) == type( [] ):
      try:
        lfns = [str( lfn.replace( 'LFN:', '' ) ) for lfn in lfns]
      except Exception, x:
        return self.__errorReport( str( x ), 'Expected strings for LFNs' )
    else:
      return self.__errorReport( 'Expected single string or list of strings for LFN(s)' )
        
    bk = BookkeepingClient()
    start = time.time()
    result = bk.getFileMetadata(lfns)
    self.log.verbose("Obtained BK file metadata in %.2f seconds" % (time.time()-start))
    if not result['OK']: 
      self.log.error('Failed to get bookkeeping metadata with result "%s"' %(result['Message']))
      return result

    if printOutput:
      print self.pPrint.pformat( result['Value'] )

    return result

  #############################################################################
  def gridWeather( self, printOutput = False ):
    """This method gives a snapshot of the current Grid weather from the perspective
       of the DIRAC site and SE masks.  Tier-1 sites are returned with more detailed
       information.
       
       Example usage:

       >>> print dirac.gridWeather()
       {'OK': True, 'Value': {{'Sites':<siteInfo>,'SEs':<seInfo>,'Tier-1s':<tierInfo>}}

       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR       
    """    
    siteInfo = self.checkSites()
    if not siteInfo['OK']:
      return siteInfo
    siteInfo = siteInfo['Value']

    seInfo = self.checkSEs()
    if not seInfo['OK']:
      return seInfo
    seInfo = seInfo['Value']
    
    tierSEs = {}
    for site in self.tier1s:      
      tierSEs[site]=getSEsForSite(site)['Value']

    tierInfo = {}
    for site,seList in tierSEs.items():
      tierInfo[site]={}
      for se in seList:
        if seInfo.has_key(se):
          tierSEInfo = seInfo[se]
          tierInfo[site][se]=tierSEInfo
      if site in siteInfo['AllowedSites']:
        tierInfo[site]['MaskStatus']='Allowed'
      else:
        tierInfo[site]['MaskStatus']='Banned'
        
    if printOutput:
      print '========> Tier-1 status in DIRAC site and SE masks'
      for site in sortList(self.tier1s):
        print '\n====> %s\n' %site
        print '%s %s %s' %('Storage Element'.ljust(25),'Read Status'.rjust(15),'Write Status'.rjust(15))
        for se in sortList(tierSEs[site]):
          if tierInfo[site].has_key(se):
            print '%s %s %s' %(se.ljust(25),tierInfo[site][se]['ReadStatus'].rjust(15),tierInfo[site][se]['WriteStatus'].rjust(15))
      
      print '\n========> Tier-2 status in DIRAC site mask\n'
      allowedSites = siteInfo['AllowedSites']
      bannedSites = siteInfo['BannedSites']
      for site in self.tier1s:
        if site in allowedSites:
          allowedSites.remove(site)
        if site in bannedSites:
          bannedSites.remove(site)
      print ' %s sites are in the site mask, %s are banned.\n' %(len(allowedSites),len(bannedSites))
          
    summary = {'Sites':siteInfo,'SEs':seInfo,'Tier-1s':tierInfo}        
    return S_OK(summary)
  
  #############################################################################
  def checkSites( self, gridType='LCG', printOutput = False ):
    """Return the list of sites in the DIRAC site mask and those which are banned.

       Example usage:

       >>> print dirac.checkSites()
       {'OK': True, 'Value': {'AllowedSites':['<Site>',...],'BannedSites':[]}

       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """
    siteMask = DiracAdmin().getSiteMask()
    if not siteMask['OK']:
      return siteMask
    
    totalList = gConfig.getSections('/Resources/Sites/%s' %gridType)
    if not totalList['OK']:
      return S_ERROR('Could not get list of sites from CS')
    totalList = totalList['Value']
    sites = siteMask['Value']
    bannedSites=[]
    for site in totalList:
      if not site in sites:
        bannedSites.append(site)

    if printOutput:
      print '\n========> Allowed Sites\n'
      print string.join(sites,'\n')
      print '\n========> Banned Sites\n'
      print string.join(bannedSites,'\n')
      print '\nThere is a total of %s allowed sites and %s banned sites in the system.' %(len(sites),len(bannedSites))
        
    return S_OK({'AllowedSites':sites,'BannedSites':bannedSites})  

  #############################################################################
  def checkSEs( self, printOutput = False ):
    """Check the status of read and write operations in the DIRAC SE mask. 

       Example usage:

       >>> print dirac.checkSEs()
       {'OK': True, 'Value': {<LFN>:{'<Name>':'<Value>',...},...}}

       @param printOutput: Optional flag to print result
       @type printOutput: boolean
       @return: S_OK,S_ERROR
    """
    storageCFGBase = '/Resources/StorageElements'
    res = gConfig.getSections(storageCFGBase,True)
    if not res['OK']:
      return S_ERROR('Failed to get storage element information')

    if printOutput:
      print '%s %s %s' % ('Storage Element'.ljust(25),'Read Status'.rjust(15),'Write Status'.rjust(15))

    result = {}
    for se in sortList(res['Value']):
      res = gConfig.getOptionsDict('%s/%s' % (storageCFGBase,se))
      if not res['OK']:
        gLogger.warn('Failed to get options dict for SE %s' % se)
      else:
        readState = 'Active'
        if res['Value'].has_key('ReadAccess'):
          readState = res['Value']['ReadAccess']
        writeState = 'Active'
        if res['Value'].has_key('WriteAccess'):
          writeState = res['Value']['WriteAccess']
        result[se] = {'ReadStatus':readState,'WriteStatus':writeState}
        if printOutput: 
          print '%s %s %s' %(se.ljust(25),readState.rjust(15),writeState.rjust(15))
           
    return S_OK(result)

  #############################################################################
  def __errorReport(self,error,message=None):
    """Internal function to return errors and exit with an S_ERROR()
    """
    if not message:
      message = error
    self.log.warn(error)
    return S_ERROR(message)
