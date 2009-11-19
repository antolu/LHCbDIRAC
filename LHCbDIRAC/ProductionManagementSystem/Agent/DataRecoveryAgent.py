########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/ProductionManagementSystem/Agent/DataRecovery.py $
########################################################################

""" In general for data processing producitons we need to completely abandon the 'by hand'
    reschedule operation such that accidental reschedulings don't result in data being processed twice.
    
    For all above cases the following procedure should be used to achieve 100%:
    
    - Starting from the data in the Production DB for each transformation
      look for files in the following status:
         Assigned   
         MaxReset     
    
    For files in MaxReset and Assigned:
    - Discover corresponding job WMS ID
    - Don't do anything for 12 or 24hrs after the last update of the job
    - Check that there are no outstanding requests for the job 
      o wait until all are treated before proceeding
    - Check that none of the job input data has BK descendants
      o remove if present (also set ancestors to unused)
    - Check that no outputs exist in the LFC
      o remove if any present
    - Mark the input file status as 'Unused' in the ProductionDB

    N.B. initially this agent will stop if it finds any descendent files for input
    data of a given production. After an optimization of the transformationDB it
    will become simpler to remove descendent files to any depth for a given production
    that may or may not affect other transformations. 
"""

__RCSID__   = "$Id: DataRecovery.py 18182 2009-11-11 14:45:10Z paterson $"
__VERSION__ = "$Revision: 1.9 $"

from DIRAC                                                     import S_OK, S_ERROR, gConfig, gLogger, rootPath
from DIRAC.Core.Base.AgentModule                               import AgentModule
from DIRAC.DataManagementSystem.Client.ReplicaManager          import ReplicaManager
from DIRAC.ProductionManagementSystem.DB.ProductionDB          import ProductionDB
from DIRAC.RequestManagementSystem.Client.RequestClient        import RequestClient
from DIRAC.Core.Utilities.List                                 import uniqueElements
from DIRAC.Core.Utilities.Time                                 import timeInterval,dateTime
from DIRAC.Core.Utilities.Shifter                              import setupShifterProxyInEnv

try:
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient      import BookkeepingClient
except Exception,x:  
  from DIRAC.BookkeepingSystem.Client.BookkeepingClient          import BookkeepingClient

import string,re,datetime

AGENT_NAME = 'ProductionManagement/DataRecoveryAgent'

class DataRecoveryAgent(AgentModule):

  #############################################################################
  def initialize(self):
    """Sets defaults
    """
#    service = self.am_getOption('TransformationService','')
#    self.transDB = TransformationDB('ProductionDB', 'ProductionManagement/ProductionDB')
    self.replicaManager = ReplicaManager()
    self.prodDB = ProductionDB()
    self.bkClient = BookkeepingClient()
    self.requestClient = RequestClient()

    self.am_setOption('PollingTime',4*60*60)    
    self.am_setModuleParam("shifterProxy", "ProductionManager")
    self.am_setModuleParam("shifterProxyLocation","%s/runit/%s/proxy" % (rootPath,AGENT_NAME))
    return S_OK()

  #############################################################################
  def execute(self):
    """ The main execution method.
    """  
    #Configuration settings
    enableFlag = self.am_getOption('EnableFlag','True')
#    enableFlag = self.am_getOption('EnableFlag','False')    
    transformationTypes = self.am_getOption('TransformationTypes',['DataReconstruction','DataStripping','MCStripping','Merge'])
    transformationStatus = self.am_getOption('TransformationStatus','Active')
    fileSelectionStatus = self.am_getOption('FileSelectionStatus',['Assigned','MaxReset'])
    updateStatus = self.am_getOption('FileUpdateStatus','Unused')
    wmsStatusList = self.am_getOption('WMSStatus',['Failed','Stalled'])
    #only worry about files > 12hrs since last update    
    selectDelay = self.am_getOption('SelectionDelay',12) #hours 
    bkDepth = self.am_getOption('BKDepth',2)

    result = self.getEligibleTransformations(transformationStatus,transformationTypes)
    if not result['OK']:
      self.log.error(result)
      return S_ERROR('Could not obtain eligible transformations')
    
    if not result['Value']:
      self.log.info('No %s transformations of types %s to process.' %(transformationStatus,string.join(transformationTypes,', ')))
      return S_OK('Nothing to do')

    transformationDict = result['Value']
    
    self.log.info('The following transformations were selected out of %s:\n%s' %(string.join(transformationTypes,', '),string.join(transformationDict.keys(),', ')))
    for transformation,typeName in transformationDict.items():
#      if not transformation in ('5589','5600'):
#      if not transformation=='5593':
#        print 'skipping %s' %transformation
#        continue
      self.log.info('='*len('Looking at transformation %s type %s:' %(transformation,typeName)))
      self.log.info('Looking at transformation %s:' %(transformation))

      result = self.selectTransformationFiles(transformation,fileSelectionStatus)
      if not result['OK']:
        self.log.error(result)
        self.log.error('Could not select files for transformation %s' %transformation)
        continue
  
      if not result['Value']:
        self.log.info('No files in status %s selected for transformation %s' %(string.join(fileSelectionStatus,', '),transformation))
        continue
    
      fileDict = result['Value']      
      result = self.obtainWMSJobIDs(transformation,fileDict,selectDelay,wmsStatusList)
      if not result['OK']:
        self.log.error(result)
        self.log.error('Could not obtain WMS jobIDs for files of transformation %s' %(transformation))
        continue
      
      if not result['Value']:
        self.log.info('No eligible WMS jobIDs found for %s files in list:\n%s ...' %(len(fileDict.keys()),fileDict.keys()[0]))
        continue
    
      jobFileDict = result['Value']
      fileCount = 0
      for job,lfnList in jobFileDict.items():
        fileCount+=len(lfnList)
      self.log.info('%s files are selected after examining related WMS jobs' %(fileCount))        
      result = self.checkOutstandingRequests(jobFileDict)
      if not result['OK']:
        self.log.error(result)
        continue

      if not result['Value']:
        self.log.info('No WMS jobs without pending requests to process.')
        continue
      
      jobFileNoRequestsDict = result['Value']
      fileCount = 0
      for job,lfnList in jobFileNoRequestsDict.items():
        fileCount+=len(lfnList)
      
      self.log.info('%s files are selected after removing any relating to jobs with pending requests' %(fileCount))
      result = self.checkDescendents(transformation,jobFileNoRequestsDict,bkDepth)
      if not result['OK']:
        self.log.error(result)
        continue
            
      problematicFiles = result['Value']['toremove']
      strandedAncestors = result['Value']['strandedancestors']
      filesToUpdate = []
      for job,fileList in jobFileNoRequestsDict.items():
        filesToUpdate+=fileList
      
      if strandedAncestors:
        filesToUpdate+=strandedAncestors
        self.log.info('The following ancestor files will also be marked as "%s" after successful removal as they were stranded:\n%s' %(updateStatus,string.join(strandedAncestors,'\n')))
            
      if problematicFiles:
        self.log.info('The following problematic files are to be removed for %s\n%s' %(transformation,string.join(problematicFiles,'\n')))
        ######### N.B. ##########
        self.log.info('*N.B. removal of files is disabled since all subsequent transformations using any descendents as input also need to be updated!*')
        continue
        ######### N.B. ##########
      else:
        self.log.info('No problematic files were found to be removed for transformation %s, %s files will be updated to "%s"' %(transformation,len(filesToUpdate),updateStatus))

      if not enableFlag == 'True':
        self.log.info('%s is disabled by configuration option EnableFlag' %(AGENT_NAME))
        self.log.info('When disabled, %s performs no "one-way" operations such as file removal or ProductionDB status updates.' %(AGENT_NAME))
        continue  

      if problematicFiles:
        result = self.removeOutputs(problematicFiles)
        if not result['OK']:
          self.log.error('Could not remove all problematic files with result\n%s' %(result))
          continue
      else:
        self.log.info('No problematic files to be removed...')
        
      result = self.updateFileStatus(transformation,filesToUpdate,updateStatus)
      if not result['OK']:
        self.log.error('Files were not updated with result:\n%s' %(result))
        continue        
      
      self.log.info('%s files were recovered for transformation %s' %(len(filesToUpdate),transformation))

    return S_OK()

  #############################################################################
  def getEligibleTransformations(self,status,typeList):
    """ Select transformations of given status and type.
    """
    result = self.prodDB.getProductionList()
    if not result['OK']:
      return result
    
    transformations={}
    prods = result['Value']
    for prod in prods:
      if prod['Status']==status and prod['Type'] in typeList:
        typeName = prod['Type']
        prodID = prod['TransformationID']
        transformations[str(prodID)]=typeName
        
    return S_OK(transformations)
  
  #############################################################################
  def selectTransformationFiles(self,transformation,statusList):
    """ Select files, production jobIDs in specified file status for a given transformation.
    """
    #Until a query for files with timestamp can be obtained must rely on the
    #WMS job last update
    fileDict = {}    
    for status in statusList:
      self.log.info('Looking for files in status %s for transformation %s' %(status,transformation))
      result =  self.prodDB.getTransformationLFNsJobs(long(transformation),status)
      if not result['OK']:
        return result
      self.log.info('Selected %s files for status %s' %(len(result['Value']),status))
      fileDict.update(result['Value'])
    
    if fileDict:
      self.log.info('Selected %s files overall for transformation %s' %(len(fileDict.keys()),transformation))
          
    return S_OK(fileDict)
  
  #############################################################################
  def obtainWMSJobIDs(self,transformation,fileDict,selectDelay,wmsStatusList):
    """ Group files by the corresponding WMS jobIDs, check the corresponding
        jobs have not been updated for the delay time.  Can't get into any 
        mess because we start from files only in MaxReset / Assigned and check
        corresponding jobs.  Mixtures of files for jobs in MaxReset and Assigned 
        statuses only possibly include some files in Unused status (not Processed 
        for example) that will not be touched.
    """
    prodJobIDs = uniqueElements(fileDict.values())
    self.log.info('The following %s production jobIDs apply to the selected files:\n%s' %(len(prodJobIDs),prodJobIDs))

    jobFileDict = {}
    for job in prodJobIDs:
      result = self.prodDB.getJobInfo(int(transformation),int(job))
      if not result['OK']:
        self.log.error('Could not get job info for %s_%s, ignoring from further consideration:\n%s' %(transformation.zfill(8),job.zfill(8),result))
        continue
      
      params = result['Value']
      if not params.has_key('InputVector'):
        self.log.error('Could not find input vector for %s_%s, ignoring from further consideration' %(transformation.zfill(8),job.zfill(8)))
        continue
      
      wmsID = params['JobWmsID']
      lastUpdate = params['LastUpdateTime']
      wmsStatus = params['WmsStatus']
      jobInputData = params['InputVector']
      jobInputData = [lfn.replace('LFN:','') for lfn in jobInputData.split(';')]
            
      if not wmsStatus in wmsStatusList:
        self.log.info('Job %s is in status %s, expected one of %s, ignoring from further consideration' %(wmsID,wmsStatus,string.join(wmsStatusList,', ')))
        continue
      
      #Exclude jobs not having an old enough last update time or appropriate WMS status now      
      delta = datetime.timedelta( hours = selectDelay )
      interval = timeInterval(lastUpdate,delta)
      now = dateTime()
      if interval.includes(now):
        self.log.info('Job %s was last updated less than %s hours ago, ignoring %s files from further consideration' %(wmsID,selectDelay,len(jobInputData)))
        continue
      
      finalJobData = []
      #Must map unique files -> jobs in expected state
      for lfn,prodID in fileDict.items():
        if prodID==job:
          finalJobData.append(lfn)
          
      jobFileDict[wmsID]=finalJobData
 
    return S_OK(jobFileDict)
  
  #############################################################################
  def checkOutstandingRequests(self,jobFileDict):
    """ Before doing anything check that no outstanding requests are pending
        for the set of WMS jobIDs.
    """
    jobs = jobFileDict.keys()
    result = self.requestClient.getRequestForJobs(jobs)
    if not result['OK']:
      return result
    
    if not result['Value']:
      self.log.info('None of the jobs have pending requests')
      return S_OK(jobFileDict)
    
    for jobID,requestName in result['Value']:
      del jobfileDict[jobID]  
      self.log.info('Removing jobID %s from consideration until requests are completed')
    
    return S_OK(jobFileDict)
  
  #############################################################################
  def checkDescendents(self,transformation,jobFileDict,bkDepth):
    """ Check BK descendents for input files, prepare list of actions to be
        taken for recovery.  Also must be careful to check any descendents do
        not have other ancestor files that could become stranded.
    """
    toRemove=[]
    strandedAncestors=[]
    for job,fileList in jobFileDict.items():
      self.log.info('Checking BK descendents for job %s...' %job)
      #check any input data has descendant files...
      result = self.bkClient.getDescendents(fileList,bkDepth)
      if not result['OK']:
        self.log.error('Could not obtain descendents for job %s with result:\n%s' %(job,result))
        continue
      if result['Value']['Failed']:
        self.log.error('Problem obtaining some descendents for job %s with result:\n%s' %(job,result['Value']))
        continue
      descendents = result['Value']['Successful'].keys()
      for fname in descendents:
        desc = result['Value']['Successful'][fname]
        # IMPORTANT: Descendents of input files can be found for parallel productions,
        # must restrict only to current transformation ID
        for d in desc:
          if re.search('\/%s\/' %(str(transformation.zfill(8))),d):
            toRemove.append(d)
    
    if toRemove:
      self.log.info('Found descendent files of transformation %s to be removed:\n%s' %(transformation,string.join(toRemove,'\n')))
      result = self.prodDB.getTransformationLFNStatus(int(transformation),toRemove)
      if not result['OK']:
        self.log.error(result)
      else:
        self.log.info('%s = %s' %(lfn,result['Value'][lfn]))
            
    #Now to double-check that all ancestors are included in the job file dictionary
    finalToRemove = []
    for i in toRemove:
      self.log.info('Checking ancestors of file to potentially be removed: %s' %(i))
      result = self.bkClient.getAncestors(i,depth=bkDepth)
      if not result['OK']:
        self.log.error('Problem during getAncestors call:\n%s' %(result['Message']))
        continue
      if result['Value']['Failed']:
        self.log.error('No ancestors found for the following file with message:\n%s' %(string.join(result['Value']['Failed'],'\n')))
        continue
      toCheck = result['Value']['Successful'].values()
      finalToRemove.append(i)
      for fname in toCheck:
        if not fname in jobFileDict.values():
          strandedAncestors+=fname
          
    if strandedAncestors:
      self.log.info('Ancestor of file to be removed was not in selected input file sample:\n%s' %(string.join(strandedAncestors,'\n')))
      result = self.prodDB.getTransformationLFNStatus(int(transformation),strandedAncestors)
      if not result['OK']:
        self.log.error(result)
      else:
        self.log.info('%s = %s' %(lfn,result['Value'][lfn]))   
        
    result={'toremove':finalToRemove,'strandedancestors':strandedAncestors}
    return S_OK(result)

  #############################################################################
  def removeOutputs(self,problematicFiles):
    """ Remove outputs from any catalog / storages.
    """
    result = self.replicaManager.removeFile(problematicFiles)
    if not result['OK']:
      self.log.error(result)
      return result
    
    if result['Value']['Failed']:
      self.log.error(result)
      return S_ERROR(result['Value']['Failed'])
        
    self.log.info('The following problematic files were successfully removed:\n%s' %(string.join(problematicFiles,'\n')))
    return S_OK()

  #############################################################################
  def updateFileStatus(self,transformation,fileList,fileStatus):
    """ Update file list to specified status.
    """
    self.log.info('Updating %s files to "%s" status for %s' %(len(fileList),fileStatus,transformation))
    result = self.prodDB.setFileStatusForTransformation(int(transformation),fileStatus,fileList,force=True)
    if not result['OK']:
      self.log.error(result)
      return result
    if result['Value']['Failed']:
      self.log.error(result['Value']['Failed'])
      return result
    
    msg = result['Value']['Successful']
    for lfn,message in msg.items():
      self.log.info('%s => %s' %(lfn,message))
    
    return S_OK()
            
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#    