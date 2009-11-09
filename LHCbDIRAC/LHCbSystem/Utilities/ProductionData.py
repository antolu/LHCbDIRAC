########################################################################
# $Id$
########################################################################
""" Utility to construct production LFNs from workflow parameters
    according to LHCb conventions.
    
    TODO: this module inherits the WorkflowLib Tools behaviour (imported
    as private methods). This can be cleaned up. 
"""

__RCSID__ = "$Id$"

import string,re

#This utility can eventually contain all of the LHCb conventions regarding input data.
from DIRAC import S_OK, S_ERROR, gLogger, gConfig

#############################################################################
def constructProductionLFNs(paramDict):
  """ Used for local testing of a workflow, a temporary measure until
      LFN construction is tidied.  This works using the workflow commons for
      on the fly construction.
  """
  keys = ['PRODUCTION_ID','JOB_ID','dataType','configVersion','JobType','outputList']
  for k in keys:
    if not paramDict.has_key(k):
      return S_ERROR('%s not defined' %k)

  productionID = paramDict['PRODUCTION_ID']
  jobID = paramDict['JOB_ID']
  wfMode = paramDict['dataType']
  wfConfigVersion=paramDict['configVersion']
  wfMask = paramDict['outputDataFileMask']
  if not type(wfMask)==type([]):
    wfMask = [i.lower().strip() for i in wfMask.split(';')]
  wfType=paramDict['JobType']
  outputList = paramDict['outputList']
  inputData=''
  if paramDict.has_key('InputData'):
    inputData=paramDict['InputData']

  fileTupleList = []
  gLogger.verbose('WFMode = %s, WFConfigVersion = %s, WFMask = %s, WFType=%s' %(wfMode,wfConfigVersion,wfMask,wfType))
  for info in outputList:
    #Nasty check on whether the created code parameters were not updated e.g. when changing defaults in a workflow
    fileName = info['outputDataName'].split('_')
    index=0
    if not re.search('^\d',fileName[index]):
      index+=1
    if not fileName[index]==str(productionID).zfill(8):
      fileName[index]=str(productionID).zfill(8)
    if not fileName[index+1]==str(jobID).zfill(8):
      fileName[index+1]=str(jobID).zfill(8)
    fileTupleList.append((string.join(fileName,'_'),info['outputDataType']))

  lfnRoot = ''
  debugRoot = ''
  if inputData:
    lfnRoot = _getLFNRoot(inputData,wfType)
  else:
    lfnRoot = _getLFNRoot('',wfType,wfConfigVersion)
    debugRoot= _getLFNRoot('','debug',wfConfigVersion) #only generate for non-processing jobs

  if not lfnRoot:
    return S_ERROR('LFN root could not be constructed')

  #Get all LFN(s) to both output data and BK lists at this point (fine for BK)
  outputData = []
  bkLFNs = []
  debugLFNs = []
  for fileTuple in fileTupleList:
    lfn = _makeProductionLfn(str(jobID).zfill(8),lfnRoot,fileTuple,wfMode,str(productionID).zfill(8))
    outputData.append(lfn)
    bkLFNs.append(lfn)
    if debugRoot:
      debugLFNs.append(_makeProductionLfn(str(jobID).zfill(8),debugRoot,fileTuple,wfMode,str(productionID).zfill(8)))

  if debugRoot:
    debugLFNs.append(_makeProductionLfn(str(jobID).zfill(8),debugRoot,('%s_core' % str(jobID).zfill(8) ,'core'),wfMode,str(productionID).zfill(8)))

  #Get log file path - unique for all modules
  logPath = _makeProductionPath(str(jobID).zfill(8),lfnRoot,'LOG',wfMode,str(productionID).zfill(8),log=True)
  logFilePath = ['%s/%s' %(logPath,str(jobID).zfill(8))]
  logTargetPath = ['%s/%s_%s.tar' %(logPath,str(productionID).zfill(8),str(jobID).zfill(8))]
  #[ aside, why does makeProductionPath not append the jobID itself ????
  #  this is really only used in one place since the logTargetPath is just written to a text file (should be reviewed)... ]

  #Strip output data according to file mask
  if wfMask:
    newOutputData = []
    newBKLFNs = []
    for od in outputData:
      for i in wfMask:
        if re.search('.%s$' %i,od):
          newOutputData.append(od)
    for bk in bkLFNs:
      newBKLFNs.append(bk)
    outputData = newOutputData
    bkLFNs = newBKLFNs

  if not outputData:
    gLogger.info('No output data LFN(s) constructed')
  else:
    gLogger.verbose('Created the following output data LFN(s):\n%s' %(string.join(outputData,'\n')))
  gLogger.verbose('Log file path is:\n%s' %logFilePath[0])
  gLogger.verbose('Log target path is:\n%s' %logTargetPath[0])
  if bkLFNs:
    gLogger.verbose('BookkeepingLFN(s) are:\n%s' %(string.join(bkLFNs,'\n')))
  if debugLFNs:
    gLogger.verbose('DebugLFN(s) are:\n%s' %(string.join(debugLFNs,'\n')))
  jobOutputs = {'ProductionOutputData':outputData,'LogFilePath':logFilePath,'LogTargetPath':logTargetPath,'BookkeepingLFNs':bkLFNs,'DebugLFNs':debugLFNs}
  return S_OK(jobOutputs)

#############################################################################
def constructDebugLFNs(paramDict,fileList):
  """ Construct LFNs for upload to the DEBUG SE.
  """
  paramDict['JobType']='debug' #apparently
  paramDict['outputDataFileMask']=''
  result = constructProductionLFNs(paramDict)
  if not result['OK']:
    return result

  fileDict = {}
  for lfn in result['ProductionOutputData']:
    for f in fileList:
      if os.path.basename(lfn)==f:
        fileDict[f]=lfn

  return S_OK(fileDict)

#############################################################################
def getLogPath(paramDict):
  """ Can construct log file paths even if job fails e.g. no output files available.
  """
  keys = ['PRODUCTION_ID','JOB_ID','dataType','configVersion','JobType']
  for k in keys:
    if not paramDict.has_key(k):
      return S_ERROR('%s not defined' %k)

  productionID = paramDict['PRODUCTION_ID']
  jobID = paramDict['JOB_ID']
  wfMode = paramDict['dataType']
  wfConfigVersion=paramDict['configVersion']
  wfType=paramDict['JobType']
  inputData=''
  if paramDict.has_key('InputData'):
    inputData=paramDict['InputData']

  gLogger.verbose('WFMode = %s, WFConfigVersion = %s, WFType=%s' %(wfMode,wfConfigVersion,wfType))
  lfnRoot = ''
  if inputData:
    lfnRoot = _getLFNRoot(inputData,wfType)
  else:
    lfnRoot = _getLFNRoot('',wfType,wfConfigVersion)

  #Get log file path - unique for all modules
  logPath = _makeProductionPath(str(jobID).zfill(8),lfnRoot,'LOG',wfMode,str(productionID).zfill(8),log=True)
  logFilePath = ['%s/%s' %(logPath,str(jobID).zfill(8))]
  logTargetPath = ['%s/%s_%s.tar' %(logPath,str(productionID).zfill(8),str(jobID).zfill(8))]

  gLogger.verbose('Log file path is:\n%s' %logFilePath)
  gLogger.verbose('Log target path is:\n%s' %logTargetPath)
  jobOutputs = {'LogFilePath':logFilePath,'LogTargetPath':logTargetPath}
  return S_OK(jobOutputs)

#############################################################################
def constructUserLFNs(paramDict):
  """ Under construction, will be used for constructing output LFNs for user job
      finalization.
  """
  return S_OK()

#############################################################################
def _makeProductionPath(JOB_ID,LFN_ROOT,typeName,mode,prodstring,log=False):
  """ Constructs the path in the logical name space where the output
      data for the given production will go. In
  """
  result = LFN_ROOT+'/'+typeName.upper()+'/'+prodstring+'/'
  if log:
    try:
      jobid = int(JOB_ID)
      jobindex = string.zfill(jobid/10000,4)
    except:
      jobindex = '0000'
    result += jobindex

  return result

#############################################################################
def _makeProductionLfn(JOB_ID,LFN_ROOT,filetuple,mode,prodstring):
  """ Constructs the logical file name according to LHCb conventions.
      Returns the lfn without 'lfn:' prepended.
  """
  try:
    jobid = int(JOB_ID)
    jobindex = string.zfill(jobid/10000,4)
  except:
    jobindex = '0000'

  if re.search('lfn:',fname) or re.search('LFN:',fname):
    return filetuple[0].replace('lfn:','').replace('LFN:','')
  
  return LFN_ROOT+'/'+filetuple[1].upper()+'/'+prodstring+'/'+jobindex+'/'+filetuple[0]
      
#############################################################################
def _getLFNRoot(lfn,namespace='',configVersion=0):
  """
  return the root path of a given lfn

  eg : /lhcb/data/CCRC08/00009909 = getLFNRoot(/lhcb/data/CCRC08/00009909/DST/0000/00009909_00003456_2.dst)
  eg : /lhcb/MC/<year>/  = getLFNRoot(None)
  """
  dataTypes = ['SIM','DIGI','DST','RAW','ETC','SETC','FETC','RDST','MDF','XDST']
  LFN_ROOT=''  
  
  if not lfn:
    LFN_ROOT='/lhcb/MC/'+str(configVersion)
    if namespace.lower() in ('test','debug'):
      LFN_ROOT='/lhcb/MC/%s' %(namespace)
    return LFN_ROOT
  
  lfn = [fname.replace(' ','') for fname in lfn.split(';')]
  lfnroot = lfn[0].split('/')
  for part in lfnroot:
    if not part in dataTypes:
      LFN_ROOT+='/%s' %(part)
    else:
      break
       
  if namespace.lower() in ('test','debug'):
    tmpLfnRoot = LFN_ROOT.split(os.path.sep)
    if len(tmpLfnRoot)>2:
      tmpLfnRoot[2] = namespace
    else:
      tmLfnRoot[-1] = namespace
        
    LFN_ROOT = string.join(tmpLfnRoot,os.path.sep)

  return LFN_ROOT
        
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#