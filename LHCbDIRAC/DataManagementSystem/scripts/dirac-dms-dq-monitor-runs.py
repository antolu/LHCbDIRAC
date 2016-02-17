#! /usr/bin/env python
########################################################################
# File :    dirac-dq-monitor-runs.py
# Author :  Adinolfi Mrco
########################################################################

"""
Script to check the status of merged histograms for a given data taking configuration
"""

__RCSID__ = "$Id$"

import DIRAC

from DIRAC.Core.Base            import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    'dirac-dms-dq-monitor-runs --version Collision12',
                                    'dirac-dms-dq-monitor-runs --list',
                                    'Arguments:',
                                    '  version:     Configuration version'])
                      )
Script.registerSwitch( '', 'version', '   Configuration version' )
Script.registerSwitch( '', 'list', '   List available versions' )

Script.parseCommandLine(ignoreErrors = True)
    
args = Script.getPositionalArgs()
sw = Script.getUnprocessedSwitches()
    
if sw[0][0] not in ['version','list']:
  Script.showHelp()
if sw[0][0] == 'version' and len( args ) < 1 or len( args ) > 1:
  Script.showHelp()


from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC                      import S_OK, S_ERROR, gLogger

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient       import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient  import TransformationClient


import sys
import os
import re
import copy
import time



bkClient = BookkeepingClient()
tfClient = TransformationClient()
################################################################################
#                                                                              #
# checkTransformationStatus:                                                   #
#                                                                              #
# Count the number of RAW files from the transformation client.                #
#                                                                              #
################################################################################
def checkTransformationStatus(transfId, run):
  retVal         = {}
  retVal['OK']   = False
  retVal['nRAW'] = 0
  retVal['LFN']  = []
  
  res = tfClient.getTransformationFiles({'TransformationID' : transfId,
                                         'Status'           : ['Assigned',
                                                               'Unused',
                                                               'Processed'],
                                         'RunNumber'        : run})

#  res = tfClient.getTransformationRunStats([transfId]) 
#  if not res['OK']:
#    gLogger.error(res['Message'])
#    return retVal

  retVal['nRAW'] = len(res['Value'])
  retVal['LFN']  = res['Value']
  retVal['OK']   = True

#  if res['Value'].has_key(transfId):
#    if res['Value'][transfId].has_key(run):
#      retVal['nRAW']  = res['Value'][transfId][run]['Total']
#      if res['Value'][transfId][run].has_key('Processed'):
#        retVal['nProc'] = res['Value'][transfId][run]['Processed']
  
  return retVal
################################################################################
#                                                                              #
# countRAWandProcessed:                                                        #
#                                                                              #
#                                                                              #
#                                                                              #
################################################################################
def countRAWandProcessed(bkDict, transfId, run):
  retVal = {}
  retVal['OK']            = False
  retVal['nRAW']          = 0
  retVal['nProc']         = 0
  retVal['nBRUNELHIST']   = 0
  retVal['nDAVINCIHIST']  = 0

  #
  # Get the list of RAW files.
  #

  res = {}
  if transfId:
    res = checkTransformationStatus(transfId, run)
    if not res['OK']:
      return retVal
    if res['nRAW'] == 0:
      res = getCountRAW(bkDict, run)      
  else:
    res = getCountRAW(bkDict, run)

  retVal['nRAW'] = res['nRAW']
  lfnRAW = res['LFN']

  #
  # Get the list of DAVINCIHIST files.
  #

  res = getCountHist(bkDict, 'DAVINCIHIST', run)
  if not res['OK']:
    return retVal
  retVal['nDAVINCIHIST'] = res['nHist']
  lfnDAVINCIHIST = res['LFN']

  #
  # Get the list of BRUNELHIST files.
  #

  res = getCountHist(bkDict, 'BRUNELHIST', run)
  if not res['OK']:
    return retVal
  retVal['nBRUNELHIST'] = res['nHist']
  lfnBRUNELHIST = res['LFN']

  #
  # Now count how many RAW ancestors the BRUNELHIST have.

  res = bkClient.getFileAncestors(lfnBRUNELHIST, 2)
  if not res['OK']:
    gLogger.error(res['Message'])
    return retVal

  ancestors = {}
  for brunelLFN in lfnBRUNELHIST:
    for lfnRecord in res['Value']['Successful'][brunelLFN]:
      if lfnRecord['FileType'] == 'RAW':
        lfn = lfnRecord['FileName']
        ancestors[lfn] = True

  retVal['OK']    = True
  retVal['nProc'] = len(ancestors.keys())

  return retVal
################################################################################
#                                                                              #
# createDictionaryList:                                                        #
#                                                                              #
# Create a list of all dictionary that need to be tried.                       #
#                                                                              #
################################################################################
def createDictionaryList(cfgName, cfgVesrionList):
  retVal = {}
  retVal['OK']         = True
  retVal['bkDictList'] = []

  for cfgVersion in cfgVersionList:
    bkTree = {'ConfigName'    : cfgName,
              'ConfigVersion' : cfgVersion}
    res = getRunningConditions(bkTree,cfgVersion)
    if not res['OK']:
      retVal['OK'] = False
      return retVal
    
    tmpBkDictList = res['bkDictList']
    
    for bkDict in tmpBkDictList:
      res = getProcessingPasses(bkDict)
      if not res['OK']:
        retVal['OK'] = False
        return retVal
      retVal['bkDictList'].extend(res['bkDictList'])

  return retVal
################################################################################
#                                                                              #
# getCountHist:                                                                #
#                                                                              #
# Count the number of histograms in the run given the pass.                    #
#                                                                              #
################################################################################
def getCountHist(bkDict, histType, runId):
  retVal = {}
  retVal['OK'] = False

  bkDictCount = copy.deepcopy(bkDict)
  
  bkDictCount['FileType']  = histType
  bkDictCount['StartRun']  = runId
  bkDictCount['EndRun']    = runId
  if bkDictCount.has_key('DataQualityFlag'):
    del(bkDictCount['DataQualityFlag'])

  res = bkClient.getFilesWithGivenDataSets(bkDictCount)

  res = bkClient.getFiles(bkDictCount)
  if not res['OK']:
    gLogger.error(res['Message'])
    return retVal

  retVal['OK']    = True
  retVal['nHist'] = len(res['Value'])
  retVal['LFN']   = res['Value']
  
  return retVal
################################################################################
#                                                                              #
# getCountRAW:                                                                 #
#                                                                              #
# Count the number of RAW files in the run.                                    #
#                                                                              #
################################################################################
def getCountRAW(bkDict, runId):
  retVal = {}
  retVal['OK'] = False

  bkDictCount = copy.deepcopy(bkDict)

  bkDictCount['FileType']        = 'RAW'
  bkDictCount['ProcessingPass']  = '/Real Data'
  bkDictCount['StartRun']        = runId
  bkDictCount['EndRun']          = runId

  if bkDictCount.has_key('DataQualityFlag'):
    del(bkDictCount['DataQualityFlag'])

  res = bkClient.getFiles(bkDictCount)
  if not res['OK']:
    gLogger.error(res['Message'])
    return retVal

  retVal['OK']   = True
  retVal['nRAW'] = len(res['Value'])
  retVal['LFN']  = res['Value']
  
  return retVal
################################################################################
#                                                                              #
# getProcessingPasses:                                                         #
#                                                                              #
# Find all known processing passes for the selected configurations.            #
#                                                                              #
################################################################################
def getProcessingPasses(bkDict):
  retVal = {}
  retVal['OK']         = True
  retVal['bkDictList'] = []

  #
  # Look for all processing passes
  #

  res = bkClient.getProcessingPass(bkDict, '/Real Data')

  if not res['OK']:
    gLogger.error( 'Cannot load the processing passes for Version Data taking condition' )
    gLogger.error(res['Message'])
    retVal['OK'] = False
    return retVal

  procPassList = {}
  for recordList in res['Value']:
    if recordList['TotalRecords'] == 0:
      continue
    parNames = recordList['ParameterNames']

    found = False
    for thisId in range(len(parNames)):
      parName = parNames[thisId]
      if parName == 'Name':
        found = True
        break
    if found:
      for reco in recordList['Records']:
        #
        # Skip Reco14 for the time being,
        #
        if reco[0] == 'Reco14':
          continue
        procPass = '/Real Data/' + reco[0]
        procPassList[procPass] = True
  #
  # Create all dictionaries
  #
  for procPass in procPassList:
    myDict = copy.deepcopy(bkDict)
    myDict['ProcessingPass']  = procPass
    myDict['DataQualityFlag'] = 'UNCHECKED'
    retVal['bkDictList'].append(myDict)

  return retVal
################################################################################
#                                                                              #
# getProductionId:                                                             #
#                                                                              #
# Extract the production id number from the histogram file name.               #
#                                                                              #
################################################################################
def getProductionId(run, procPass):
  retVal = {}
  retVal['OK']     = False
  retVal['prodId'] = 0

  bkDict = {'Runnumber' : run,
            'ProcPass'  : procPass}
  res = bkClient.getProductionsFromView(bkDict)

  if not res['OK']:
    outMess = 'Cannot get the production id for run %s proc. pass. %s' %(
      run, procPass)
    gLogger.error(outMess)
    gLogger.error(res['Message'])
    return retVal

  prodId = 0
  if not len(res['Value']):
    outMess = 'Empty production id list for run %s proc. pass. %s' %(
      run, procPass)
    gLogger.error(outMess)
    gLogger.error(res)
    return retVal

  allProdList = res['Value']
  for prodList in allProdList:
    thisProdId = int(prodList[0])
    res = bkClient.getProductionInformations(thisProdId)
    if not res['OK']:
      outMess = 'Cannot get the information for production %s' %(thisProdId)
      gLogger.error(outMess)
      gLogger.error(res['Message'])
      return retVal

    if int(res['Value']['Production informations'][0][2]) == 90000000:
      if thisProdId > prodId:
        prodId = thisProdId

  if prodId == 0:
    return retVal

  retVal['OK']     = True
  retVal['prodId'] = str(prodId)

  length = len(retVal['prodId'])  
  for i in range(8-length):
    retVal['prodId'] = '0' + retVal['prodId']

  return retVal
################################################################################
#                                                                              #
# getRunningConditions:                                                        #
#                                                                              #
# Find all known running conditions for the selected configurations.           #
#                                                                              #
################################################################################
def getRunningConditions(bkTree,cfgVersion):
  retVal = {}
  retVal['OK']         = True
  retVal['bkDictList'] = []
  
  res = bkClient.getConditions(bkTree)
  
  dtdList = {}
  
  if not res['OK']:
    outMess = 'Cannot load the data taking conditions for version %s' %( cfgVersion)
    gLogger.error(outMess)
    gLogger.error(res['Message'])
    retVal['OK'] = False
    return retVal

  #
  # Look for all known DataTakingDescription
  #
  for recordList in res['Value']:
    if recordList['TotalRecords'] == 0:
      continue
    parNames = recordList['ParameterNames']
    
    descId = -1
    for thisId in range(len(parNames)):
      parName = parNames[thisId]
      if parName == 'Description':
        descId = thisId
        break

    if descId < 0:
      continue

    records = recordList['Records']
    for record in records:
      desc = record[descId]
      #
      # Skip non interesting DataTakingDescriptions
      #
      if re.search('4000', desc):
        if re.search('Excl', desc):
          continue
        elif re.search('VeloOpen', desc):
          continue
      else:
        continue

      dtdList[desc] = True

  #
  # Create all dictionaries
  #
  for dtd in dtdList:
    myDict = copy.deepcopy(bkTree)
    myDict['ConditionDescription'] = dtd
    retVal['bkDictList'].append(myDict)
      
  return retVal
################################################################################
#                                                                              #
# getTransformationId:                                                         #
#                                                                              #
#                                                                              #
#                                                                              #
################################################################################
def getTransformationId(bkDict, run):
  retVal             = {}
  retVal['OK']       = False
  #
  # Get the  production ids for this run
  #
  res = tfClient.getTransformationRuns({'RunNumber' : run})
  if not res['OK']:
    gLogger.error(res['Message'])
    return retVal
  if not res['Records']:
    gLogger.error('Cannot get the transformations that processing run %s' %str(run))
    return retVal

  prodIdList = []
  for tf in res['Records']:
    prodIdList.append(tf[0])

  transfIdList = []
  for prodId in prodIdList:
    res = bkClient.getProductionProcessingPass(prodId)
    if not res['OK']:
      continue
    else:
      if res['Value'] == bkDict['ProcessingPass']:
        transfIdList.append(prodId)

  if len(transfIdList) > 1:
    gLogger.error('Number of transformation = %d for run %d' %(len(transfIdList), run))
    return retVal
  elif len(transfIdList) < 1:
    return retVal
  else:
    retVal['OK']       = True
    retVal['transfId'] = transfIdList[0]

  return retVal
################################################################################
#                                                                              #
# makeTargetFile:                                                              #
#                                                                              #
# Define the full path of the final histogram file.                            #
#                                                                              #
################################################################################
def makeTargetFile(run, dtd, procPass):
  retVal         = {}
  retVal['OK']   = True

  res = getProductionId(run, procPass)
  if not res['OK']:
    retVal['OK'] = False
    return retVal
  
  procPassDirName = re.sub('\/',        '',  procPass)
  procPassDirName = re.sub('Real Data', '',  procPassDirName)
  procPassDirName = re.sub('\+',        '',  procPassDirName)
  procPassDirName = re.sub('\s* ',      '_', procPassDirName)

  prodId  = res['prodId']
  destDir = "%s/%s/%s/%s/90000000" %(homeDir, dtd, run, procPassDirName)

  retVal['targetFile'] = '%s/BrunelDaVinci_FULL_%s_%s.root' %(destDir, run, prodId)

  return retVal
################################################################################
#                                                                              #
# processDictionaryList:                                                       #
#                                                                              #
#                                                                              #
#                                                                              #
################################################################################
def processDictionaryList(bkDictList):
  retVal         = {}
  retVal['OK']   = True

  for bkDict in bkDictList:
      res = bkClient.getListOfRuns(bkDict)
      if not res['OK']:
        gLogger.error(res['Message'])
        retVal['OK'] = False
        return retVal

      runList ={}
      for run in res['Value']:
        runList[run] = True
      #
      # Skip flagged runs
      #
      
      res = bkClient.getRunWithProcessingPassAndDataQuality(bkDict['ProcessingPass'])
      if not res['OK']:
        gLogger.error(res['Message'])
        return retVal
        
      for run in res['Value']:
        if runList.has_key(run):
          del(runList[run])
        
      res = processRuns(bkDict, runList)
      if not res['OK']:
        retVal['OK'] = False
        return retVal

      if not len(res['Records']) == 0:
          print 'There are %d UNCHECKED runs in %s%s' %(len(res['Records']),
                                                        bkDict['ConditionDescription'],
                                                        bkDict['ProcessingPass'])
          for record in res['Records']:
            print record
          print
          
  return retVal
################################################################################
#                                                                              #
# processRuns:                                                                 #
#                                                                              #
#                                                                              #
#                                                                              #
################################################################################
def processRuns(bkDict, runList):
  retVal            = {}
  retVal['OK']      = False
  retVal['Records'] = []
  
  dtd      = bkDict['ConditionDescription'] 
  procPass = bkDict['ProcessingPass']


  for run in sorted( runList.keys() ):
    #
    # Check if a run has been merged.
    #
    res = tfClient.getRunsMetadata(int(run))
    if not res['OK']:
      gLogger.error(res['Message'])
      return retVal

    mergeFlag = ''
    if res['Value'].has_key(run):
      if res['Value'][run].has_key('DQFlag'):
        mergeFlag = res['Value'][run]['DQFlag']
    #
    # If merged check if downloaded.
    #
    res = makeTargetFile(run, dtd, procPass)
    if not res['OK']:
      return retVal

    if mergeFlag == 'M':      
      if os.path.isfile(res['targetFile']):
        outmess = '%d %s' %(run, res['targetFile'])
      else:
        outmess = '%d Merged to be downloaded' %(run)
      retVal['Records'].append(outmess)
    else:
      #
      # If not merged check the processing status counting the number of RAW
      # BRUNELHIST and DAVINCIHIST
      #
      #
      # First get the transformation id and fraction of files
      # that will be processed. 
      #
      transfId = 0
      res = getTransformationId(bkDict, run)
      if res['OK']:
        transfId = res['transfId']

      res = countRAWandProcessed(bkDict, transfId, run)
      if not res['OK']:
        return retVal

      nRAW     = res['nRAW']
      nProc    = res['nProc']
      nBrunel  = res['nBRUNELHIST']
      nDaVinci = res['nDAVINCIHIST']
      #
      # Calculate how many files were sent for reconstruction and how many
      # is 95% of those.
      #
      nEXP  = int(0.95 * nRAW)

      if nProc and nProc >= nEXP:
        outmess = '%s Completed not merged: RAW Count = %4d (%4d) Processed = %4d BRUNEL hist count = %4d DAVINCI hist count = %4d' %(run, nRAW, nEXP, nProc, nBrunel, nDaVinci)
      else:
        outmess = '%s Not completed: RAW Count = %4d (%4d) Processed = %4d BRUNEL hist count = %4d DAVINCI hist count = %4d' %(run, nRAW, nEXP, nProc, nBrunel, nDaVinci)

      retVal['Records'].append(outmess)

  retVal['OK'] = True
  return retVal
################################################################################
#                                                                              #
#                                  >>> Main <<<                                #
#                                                                              #
################################################################################
if __name__=="__main__":
  baseDir = '/afs/cern.ch/lhcb/group/dataquality/ROOT'
  homeDir = '/afs/cern.ch/lhcb/group/dataquality/ROOT/Collision12'
  
  exitCode = 0
  
  cfgName        = 'LHCb'
  if sw[0][0] == 'list':
    res = bkClient.getConfigVersions({'ConfigName':cfgName})
    for vers in res['Value']['Records']:
      print vers[0]
    DIRAC.exit(exitCode)
    
  cfgVersionList = [str(args[0])]
  



  res      = createDictionaryList(cfgName, cfgVersionList)
  if not res:
    exitCode = 2
  else:
    bkDictList = res['bkDictList']
    res = processDictionaryList(bkDictList)
    if not res:
      exitCode = 2

  DIRAC.exit(exitCode)


