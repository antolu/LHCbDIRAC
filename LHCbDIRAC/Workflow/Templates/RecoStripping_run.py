""" Moving toward a templates-less system
"""

__RCSID__ = "$Id$"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
from DIRAC.Core.Base import Script
Script.parseCommandLine()

import DIRAC

import LHCbDIRAC.Workflow.Templates.TemplatesUtilities

from DIRAC import gLogger
gLogger = gLogger.getSubLogger( 'RecoStripping_run.py' )

#################################################################################
# Below here is the actual production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

###########################################
# Configurable and fixed parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

w = '{{w#----->WORKFLOW: choose one below#}}'
w1 = '{{w1#-WORKFLOW1: Reconstruction#False}}'
w2 = '{{w2#-WORKFLOW2: Stripping+Merge#False}}'
w3 = '{{w3#-WORKFLOW3: RecoStripping+Merge#False}}'
w4 = '{{w4#-WORKFLOW4: Reconstruction+Stripping+Merge#False}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True for validation prod#False}}'

# workflow params for all productions
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc46-opt#ANY}}'
startRun = '{{startRun#GENERAL: run start, to set the start run#0}}'
endRun = '{{endRun#GENERAL: run end, to set the end of the range#0}}'
runsList = '{{runsList#GENERAL: discrete list of run numbers (do not mix with start/endrun)#}}'
targetSite = '{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch#ALL}}'
extraOptions = '{{extraOptions#GENERAL: extra options as python dict stepNumber:options#}}'
outputMode = '{{outputMode#GENERAL: Workflow upload workflow output#Any}}'

#reco params
recoPriority = '{{RecoPriority#PROD-RECO(Stripp): priority#2}}'
recoCPU = '{{RecoMaxCPUTime#PROD-RECO(Stripp): Max CPU time in secs#1000000}}'
recoPlugin = '{{RecoPluginType#PROD-RECO(Stripp): production plugin name#AtomicRun}}'
recoAncestorProd = '{{RecoAncestorProd#PROD-RECO(Stripp): ancestor production if any#0}}'
recoDataSE = '{{RecoDataSE#PROD-RECO(Stripp): Output Data Storage Element#Tier1-BUFFER}}'
recoFilesPerJob = '{{RecoFilesPerJob#PROD-RECO(Stripp): Group size or number of files per job#1}}'
recoType = '{{RecoType#PROD-RECO(Stripp): DataReconstruction or DataReprocessing#DataReconstruction}}'

#stripp params
strippPriority = '{{priority#PROD-Stripping: priority#5}}'
strippCPU = '{{StrippMaxCPUTime#PROD-Stripping: Max CPU time in secs#1000000}}'
strippPlugin = '{{StrippPluginType#PROD-Stripping: plugin name#ByRunWithFlush}}'
strippFilesPerJob = '{{StrippFilesPerJob#PROD-Stripping: Group size or number of files per job#2}}'
strippDataSE = '{{StrippStreamSE#PROD-Stripping: output data SE (un-merged streams)#Tier1-BUFFER}}'
strippIDPolicy = '{{strippIDPolicy#PROD-Stripping: policy for input data access (download or protocol)#download}}'

#merging params
mergingDQFlag = '{{MergeDQFlag#PROD-Merging: DQ Flag e.g. OK,UNCHECKED#OK;;;UNCHECKED}}'
mergingPriority = '{{MergePriority#PROD-Merging: priority#8}}'
mergingPlugin = '{{MergePlugin#PROD-Merging: plugin#MergeByRunWithFlush}}'
mergingRemoveInputsFlag = '{{MergeRemoveFlag#PROD-Merging: remove input data flag True/False#True}}'
mergingCPU = '{{MergeMaxCPUTime#PROD-Merging: Max CPU time in secs#300000}}'
mergingFileSize = '{{MergeFileSize#PROD-Merging: Size (in GB) of the merged files#5}}'
mergingIDPolicy = '{{MergeIDPolicy#PROD-Merging: policy for input data access (download or protocol)#download}}'
mergingDataSE = '{{MergeStreamSE#PROD-Merging: output data SE (merged streams)#Tier1_M-DST}}'

requestID = '{{ID}}'
parentReq = '{{_parent}}'
prodGroup = '{{pDsc}}'
#used in case of a test e.g. certification etc.
bkConfigName = '{{configName}}'
bkConfigVersion = '{{configVersion}}'
#Other parameters from the request page
DQFlag = '{{inDataQualityFlag}}' #UNCHECKED
dataTakingCond = '{{simDesc}}'
processingPass = '{{inProPass}}'
bkFileType = '{{inFileType}}'
eventType = '{{eventType}}'
recoEvtsPerJob = '-1'

w1 = eval( w1 )
w2 = eval( w2 )
w3 = eval( w3 )
w4 = eval( w4 )

certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
if extraOptions:
  extraOptions = eval( extraOptions )
mergeRemoveInputsFlag = eval( mergingRemoveInputsFlag )

if not w1 and not w2 and not w3 and not w4:
  gLogger.error( 'I told you to select at least one workflow!' )
  DIRAC.exit( 2 )

recoTracking = 0
strippingTracking = 0
mergingTracking = 0

if w1:
  recoTracking = 1
else:
  simulationTracking = 1

if certificationFlag or localTestFlag:
  testFlag = True
  if certificationFlag:
    publishFlag = True
  if localTestFlag:
    publishFlag = False
    stripping = False
    merging = False
else:
  publishFlag = True
  testFlag = False

diracProd = DiracProduction()

#In case we want just to test, we publish in the certification/test part of the BKK
if testFlag:
  configName = 'certification'
  configVersion = 'test'
  recoEvtsPerJob = '25'
  strppEventsPerJob = '1000'
  mergingGroupSize = '1'
  recoCPU = '200000'
  recoStartRun = '93718'
  recoEndRun = '93720'
  recoCPU = '100000'
  dataTakingCond = 'Beam3500GeV-VeloClosed-MagUp'
  processingPass = 'Real Data'
  bkFileType = 'RAW'
  eventType = '90000000'
  DQFlag = 'ALL'

DQFlag = DQFlag.replace( ',', ';;;' ).replace( ' ', '' )

inputBKQuery = {
                    'DataTakingConditions'     : dataTakingCond,
                    'ProcessingPass'           : processingPass,
                    'FileType'                 : bkFileType,
                    'EventType'                : eventType,
                    'ConfigName'               : bkConfigName,
                    'ConfigVersion'            : bkConfigVersion,
                    'ProductionID'             : 0,
                    'DataQualityFlag'          : DQFlag
                    }


stepsList = []

stepsList.append( '{{p1Step}}' )
stepsList.append( '{{p2Step}}' )
stepsList.append( '{{p3Step}}' )
stepsList.append( '{{p4Step}}' )
stepsList.append( '{{p5Step}}' )
stepsList.append( '{{p6Step}}' )
stepsList.append( '{{p7Step}}' )
stepsList.append( '{{p8Step}}' )
stepsList.append( '{{p9Step}}' )

#get a list of steps dictionaries
stepsDictList = LHCbDIRAC.Workflow.Templates.TemplatesUtilities.resolveSteps( stepsList )

recoSteps = []
strippingSteps = []
mergingSteps = []

prodsList = []

if w1:
  recoSteps = stepsDictList
  prodsList.append( ( recoType, recoSteps, inputBKQuery, False, recoTracking, recoDataSE, recoPriority, recoCPU ) )
elif w2:
  strippingSteps = stepsDictList[:-1]
  if mergingPlugin == 'ByRunFileTypeSizeWithFlush':
    mergingSteps = stepsDictList[-1:]
  else:
    mergingSteps = stepsDictList[-1:]
  prodsList.append( ( 'DataStripping', strippingSteps, inputBKQuery, False, strippingTracking,
                      strippDataSE, strippPriority, strippCPU ) )
  for s in mergingSteps:
    prodsList.append( ( 'Merge', [s], 'fromPreviousProd', mergeRemoveInputsFlag, mergingTracking,
                        mergingDataSE, mergingPriority, mergingCPU ) )
elif w3:
  recoSteps = stepsDictList[:-1]
  if mergingPlugin == 'ByRunFileTypeSizeWithFlush':
    mergingSteps = stepsDictList[-1:]
  else:
    mergingSteps = LHCbDIRAC.Workflow.Templates.TemplatesUtilities._splitIntoProductionSteps( stepsDictList[-1:] )
  prodsList.append( ( recoType, recoSteps, inputBKQuery, False, recoTracking, recoDataSE, recoPriority, recoCPU ) )
  for s in mergingSteps:
    prodsList.append( ( 'Merge', [s], 'fromPreviousProd', mergeRemoveInputsFlag, mergingTracking,
                        mergingDataSE, mergingPriority, mergingCPU ) )
elif w4:
  recoSteps = stepsDictList[:-2]
  strippingSteps = stepsDictList[-2:-1]
  if mergingPlugin == 'ByRunFileTypeSizeWithFlush':
    mergingSteps = stepsDictList[-1:]
  else:
    mergingSteps = LHCbDIRAC.Workflow.Templates.TemplatesUtilities._splitIntoProductionSteps( stepsDictList[-1:] )
  prodsList.append( ( recoType, recoSteps, inputBKQuery, False, recoTracking, recoDataSE, recoPriority, recoCPU ) )
  prodsList.append( ( 'DataStripping', strippingSteps, 'fromPreviousProd', False,
                      strippingTracking, strippDataSE, strippPriority, strippCPU ) )
  for s in mergingSteps:
    prodsList.append( ( 'Merge', [s], 'fromPreviousProd', mergeRemoveInputsFlag, mergingTracking,
                        mergingDataSE, mergingPriority, mergingCPU ) )

prodID = 0
for prodType, stepsList, bkQuery, removeInput, tracking, outputSE, priority, cpu in prodsList:
  prod = LHCbDIRAC.Workflow.Templates.TemplatesUtilities.buildProduction( prodType = prodType,
                                                                          stepsList = stepsList,
                                                                          requestID = requestID,
                                                                          prodDesc = prodGroup,
                                                                          configName = configName,
                                                                          configVersion = configVersion,
                                                                          dataTakingConditions = dataTakingCond,
                                                                          appendName = appendName,
                                                                          extraOptions = extraOptions,
                                                                          defaultOutputSE = outputSE,
                                                                          eventType = '{{eventType}}',
                                                                          events = eventType,
                                                                          priority = priority,
                                                                          cpu = cpu,
                                                                          sysConfig = sysConfig,
                                                                          generatorName = '{{Generator}}',
                                                                          outputMode = outputMode,
                                                                          targetSite = targetSite,
                                                                          removeInputData = removeInput,
                                                                          bkQuery = bkQuery,
                                                                          previousProdID = prodID )

  prodID = LHCbDIRAC.Workflow.Templates.TemplatesUtilities.launchProduction( prod = prod,
                                                                             publishFlag = publishFlag,
                                                                             testFlag = testFlag,
                                                                             requestID = requestID,
                                                                             tracking = tracking,
                                                                             diracProd = diracProd,
                                                                             logger = gLogger )
