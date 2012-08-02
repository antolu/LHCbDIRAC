""" Moving toward a templates-less system
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import DIRAC

from DIRAC import gLogger
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

gLogger = gLogger.getSubLogger( 'RecoStripping_run.py' )

pr = ProductionRequest()

pr.stepsList.append( '{{p1Step}}' )
pr.stepsList.append( '{{p2Step}}' )
pr.stepsList.append( '{{p3Step}}' )
pr.stepsList.append( '{{p4Step}}' )
pr.stepsList.append( '{{p5Step}}' )
pr.stepsList.append( '{{p6Step}}' )
pr.stepsList.append( '{{p7Step}}' )
pr.stepsList.append( '{{p8Step}}' )
pr.stepsList.append( '{{p9Step}}' )

pr.resolveSteps()

###########################################
# Configurable and fixed parameters
###########################################

pr.appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

w = '{{w#----->WORKFLOW: choose one below#}}'
w1 = '{{w1#-WORKFLOW1: Reconstruction#False}}'
w2 = '{{w2#-WORKFLOW2: Stripping+Merge#False}}'
w3 = '{{w3#-WORKFLOW3: RecoStripping+Merge#False}}'
w4 = '{{w4#-WORKFLOW4: Reconstruction+Stripping+Merge#False}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True for validation prod#False}}'

# workflow params for all productions
pr.sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc46-opt#ANY}}'
pr.startRun = '{{startRun#GENERAL: run start, to set the start run#0}}'
pr.endRun = '{{endRun#GENERAL: run end, to set the end of the range#0}}'
pr.runsList = '{{runsList#GENERAL: discrete list of run numbers (do not mix with start/endrun)#}}'
targetSite = '{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch#ALL}}'
extraOptions = '{{extraOptions#GENERAL: extra options as python dict stepNumber:options#}}'

#reco params
recoPriority = '{{RecoPriority#PROD-RECO(Stripp): priority#2}}'
recoCPU = '{{RecoMaxCPUTime#PROD-RECO(Stripp): Max CPU time in secs#1000000}}'
recoPlugin = '{{RecoPluginType#PROD-RECO(Stripp): production plugin name#AtomicRun}}'
recoFilesPerJob = '{{RecoFilesPerJob#PROD-RECO(Stripp): Group size or number of files per job#1}}'
recoDataSE = '{{RecoDataSE#PROD-RECO(Stripp): Output Data Storage Element#Tier1-BUFFER}}'
recoType = '{{RecoType#PROD-RECO(Stripp): DataReconstruction or DataReprocessing#DataReconstruction}}'
recoAncestorProd = '{{RecoAncestorProd#PROD-RECO(Stripp): ancestor production if any#0}}'

#stripp params
strippPriority = '{{priority#PROD-Stripping: priority#5}}'
strippCPU = '{{StrippMaxCPUTime#PROD-Stripping: Max CPU time in secs#1000000}}'
strippPlugin = '{{StrippPluginType#PROD-Stripping: plugin name#ByRunWithFlush}}'
strippFilesPerJob = '{{StrippFilesPerJob#PROD-Stripping: Group size or number of files per job#2}}'
strippDataSE = '{{StrippStreamSE#PROD-Stripping: output data SE (un-merged streams)#Tier1-BUFFER}}'
strippIDPolicy = '{{strippIDPolicy#PROD-Stripping: policy for input data access (download or protocol)#download}}'
strippAncestorProd = '{{StrippAncestorProd#PROD-Stripping: ancestor production if any#0}}'

#merging params
mergingPriority = '{{MergePriority#PROD-Merging: priority#8}}'
mergingCPU = '{{MergeMaxCPUTime#PROD-Merging: Max CPU time in secs#300000}}'
mergingPlugin = '{{MergePlugin#PROD-Merging: plugin#MergeByRunWithFlush}}'
mergingFileSize = '{{MergeFileSize#PROD-Merging: Size (in GB) of the merged files#5}}'
mergingDataSE = '{{MergeStreamSE#PROD-Merging: output data SE (merged streams)#Tier1_M-DST}}'
mergingIDPolicy = '{{MergeIDPolicy#PROD-Merging: policy for input data access (download or protocol)#download}}'
mergingRemoveInputsFlag = '{{MergeRemoveFlag#PROD-Merging: remove input data flag True/False#True}}'

pr.requestID = '{{ID}}'
pr.prodGroup = '{{pDsc}}'
#used in case of a test e.g. certification etc.
pr.configName = '{{configName}}'
pr.configVersion = '{{configVersion}}'
#Other parameters from the request page
pr.DQFlag = '{{inDataQualityFlag}}' #UNCHECKED
pr.dataTakingConditions = '{{simDesc}}'
pr.processingPass = '{{inProPass}}'
pr.bkFileType = '{{inFileType}}'
pr.eventType = '{{eventType}}'
pr.events = int( '{{NbOfEvents}}' )

w1 = eval( w1 )
w2 = eval( w2 )
w3 = eval( w3 )
w4 = eval( w4 )

certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
if extraOptions:
  pr.extraOptions = eval( extraOptions )
mergeRemoveInputsFlag = eval( mergingRemoveInputsFlag )

if not w1 and not w2 and not w3 and not w4:
  gLogger.error( 'I told you to select at least one workflow!' )
  DIRAC.exit( 2 )

if certificationFlag or localTestFlag:
  testFlag = True
  if certificationFlag:
    publishFlag = True
  if localTestFlag:
    publishFlag = False
else:
  publishFlag = True
  testFlag = False

inputDataList = []
if not publishFlag:
  #this is 1380Gev MagUp
  #recoTestData = 'LFN:/lhcb/data/2011/RAW/FULL/LHCb/COLLISION11/88162/088162_0000000020.raw'
  #this is collision11
  recoTestData = 'LFN:/lhcb/data/2011/RAW/FULL/LHCb/COLLISION11/89333/089333_0000000003.raw'

  inputDataList.append( recoTestData )
  recoIDPolicy = 'protocol'

#In case we want just to test, we publish in the certification/test part of the BKK
if testFlag:
  pr.configName = 'certification'
  pr.configVersion = 'test'
  pr.events = 25
  strppEventsPerJob = '1000'
  mergingGroupSize = '1'
  recoCPU = '200000'
  pr.endRun = '93718'
  pr.startRun = '93720'
  recoCPU = '100000'
  pr.dataTakingConditions = 'Beam3500GeV-VeloClosed-MagUp'
  pr.processingPass = 'Real Data'
  pr.bkFileType = 'RAW'
  pr.eventType = '90000000'
  pr.DQFlag = 'ALL'

pr._buildFullBKKQuery()

if w1:
  pr.prodsTypeList = [recoType]
  pr.outputSEs = [recoDataSE]
  pr.stepsInProds = [range( 1, len( pr.stepsList ) + 1 )]
  pr.removeInputsFlags = [False]
  pr.priorities = [recoPriority]
  pr.CPUs = [recoCPU]
  pr.groupSizes = [recoFilesPerJob]
  pr.plugins = [recoPlugin]
elif w2:
  pr.prodsTypeList = ['DataStripping', 'Merge']
  pr.outputSEs = [strippDataSE, mergingDataSE]
  pr.stepsInProds = [range( 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, mergingRemoveInputsFlag]
  pr.priorities = [strippPriority, mergingPriority]
  pr.CPUs = [strippCPU, mergingCPU]
  pr.groupSizes = [strippFilesPerJob, mergingGroupSize]
  pr.plugins = [strippPlugin, mergingPlugin]
elif w3:
  pr.prodsTypeList = [recoType, 'Merge']
  pr.outputSEs = [recoDataSE, mergingDataSE]
  pr.stepsInProds = [range( 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, mergingRemoveInputsFlag]
  pr.priorities = [recoPriority, mergingPriority]
  pr.CPUs = [recoCPU, mergingCPU]
  pr.groupSizes = [recoFilesPerJob, mergingGroupSize]
  pr.plugins = [recoPlugin, mergingPlugin]
elif w4:
  pr.prodsTypeList = [recoType, 'DataStripping', 'Merge']
  pr.outputSEs = [recoDataSE, strippDataSE, mergingDataSE]
  pr.stepsInProds = [range( 1, len( pr.stepsList ) - 1 ),
                     range( len( pr.stepsList ) - 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, False, mergingRemoveInputsFlag]
  pr.priorities = [recoPriority, strippPriority, mergingPriority]
  pr.CPUs = [recoCPU, strippCPU, mergingCPU]
  pr.groupSizes = [recoFilesPerJob, strippFilesPerJob, mergingGroupSize]
  pr.plugins = [recoPlugin, strippPlugin, mergingPlugin]

pr.buildAndLaunchRequest()
