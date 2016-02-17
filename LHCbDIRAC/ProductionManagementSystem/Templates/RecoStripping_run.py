""" Moving toward a templates-less system

    The RecoStripping Template creates workflows for the following use-cases:
      WORKFLOW1: Reconstruction
      WORKFLOW2: Stripping+Merge
      WORKFLOW3: RecoStripping+Merge (Reco and Stripping within the same job)
      WORKFLOW4: Reconstruction+Stripping+Merge
      WORKFLOW5: Stripping+Merge+Indexing

    Exotic things you might want to do:
    * run a local test:
      pre: remember to check if your input file is online, if not use lcg-bringonline <PFN>
    * run only part of the request on the Grid:
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import DIRAC

from DIRAC import gLogger
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

gLogger = gLogger.getSubLogger( 'RecoStripping_run.py' )

pr = ProductionRequest()

stepsList = [ '{{p1Step}}' ]
stepsList.append( '{{p2Step}}' )
stepsList.append( '{{p3Step}}' )
stepsList.append( '{{p4Step}}' )
stepsList.append( '{{p5Step}}' )
stepsList.append( '{{p6Step}}' )
stepsList.append( '{{p7Step}}' )
stepsList.append( '{{p8Step}}' )
stepsList.append( '{{p9Step}}' )
pr.stepsList = stepsList

###########################################
# Configurable and fixed parameters
###########################################

pr.appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

w = '{{w#----->WORKFLOW: choose one below#}}'
w1 = '{{w1#-WORKFLOW1: Reconstruction#False}}'
w2 = '{{w2#-WORKFLOW2: Stripping+Merge#False}}'
w3 = '{{w3#-WORKFLOW3: RecoStripping+Merge#False}}'
w4 = '{{w4#-WORKFLOW4: Reconstruction+Stripping+Merge#False}}'
w5 = '{{w5#-WORKFLOW5: Stripping+Merge+Indexing #False}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True for validation prod#False}}'

# workflow params for all productions
pr.startRun = '{{startRun#GENERAL: run start, to set the start run#0}}'
pr.endRun = '{{endRun#GENERAL: run end, to set the end of the range#0}}'
pr.runsList = '{{runsList#GENERAL: discrete list of run numbers (do not mix with start/endrun)#}}'
targetSite = '{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch#ALL}}'
extraOptions = '{{extraOptions#GENERAL: extra options as python dict stepNumber:options#}}'
pr.derivedProduction = '{{AncestorProd#GENERAL: ancestor prod to be derived#0}}'
pr.previousProdID = int( '{{previousProdID#GENERAL: previous prod ID (for BK query)#0}}' )
pr.fractionToProcess = int( '{{fractionToProcess#GENERAL: fraction to process, per run#0}}' )
pr.minFilesToProcess = int( '{{minFilesToProcess#GENERAL: minimum number of files to process, per run#0}}' )

# reco params
recoPriority = int( '{{RecoPriority#PROD-1:RECO(Stripp): priority#2}}' )
recoCPU = '{{RecoMaxCPUTime#PROD-1:RECO(Stripp): Max CPU time in secs#1000000}}'
recoPlugin = '{{RecoPluginType#PROD-1:RECO(Stripp): production plugin name#RAWProcessing}}'
recoFilesPerJob = '{{RecoFilesPerJob#PROD-1:RECO(Stripp): Group size or number of files per job#1}}'
recoDataSE = '{{RecoDataSE#PROD-1:RECO(Stripp): Output Data Storage Element#Tier1-BUFFER}}'
try:
  recoDataSESpecial = eval( '{{RecoDataSESpecial#PROD-1:RECO(Stripp): Special SE (a dictionary Type:SE)#}}' )
except SyntaxError:
  recoDataSESpecial = {}
recoType = '{{RecoType#PROD-1:RECO(Stripp): DataReconstruction or DataReprocessing#DataReconstruction}}'
recoIDPolicy = '{{recoIDPolicy#PROD-1:RECO(Stripp): policy for input data access (download or protocol)#download}}'
recoMulticoreFlag = '{{recoMulticoreFLag#PROD-1: multicore flag#True}}'
recoAncestorDepth = int( '{{recoAncestorDepth#PROD-1: Ancestor Depth#0}}' )

# stripp params
strippPriority = int( '{{priority#PROD-2:Stripping: priority#5}}' )
strippCPU = '{{StrippMaxCPUTime#PROD-2:Stripping: Max CPU time in secs#1000000}}'
strippPlugin = '{{StrippPluginType#PROD-2:Stripping: plugin name#ByRunWithFlush}}'
strippFilesPerJob = '{{StrippFilesPerJob#PROD-2:Stripping: Group size or number of files per job#2}}'
strippDataSE = '{{StrippStreamSE#PROD-2:Stripping: output data SE (un-merged streams)#Tier1-BUFFER}}'
try:
  strippDataSESpecial = eval( '{{StrippDataSESpecial#PROD-2:Stripping: Special SE (a dictionary Type:SE)#}}' )
except SyntaxError:
  strippDataSESpecial = {}
strippIDPolicy = '{{strippIDPolicy#PROD-2:Stripping: policy for input data access (download or protocol)#download}}'
strippMulticoreFlag = '{{strippMulticoreFLag#PROD-2: multicore flag#True}}'
strippAncestorDepth = int( '{{strippAncestorDepth#PROD-2: Ancestor Depth#0}}' )

# merging params
mergingPriority = int( '{{MergePriority#PROD-3:Merging: priority#8}}' )
mergingCPU = '{{MergeMaxCPUTime#PROD-3:Merging: Max CPU time in secs#300000}}'
mergingPlugin = '{{MergePlugin#PROD-3:Merging: plugin#ByRunFileTypeSizeWithFlush}}'
mergingGroupSize = '{{MergeFileSize#PROD-3:Merging: Size (in GB) of the merged files#5}}'
mergingDataSE = '{{MergeStreamSE#PROD-3:Merging: output data SE (merged streams)#Tier1-DST}}'
try:
  mergingDataSESpecial = eval( '{{MergingDataSESpecial#PROD-3:Merging: Special SE (a dictionary Type:SE)#}}' )
except SyntaxError:
  mergingDataSESpecial = {}
mergingIDPolicy = '{{MergeIDPolicy#PROD-3:Merging: policy for input data access (download or protocol)#download}}'
mergingRemoveInputsFlag = '{{MergeRemoveFlag#PROD-3:Merging: remove input data flag True/False#True}}'
mergeMulticoreFlag = '{{mergeMulticoreFLag#PROD-3: multicore flag#True}}'

# indexing params
indexingPriority = int( '{{IndexingPriority#PROD-4:indexing: priority#5}}' )
indexingCPU = '{{IndexingMaxCPUTime#PROD-4:indexing: Max CPU time in secs#50000}}'
indexingPlugin = '{{IndexingPlugin#PROD-4:indexing: plugin#ByRunFileTypeSizeWithFlush}}'
indexingGroupSize = '{{IndexingFileSize#PROD-4:indexing: Size (in GB) of the Indexed files#50}}'
indexingDataSE = '{{IndexingStreamSE#PROD-4:indexing: output data SE (Indexed streams)#IndexerSE}}'
indexingIDPolicy = '{{IndexingIDPolicy#PROD-4:indexing: policy for input data access (download or protocol)#protocol}}'

pr.requestID = '{{ID}}'
pr.prodGroup = '{{inProPass}}' + '/' + '{{pDsc}}'
# used in case of a test e.g. certification etc.
pr.configName = '{{configName}}'
pr.configVersion = '{{configVersion}}'
# Other parameters from the request page
pr.dqFlag = '{{inDataQualityFlag}}'  # UNCHECKED
pr.dataTakingConditions = '{{simDesc}}'
pr.processingPass = '{{inProPass}}'
pr.bkFileType = '{{inFileType}}'
pr.eventType = '{{eventType}}'
pr.visibility = 'Yes'

w1 = eval( w1 )
w2 = eval( w2 )
w3 = eval( w3 )
w4 = eval( w4 )
w5 = eval( w5 )

certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
validationFlag = eval( validationFlag )

if extraOptions:
  pr.extraOptions = eval( extraOptions )
mergeRemoveInputsFlag = eval( mergingRemoveInputsFlag )

pr.resolveSteps()

if not w1 and not w2 and not w3 and not w4:
  gLogger.error( 'I told you to select at least one workflow!' )
  DIRAC.exit( 2 )

if certificationFlag or localTestFlag:
  pr.testFlag = True
  if certificationFlag:
    pr.publishFlag = True
  if localTestFlag:
    pr.publishFlag = False
    pr.prodsToLaunch = [1]
else:
  pr.publishFlag = True
  pr.testFlag = False

recoInputDataList = []
strippInputDataList = []
if not pr.publishFlag:
  # this is 1380Gev MagUp
  # recoTestData = 'LFN:/lhcb/data/2011/RAW/FULL/LHCb/COLLISION11/88162/088162_0000000020.raw'
  # this is collision11
  recoTestData = 'LFN:/lhcb/data/2011/RAW/FULL/LHCb/COLLISION11/89333/089333_0000000003.raw'
  recoInputDataList.append( recoTestData )
  recoIDPolicy = 'protocol'

  strippTestData = 'LFN:/lhcb/data/2010/SDST/00008375/0001/00008375_00016947_1.sdst'
  strippInputDataList.append( strippTestData )
#  strippTestDataRAW = 'LFN:/lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/75338/075338_0000000069.raw'
#  strippInputDataList.append( strippTestDataRAW )
  strippIDPolicy = 'protocol'
  evtsPerJob = '2000'


pr.outConfigName = pr.configName

# In case we want just to test, we publish in the certification/test part of the BKK
if pr.testFlag:
  pr.outConfigName = 'certification'
  pr.configVersion = 'test'
  pr.dataTakingConditions = 'Beam3500GeV-VeloClosed-MagUp'
  if w1 or w3:
    pr.events = ['25']
    pr.processingPass = 'Real Data'
    pr.bkFileType = 'RAW'
  else:
    pr.events = ['2000']
    pr.processingPass = 'Real Data/Reco12'
    pr.bkFileType = 'SDST'
  mergingGroupSize = '1'
  recoCPU = strippCPU = '200000'
  pr.startRun = '93718'
  pr.endRun = '93720'
  pr.eventType = '90000000'
  pr.dqFlag = 'ALL'

if validationFlag:
  pr.outConfigName = 'validation'

if w1:
  pr.prodsTypeList = [recoType]
  pr.outputSEs = [recoDataSE]
  pr.specialOutputSEs = [recoDataSESpecial]
  pr.stepsInProds = [range( 1, len( pr.stepsList ) + 1 )]
  pr.removeInputsFlags = [False]
  pr.priorities = [recoPriority]
  pr.cpus = [recoCPU]
  pr.groupSizes = [recoFilesPerJob]
  pr.plugins = [recoPlugin]
  pr.inputs = [recoInputDataList]
  pr.inputDataPolicies = [recoIDPolicy]
  pr.bkQueries = ['Full']
  pr.targets = [targetSite]
  pr.multicore = [recoMulticoreFlag]
  pr.outputModes = ['Run']
  pr.ancestorDepths = [recoAncestorDepth]

elif w2:
  pr.prodsTypeList = ['DataStripping', 'Merge']
  pr.outputSEs = [strippDataSE, mergingDataSE]
  pr.specialOutputSEs = [strippDataSESpecial, mergingDataSESpecial]
  pr.stepsInProds = [range( 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, mergeRemoveInputsFlag]
  pr.priorities = [strippPriority, mergingPriority]
  pr.cpus = [strippCPU, mergingCPU]
  pr.groupSizes = [strippFilesPerJob, mergingGroupSize]
  pr.plugins = [strippPlugin, mergingPlugin]
  pr.inputs = [strippInputDataList, []]
  pr.inputDataPolicies = [strippIDPolicy, mergingIDPolicy]
  pr.bkQueries = ['Full', 'fromPreviousProd']
  pr.targets = [targetSite, targetSite]
  pr.multicore = [strippMulticoreFlag, mergeMulticoreFlag]
  pr.outputModes = ['Run', 'Run']
  pr.ancestorDepths = [strippAncestorDepth, 0]

elif w3:
  pr.prodsTypeList = [recoType, 'Merge']
  pr.outputSEs = [recoDataSE, mergingDataSE]
  pr.specialOutputSEs = [recoDataSESpecial, mergingDataSESpecial]
  pr.stepsInProds = [range( 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, mergeRemoveInputsFlag]
  pr.priorities = [recoPriority, mergingPriority]
  pr.cpus = [recoCPU, mergingCPU]
  pr.groupSizes = [recoFilesPerJob, mergingGroupSize]
  pr.plugins = [recoPlugin, mergingPlugin]
  pr.inputs = [recoInputDataList, []]
  pr.inputDataPolicies = [recoIDPolicy, mergingIDPolicy]
  pr.bkQueries = ['Full', 'fromPreviousProd']
  pr.targets = [targetSite, targetSite]
  pr.multicore = [recoMulticoreFlag, mergeMulticoreFlag]
  pr.outputModes = ['Run', 'Run']
  pr.ancestorDepths = [recoAncestorDepth, 0]

elif w4:
  pr.prodsTypeList = [recoType, 'DataStripping', 'Merge']
  pr.outputSEs = [recoDataSE, strippDataSE, mergingDataSE]
  pr.specialOutputSEs = [recoDataSESpecial, strippDataSESpecial, mergingDataSESpecial]
  pr.stepsInProds = [range( 1, len( pr.stepsList ) - 1 ),
                     xrange( len( pr.stepsList ) - 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, False, mergeRemoveInputsFlag]
  pr.priorities = [recoPriority, strippPriority, mergingPriority]
  pr.cpus = [recoCPU, strippCPU, mergingCPU]
  pr.groupSizes = [recoFilesPerJob, strippFilesPerJob, mergingGroupSize]
  pr.plugins = [recoPlugin, strippPlugin, mergingPlugin]
  pr.inputs = [recoInputDataList, [], []]
  pr.inputDataPolicies = [recoIDPolicy, strippIDPolicy, mergingIDPolicy]
  pr.bkQueries = ['Full', 'fromPreviousProd', 'fromPreviousProd']
  pr.targets = [targetSite, targetSite, targetSite]
  pr.multicore = [recoMulticoreFlag, strippMulticoreFlag, mergeMulticoreFlag]
  pr.outputModes = ['Run', 'Run', 'Run']
  pr.ancestorDepths = [recoAncestorDepth, strippAncestorDepth, 0]

elif w5:
  pr.prodsTypeList = ['DataStripping', 'Merge', 'WGProduction']
  pr.outputSEs = [strippDataSE, mergingDataSE, indexingDataSE]
  pr.specialOutputSEs = [strippDataSESpecial, mergingDataSESpecial, {}]
  pr.stepsInProds = [range( 1, len( pr.stepsList ) - 1 ),
                     xrange( len( pr.stepsList ) - 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, mergeRemoveInputsFlag, False]
  pr.priorities = [strippPriority, mergingPriority, indexingPriority]
  pr.cpus = [strippCPU, mergingCPU, indexingCPU]
  pr.groupSizes = [strippFilesPerJob, mergingGroupSize, indexingGroupSize]
  pr.plugins = [strippPlugin, mergingPlugin, indexingPlugin]
  pr.inputs = [strippInputDataList, [], []]
  pr.inputDataPolicies = [strippIDPolicy, mergingIDPolicy, indexingIDPolicy]
  pr.bkQueries = ['Full', 'fromPreviousProd', 'fromPreviousProd']
  pr.targets = [targetSite, targetSite, targetSite]
  pr.multicore = [strippMulticoreFlag, mergeMulticoreFlag, False]
  pr.outputModes = ['Run', 'Run', 'Any']
  pr.ancestorDepths = [strippAncestorDepth, 0, 0]

pr.buildAndLaunchRequest()
