"""  The MC Simulation Template creates workflows for the following simulation
     use-cases:
      WORKFLOW1: Simulation+MCMerge+Selection+Merge
      WORKFLOW2: Simulation+MCMerge+Selection
      WORKFLOW3: Simulation+MCMerge
      WORKFLOW4: Simulation

"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

from DIRAC import gLogger

from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

Script.parseCommandLine()
gLogger = gLogger.getSubLogger( 'MCSimulation_run.py' )

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
w1 = '{{w1#-WORKFLOW1: Simulation+Selection+Merge#False}}'
w2 = '{{w2#-WORKFLOW2: Simulation+Selection#False}}'
w3 = '{{w3#-WORKFLOW3: Simulation+MCMerge#False}}'
w4 = '{{w4#-WORKFLOW4: Simulation#False}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True for validation prod#False}}'

pr.configName = '{{BKConfigName#GENERAL: BK configuration name e.g. MC #MC}}'
pr.configVersion = '{{BKConfigVersion#GENERAL: BK configuration version e.g. MC09, 2009, 2010#MC11a}}'
outputFileMask = '{{WorkflowOutputDataFileMask#GENERAL: Workflow file extensions to save (comma separated) e.g. DST,DIGI#ALLSTREAMS.DST}}'

pr.events = '{{MCNumberOfEvents#GENERAL: Number of events per job#100}}'
pr.sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc43-opt, ANY#i686-slc5-gcc43-opt}}'

targets = '{{Target#PROD-MC: Target for MC (e.g. Tier2, ALL, LCG.CERN.ch#Tier2}}'
MCCpu = '{{MCMaxCPUTime#PROD-MC: Max CPU time in secs#1000000}}'
MCPriority = '{{MCPriority#PROD-MC: Production priority#0}}'
pr.extend = '{{MCExtend#PROD-MC: extend production by this many jobs#100}}'
pr.extraOptions = '{{extraOptions#GENERAL: extra options as python dict stepNumber:options#}}'

selectionPlugin = '{{selectionPlugin#PROD-Selection: plugin e.g. Standard, BySize#BySize}}'
selectionGroupSize = '{{selectionGroupSize#PROD-Selection: input files total size (we\'ll use protocol access)#20}}'
selectionPriority = '{{selectionPriority#PROD-Selection: Job Priority e.g. 8 by default#6}}'
selectionCPU = '{{selectionCPU#PROD-Selection: Max CPU time in secs#100000}}'
removeInputSelection = '{{removeInputSelection#PROD-Selection: remove inputs#True}}'

mergingPlugin = '{{MergingPlugin#PROD-Merging: plugin e.g. Standard, BySize#BySize}}'
mergingGroupSize = '{{MergingGroupSize#PROD-Merging: Group Size e.g. BySize = GB file size#5}}'
mergingPriority = '{{MergingPriority#PROD-Merging: Job Priority e.g. 8 by default#8}}'
mergingCPU = '{{mergingCPU#PROD-Merging: Max CPU time in secs#100000}}'
removeInputMerge = '{{removeInputMerge#PROD-Merging: remove inputs#True}}'

#replicationFlag = '{{TransformationEnable#PROD-Replication: flag Boolean True/False#True}}'
#replicationPlugin = '{{ReplicationPlugin#PROD-Replication: ReplicationPlugin#LHCbMCDSTBroadcastRandom}}'

pr.eventType = '{{eventType}}'
#Often MC requests are defined with many subrequests but we want to retain
#the parent ID for viewing on the production monitoring page. If a parent
#request is defined then this is used.
parentReq = '{{_parent}}'
eventNumberTotal = '{{EventNumberTotal}}'

if not parentReq:
  pr.requestID = '{{ID}}'
else:
  pr.requestID = parentReq

pr.prodGroup = '{{pDsc}}'
pr.dataTakingConditions = '{{simDesc}}'

MCPriority = int( MCPriority )
selectionPriority = int( selectionPriority )
mergingPriority = int( mergingPriority )

###########################################
# LHCb conventions implied by the above
###########################################

certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )

if certificationFlag or localTestFlag:
  pr.testFlag = True
  if certificationFlag:
    pr.publishFlag = True
  if localTestFlag:
    pr.publishFlag = False

#In case we want just to test, we publish in the certification/test part of the BKK
if pr.testFlag:
  pr.configName = 'certification'
  pr.configVersion = 'test'
  pr.events = '3'
  pr.extend = '10'
  mergingGroupSize = '1'
  MCCpu = '50000'

w1 = eval( w1 )
w2 = eval( w2 )
w3 = eval( w3 )
w4 = eval( w4 )

if not w1 and not w2 and not w3 and not w4:
  gLogger.error( 'Vladimir, I told you to select at least one workflow!' )

if w1:
  pr.prodsTypeList = ['MCSimulation', 'DataStripping', 'Merge']
  pr.outputSEs = ['Tier1_MC-DST', 'Tier1_MC-DST', 'Tier1_MC_M-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) - 1 ),
                     range( len( pr.stepsList ) - 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, removeInputSelection, removeInputMerge]
  pr.priorities = [MCPriority, selectionPriority, mergingPriority]
  pr.CPUs = [MCCpu, selectionCPU, mergingCPU]
  pr.outputFileMasks = [outputFileMask, '', '']
  pr.targets = [targets, '', '']
  pr.groupSizes = [1, selectionGroupSize, mergingGroupSize]
  pr.plugins = ['', selectionPlugin, mergingPlugin]
elif w2:
  pr.prodsTypeList = ['MCSimulation', 'DataStripping']
  pr.outputSEs = ['Tier1_MC-DST', 'Tier1_MC-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, removeInputSelection]
  pr.priorities = [MCPriority, selectionPriority]
  pr.CPUs = [MCCpu, selectionCPU]
  pr.outputFileMasks = [outputFileMask, '']
  pr.targets = [targets, '']
  pr.groupSizes = [1, selectionGroupSize]
  pr.plugins = ['', selectionPlugin]
elif w3:
  pr.prodsTypeList = ['MCSimulation', 'Merge']
  pr.outputSEs = ['Tier1_MC-DST', 'Tier1_MC_M-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, removeInputMerge]
  pr.priorities = [MCPriority, mergingPriority]
  pr.CPUs = [MCCpu, mergingCPU]
  pr.outputFileMasks = [outputFileMask, '']
  pr.targets = [targets, '']
  pr.groupSizes = [1, mergingGroupSize]
  pr.plugins = ['', mergingPlugin]
elif w4:
  pr.prodsTypeList = ['MCSimulation']
  pr.outputSEs = ['Tier1_MC-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) + 1 )]
  pr.removeInputsFlags = [False]
  pr.priorities = [MCPriority]
  pr.CPUs = [MCCpu]
  pr.outputFileMasks = [outputFileMask]
  pr.targets = [targets]
  pr.groupSizes = [1]
  pr.plugins = ['']

pr.buildAndLaunchRequest()

