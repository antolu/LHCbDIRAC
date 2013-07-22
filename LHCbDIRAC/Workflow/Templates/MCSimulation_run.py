"""  The MC Simulation Template creates workflows for the following simulation
     use-cases:
      WORKFLOW1: Simulation+MCMerge+Selection+Merge
      WORKFLOW2: Simulation+MCMerge+Selection
      WORKFLOW3: Simulation+MCMerge
      WORKFLOW4: Simulation

    Exotic things you might want to do:
    * run a local test:
      - of the MC: just set the localTestFlag to True
      - of the merging/stripping: set pr.prodsToLaunch to, e.g., [2], and adjust the pr.inputs at the end of the script
    * run only part of the request on the Grid:
      - for the MC: just set pr.prodsToLaunch = [1]
      - for the merge and/or stripping: set pr.prodsToLaunch, then set pr.previousProdID
"""

__RCSID__ = "$Id: MCSimulation_run.py 53649 2012-06-25 12:23:13Z fstagni $"

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import DIRAC
from DIRAC import gLogger
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

gLogger = gLogger.getSubLogger( 'MCSimulation_run.py' )

pr = ProductionRequest()

stepsList = ['{{p1Step}}']
stepsList.append( '{{p2Step}}' )
stepsList.append( '{{p3Step}}' )
stepsList.append( '{{p4Step}}' )
stepsList.append( '{{p5Step}}' )
stepsList.append( '{{p6Step}}' )
stepsList.append( '{{p7Step}}' )
stepsList.append( '{{p8Step}}' )
stepsList.append( '{{p9Step}}' )
pr.stepsList = stepsList

pr.resolveSteps()

###########################################
# Configurable and fixed parameters
###########################################

pr.appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

w = '{{w#----->WORKFLOW: choose one below#}}'
w1 = '{{w1#-WORKFLOW1: Simulation+Selection+Merge#False}}'
w2 = '{{w2#-WORKFLOW2: Simulation(up to Moore)+Selection+Merge#False}}'
w3 = '{{w3#-WORKFLOW3: Simulation(up to Moore)+Selection(-->avoid merge)#False}}'
w4 = '{{w4#-WORKFLOW4: Simulation+Merge#False}}'
w5 = '{{w5#-WORKFLOW5: Simulation#False}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True for validation prod#False}}'

pr.configName = '{{BKConfigName#GENERAL: BK configuration name e.g. MC #MC}}'
pr.configVersion = '{{mcConfigVersion#GENERAL: BK configuration version, e.g. MC10#2012}}'
extraOptions = '{{extraOptions#GENERAL: extra options as python dict stepNumber:options#}}'

CPUe = '{{MCCPUperEvent#GENERAL: CPU time per event#1.0}}'

targets = '{{Target#PROD-1:MC: Target for MC (e.g. Tier2, ALL, LCG.CERN.ch#Tier2}}'
MCCpu = '{{MCMaxCPUTime#PROD-1:MC: Max CPU time in secs#1000000}}'
MCPriority = '{{MCPriority#PROD-1:MC: Production priority#0}}'
pr.extend = '{{MCExtend#PROD-1:MC: extend production by this many jobs#100}}'
MCSysConfig = '{{MCSystemConfig#PROD-1:MC System config e.g. x86_64-slc5-gcc43-opt, ANY#x86_64-slc5-gcc46-opt}}'
MCmulticoreFlag = '{{MCMulticoreFLag#PROD-1: multicore flag#True}}'

selectionPlugin = '{{selectionPlugin#PROD-2:Selection: plugin e.g. Standard, BySize#BySize}}'
selectionGroupSize = '{{selectionGroupSize#PROD-2:Selection: input files total size (we\'ll download)#20}}'
selectionPriority = '{{selectionPriority#PROD-2:Selection: Job Priority e.g. 8 by default#6}}'
selectionCPU = '{{selectionCPU#PROD-2:Selection: Max CPU time in secs#100000}}'
removeInputSelection = '{{removeInputSelection#PROD-2:Selection: remove inputs#True}}'
selSysConfig = '{{selSystemConfig#PROD-2:Selection System config e.g. x86_64-slc5-gcc43-opt, ANY#x86_64-slc5-gcc46-opt}}'
selmulticoreFlag = '{{selMulticoreFLag#PROD-2: multicore flag#True}}'

mergingPlugin = '{{MergingPlugin#PROD-3:Merging: plugin e.g. Standard, BySize#BySize}}'
mergingGroupSize = '{{MergingGroupSize#PROD-3:Merging: Group Size e.g. BySize = GB file size#5}}'
mergingPriority = '{{MergingPriority#PROD-3:Merging: Job Priority e.g. 8 by default#8}}'
mergingCPU = '{{mergingCPU#PROD-3:Merging: Max CPU time in secs#100000}}'
removeInputMerge = '{{removeInputMerge#PROD-3:Merging: remove inputs#True}}'
mergeSysConfig = '{{mergeSystemConfig#PROD-3:Merging System config e.g. x86_64-slc5-gcc43-opt, ANY#x86_64-slc5-gcc46-opt}}'
mergemulticoreFlag = '{{mergeMulticoreFLag#PROD-3: multicore flag#True}}'

pr.eventType = '{{eventType}}'
# Often MC requests are defined with many subrequests but we want to retain
# the parent ID for viewing on the production monitoring page. If a parent
# request is defined then this is used.
pr.parentRequestID = '{{_parent}}'
pr.requestID = '{{ID}}'

if extraOptions:
  pr.extraOptions = eval( extraOptions )
pr.prodGroup = '{{pDsc}}'
pr.dataTakingConditions = '{{simDesc}}'

MCPriority = int( MCPriority )
selectionPriority = int( selectionPriority )
mergingPriority = int( mergingPriority )

removeInputMerge = eval( removeInputMerge )
removeInputSelection = eval( removeInputSelection )

###########################################
# LHCb conventions implied by the above
###########################################

certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
validationFlag = eval( validationFlag )

if certificationFlag or localTestFlag:
  pr.testFlag = True
  if certificationFlag:
    pr.publishFlag = True
  if localTestFlag:
    pr.publishFlag = False
    pr.prodsToLaunch = [1]

pr.outConfigName = pr.configName

# In case we want just to test, we publish in the certification/test part of the BKK
if pr.testFlag:
  pr.outConfigName = 'certification'
  pr.configVersion = 'test'
  pr.extend = '10'
  mergingGroupSize = '1'
  MCCpu = '50000'
  pr.previousProdID = 0  # set this for, e.g., launching only merging

if validationFlag:
  pr.outConfigName = 'validation'

w1 = eval( w1 )
w2 = eval( w2 )
w3 = eval( w3 )
w4 = eval( w4 )
w5 = eval( w5 )

if not w1 and not w2 and not w3 and not w4 and not w5:
  gLogger.error( 'Vladimir, I told you to select at least one workflow!' )
  DIRAC.exit( 2 )

if w1:
  pr.prodsTypeList = ['MCSimulation', 'MCStripping', 'Merge']
  pr.outputSEs = ['Tier1-BUFFER', 'Tier1-BUFFER', 'Tier1_MC-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) - 1 ),
                     range( len( pr.stepsList ) - 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, removeInputSelection, removeInputMerge]
  pr.priorities = [MCPriority, selectionPriority, mergingPriority]
  pr.cpus = [MCCpu, selectionCPU, mergingCPU]
  pr.outputFileSteps = [str( len( pr.stepsListDict ) - 2 ), '', '']
  pr.targets = [targets, '', '']
  pr.groupSizes = [1, selectionGroupSize, mergingGroupSize]
  pr.plugins = ['', selectionPlugin, mergingPlugin]
  pr.inputDataPolicies = ['', 'download', 'download']
  pr.CPUeList = [CPUe, '1.0', '1.0']
  pr.bkQueries = ['', 'fromPreviousProd', 'fromPreviousProd']
  pr.sysConfig = [MCSysConfig, selSysConfig, mergeSysConfig]
  pr.multicore = [MCmulticoreFlag, selmulticoreFlag, mergemulticoreFlag]

elif w2:
  pr.prodsTypeList = ['MCSimulation', 'MCReconstruction', 'Merge']
  pr.outputSEs = ['Tier1-BUFFER', 'Tier1-BUFFER', 'Tier1_MC-DST']

  mooreStepIndex = 1
  for sld in pr.stepsListDict:
    if sld['ApplicationName'].lower() == 'moore':
      break
    mooreStepIndex += 1

  pr.stepsInProds = [range( 1, mooreStepIndex + 1 ),
                     range( mooreStepIndex + 1, len( pr.stepsListDict ) ),
                     [len( pr.stepsListDict )]]
  pr.outputFileSteps = [str( len( pr.stepsInProds[0] ) ),
                        str( len( pr.stepsInProds[1] ) ),
                        '1']

  pr.removeInputsFlags = [False, removeInputSelection, removeInputMerge]
  pr.priorities = [MCPriority, selectionPriority, mergingPriority]
  pr.cpus = [MCCpu, selectionCPU, mergingCPU]
  pr.targets = [targets, '', '']
  pr.groupSizes = [1, selectionGroupSize, mergingGroupSize]
  pr.plugins = ['', selectionPlugin, mergingPlugin]
  pr.inputDataPolicies = ['', 'download', 'download']
  pr.CPUeList = [CPUe, '1.0', '1.0']
  pr.bkQueries = ['', 'fromPreviousProd', 'fromPreviousProd']
  pr.sysConfig = [MCSysConfig, selSysConfig, mergeSysConfig]
  pr.multicore = [MCmulticoreFlag, selmulticoreFlag, mergemulticoreFlag]

elif w3:
  pr.prodsTypeList = ['MCSimulation', 'MCReconstruction']
  pr.outputSEs = ['Tier1-BUFFER', 'Tier1_MC-DST']

  if pr.stepsListDict[-1]['ApplicationName'].lower() == 'lhcb':
    gLogger.error( "This request contains a merge step, I can't submit it with this workflow" )
    DIRAC.exit( 2 )

  mooreStepIndex = 1
  for sld in pr.stepsListDict:
    if sld['ApplicationName'].lower() == 'moore':
      break
    mooreStepIndex += 1

  pr.stepsInProds = [range( 1, mooreStepIndex + 1 ),
                     range( mooreStepIndex + 1, len( pr.stepsListDict ) + 1 )]
  pr.outputFileSteps = [str( len( pr.stepsInProds[0] ) ),
                        str( len( pr.stepsInProds[1] ) )]

  pr.removeInputsFlags = [False, removeInputSelection]
  pr.priorities = [MCPriority, selectionPriority]
  pr.cpus = [MCCpu, selectionCPU]
  pr.targets = [targets, '']
  pr.groupSizes = [1, selectionGroupSize]
  pr.plugins = ['', selectionPlugin]
  pr.inputDataPolicies = ['', 'download']
  pr.CPUeList = [CPUe, '1.0']
  pr.bkQueries = ['', 'fromPreviousProd']
  pr.sysConfig = [MCSysConfig, selSysConfig]
  pr.multicore = [MCmulticoreFlag, selmulticoreFlag]

elif w4:
  pr.prodsTypeList = ['MCSimulation', 'Merge']
  pr.outputSEs = ['Tier1-BUFFER', 'Tier1_MC-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, removeInputMerge]
  pr.priorities = [MCPriority, mergingPriority]
  pr.cpus = [MCCpu, mergingCPU]
  pr.outputFileSteps = [str( len( pr.stepsListDict ) - 1 ), '']
  pr.targets = [targets, '']
  pr.groupSizes = [1, mergingGroupSize]
  pr.plugins = ['', mergingPlugin]
  pr.inputDataPolicies = ['', 'download']
  pr.CPUeList = [CPUe, '1.0']
  pr.bkQueries = ['', 'fromPreviousProd']
  pr.sysConfig = [MCSysConfig, mergeSysConfig]
  pr.multicore = [MCmulticoreFlag, mergemulticoreFlag]

elif w5:
  pr.prodsTypeList = ['MCSimulation']
  pr.outputSEs = ['Tier1_MC-DST']
  if pr.stepsListDict[-1]['ApplicationName'].lower() == 'lhcb':
    gLogger.error( "This request contains a merge step, I can't submit it with this workflow" )
    DIRAC.exit( 2 )

  pr.stepsInProds = [range( 1, len( pr.stepsList ) + 1 )]
  pr.removeInputsFlags = [False]
  pr.priorities = [MCPriority]
  pr.cpus = [MCCpu]
  pr.outputFileSteps = [str( len( pr.stepsListDict ) )]
  pr.targets = [targets]
  pr.groupSizes = [1]
  pr.plugins = ['']
  pr.inputDataPolicies = ['']
  pr.CPUeList = [CPUe]
  pr.bkQueries = ['']
  pr.sysConfig = [MCSysConfig]
  pr.multicore = [MCmulticoreFlag]

res = pr.buildAndLaunchRequest()
if not res['OK']:
  gLogger.error( "Errors with submission: %s" % res['Message'] )
  DIRAC.exit( 2 )
else:
  gLogger.always( "Submitted %s" % str( res['Value'] ) )

gLogger.always( "##########################################################################################" )
gLogger.always( "##########################################################################################" )
gLogger.always( "REMINDER: no replication has been created!!! If you want one, please use the script" )
gLogger.always( "dirac-dms-add-replication --Plugin <yourPlugin> --Production <yourProd> --FileType <yourFileType' --Start" )
gLogger.always( "##########################################################################################" )
