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

__RCSID__ = "$Id$"

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
w2 = '{{w2#-WORKFLOW2: Simulation+Selection#False}}'
w3 = '{{w3#-WORKFLOW3: Simulation+MCMerge#False}}'
w4 = '{{w4#-WORKFLOW4: Simulation#False}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True for validation prod#False}}'

pr.configName = '{{BKConfigName#GENERAL: BK configuration name e.g. MC #MC}}'
pr.configVersion = '{{mcConfigVersion#GENERAL: BK configuration version, e.g. MC10#MC11a}}'
outputFileMask = '{{WorkflowOutputDataFileMask#GENERAL: Workflow file extensions to save (comma separated) e.g. DST,DIGI#ALLSTREAMS.DST}}'

pr.events = '{{MCNumberOfEvents#GENERAL: Number of events per job#100}}'
pr.sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc43-opt, ANY#i686-slc5-gcc43-opt}}'

targets = '{{Target#PROD-MC: Target for MC (e.g. Tier2, ALL, LCG.CERN.ch#Tier2}}'
MCCpu = '{{MCMaxCPUTime#PROD-MC: Max CPU time in secs#1000000}}'
MCPriority = '{{MCPriority#PROD-MC: Production priority#0}}'
pr.extend = '{{MCExtend#PROD-MC: extend production by this many jobs#100}}'
extraOptions = '{{extraOptions#GENERAL: extra options as python dict stepNumber:options#}}'

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

pr.eventType = '{{eventType}}'
#Often MC requests are defined with many subrequests but we want to retain
#the parent ID for viewing on the production monitoring page. If a parent
#request is defined then this is used.
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

if certificationFlag or localTestFlag:
  pr.testFlag = True
  if certificationFlag:
    pr.publishFlag = True
  if localTestFlag:
    pr.publishFlag = False
    pr.prodsToLaunch = [1]

#In case we want just to test, we publish in the certification/test part of the BKK
if pr.testFlag:
  pr.configName = 'certification'
  pr.configVersion = 'test'
  pr.events = '3'
  pr.extend = '10'
  mergingGroupSize = '1'
  MCCpu = '50000'
  pr.previousProdID = 0 #set this for, e.g., launching only merging

if validationFlag:
  pr.configName = 'validation'

w1 = eval( w1 )
w2 = eval( w2 )
w3 = eval( w3 )
w4 = eval( w4 )

if not w1 and not w2 and not w3 and not w4:
  gLogger.error( 'Vladimir, I told you to select at least one workflow!' )
  DIRAC.exit( 2 )

if w1:
  pr.prodsTypeList = ['MCSimulation', 'DataStripping', 'Merge']
  pr.outputSEs = ['Tier1_MC-DST', 'Tier1_MC-DST', 'Tier1_MC_M-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) - 1 ),
                     range( len( pr.stepsList ) - 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, removeInputSelection, removeInputMerge]
  pr.priorities = [MCPriority, selectionPriority, mergingPriority]
  pr.cpus = [MCCpu, selectionCPU, mergingCPU]
  pr.outputFileMasks = [outputFileMask, '', '']
  pr.targets = [targets, '', '']
  pr.groupSizes = [1, selectionGroupSize, mergingGroupSize]
  pr.plugins = ['', selectionPlugin, mergingPlugin]
  pr.inputDataPolicies = ['', 'protocol', 'download']
elif w2:
  pr.prodsTypeList = ['MCSimulation', 'DataStripping']
  pr.outputSEs = ['Tier1_MC-DST', 'Tier1_MC-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, removeInputSelection]
  pr.priorities = [MCPriority, selectionPriority]
  pr.cpus = [MCCpu, selectionCPU]
  pr.outputFileMasks = [outputFileMask, '']
  pr.targets = [targets, '']
  pr.groupSizes = [1, selectionGroupSize]
  pr.plugins = ['', selectionPlugin]
  pr.inputDataPolicies = ['', 'protocol']
elif w3:
  pr.prodsTypeList = ['MCSimulation', 'Merge']
  pr.outputSEs = ['Tier1_MC-DST', 'Tier1_MC_M-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, removeInputMerge]
  pr.priorities = [MCPriority, mergingPriority]
  pr.cpus = [MCCpu, mergingCPU]
  pr.outputFileMasks = [outputFileMask, '']
  pr.targets = [targets, '']
  pr.groupSizes = [1, mergingGroupSize]
  pr.plugins = ['', mergingPlugin]
  pr.inputDataPolicies = ['', 'download']
elif w4:
  pr.prodsTypeList = ['MCSimulation']
  pr.outputSEs = ['Tier1_MC-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) + 1 )]
  pr.removeInputsFlags = [False]
  pr.priorities = [MCPriority]
  pr.cpus = [MCCpu]
  pr.outputFileMasks = [outputFileMask]
  pr.targets = [targets]
  pr.groupSizes = [1]
  pr.plugins = ['']
  pr.inputDataPolicies = ['']

pr.inputs = [[]] * len( pr.prodsTypeList )

#In case of local test (these examples are for the merging)
if localTestFlag:
  pr.inputs = [[],
                 ['/lhcb/certification/test/ALLSTREAMS.DST/00000127/0000/00000127_00000030_5.AllStreams.dst',
                  '/lhcb/certification/test/ALLSTREAMS.DST/00000127/0000/00000127_00000030_5.AllStreams.dst']
                ]
  pr.inputDataPolicies = ['', 'protocol']

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
