"""  The MC Simulation Template creates workflows for the following simulation
     use-cases:
      WORKFLOW1: Simulation+Selection+Merge
      WORKFLOW2: Simulation+Selection+MCMerge
      WORKFLOW3: Simulation+Selection
      WORKFLOW4: Simulation+MCMerge
      WORKFLOW5: Simulation

    Exotic things you might want to do:
    * run a local test:
      - of the MC: just set the localTestFlag to True
      - of the merging/stripping: set pr.prodsToLaunch to, e.g., [2], and adjust the pr.inputs at the end of the script
    * run only part of the request on the Grid:
      - for the MC: just set pr.prodsToLaunch = [1]
      - for the merge and/or stripping: set pr.prodsToLaunch, then set pr.previousProdID
"""

import ast

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import gConfig, gLogger, exit as DIRACexit
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

__RCSID__ = "$Id$"

gLogger = gLogger.getSubLogger( 'MCSimulation_run.py' )
currentSetup = gConfig.getValue( 'DIRAC/Setup' )

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
stepsList.append( '{{p10Step}}' )
stepsList.append( '{{p11Step}}' )
stepsList.append( '{{p12Step}}' )
stepsList.append( '{{p13Step}}' )
stepsList.append( '{{p14Step}}' )
stepsList.append( '{{p15Step}}' )
stepsList.append( '{{p16Step}}' )
stepsList.append( '{{p17Step}}' )
stepsList.append( '{{p18Step}}' )
stepsList.append( '{{p19Step}}' )
stepsList.append( '{{p20Step}}' )
pr.stepsList = stepsList

###########################################
# Configurable and fixed parameters
###########################################

pr.appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

w = '{{w#----->WORKFLOW: choose one below#}}'
w1 = '{{w1#-WORKFLOW1: Simulation+Selection+Merge#False}}'
w2 = '{{w2#-WORKFLOW2: Simulation(up to Moore)+Selection+Merge#False}}'
w3 = '{{w3#-WORKFLOW3: Simulation(up to Moore)+Selection(-->avoid merge)#False}}'
w4 = '{{w4#-WORKFLOW4: Simulation+MCMerge#False}}'
w5 = '{{w5#-WORKFLOW5: Simulation#False}}'

localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True for validation prod - will create histograms#False}}'

pr.configName = '{{BKConfigName#GENERAL: BK configuration name e.g. MC #MC}}'
extraOptions = '{{extraOptions#GENERAL: extra options as python dict stepID:options#}}'

targets = '{{Target#PROD-1:MC: Target for MC (e.g. Tier2, ALL, LCG.CERN.ch#Tier2}}'
MCPriority = '{{MCPriority#PROD-1:MC: Production priority#0}}'
MCmulticoreFlag = '{{MCMulticoreFLag#PROD-1: multicore flag#True}}'

selectionPlugin = '{{selectionPlugin#PROD-2:Selection: plugin e.g. Standard, BySize#BySize}}'
selectionGroupSize = '{{selectionGroupSize#PROD-2:Selection: input files total size (we\'ll download)#20}}'
selectionPriority = '{{selectionPriority#PROD-2:Selection: Job Priority e.g. 8 by default#6}}'
selectionCPU = '{{selectionCPU#PROD-2:Selection: Max CPU time in secs#100000}}'
removeInputSelection = '{{removeInputSelection#PROD-2:Selection: remove inputs#True}}'
selmulticoreFlag = '{{selMulticoreFLag#PROD-2: multicore flag#True}}'

mergingPlugin = '{{MergingPlugin#PROD-3:Merging: plugin e.g. Standard, BySize#BySize}}'
mergingGroupSize = '{{MergingGroupSize#PROD-3:Merging: Group Size e.g. BySize = GB file size#5}}'
mergingPriority = '{{MergingPriority#PROD-3:Merging: Job Priority e.g. 8 by default#8}}'
mergingCPU = '{{mergingCPU#PROD-3:Merging: Max CPU time in secs#100000}}'
removeInputMerge = '{{removeInputMerge#PROD-3:Merging: remove inputs#True}}'
mergemulticoreFlag = '{{mergeMulticoreFLag#PROD-3: multicore flag#True}}'

pr.configVersion = '{{mcConfigVersion}}'
pr.eventType = '{{eventType}}'
# Often MC requests are defined with many subrequests but we want to retain
# the parent ID for viewing on the production monitoring page. If a parent
# request is defined then this is used.
pr.parentRequestID = '{{_parent}}'
pr.requestID = '{{ID}}'

if extraOptions:
  pr.extraOptions = ast.literal_eval( extraOptions )
pr.prodGroup = '{{pDsc}}'
pr.dataTakingConditions = '{{simDesc}}'

MCPriority = int( MCPriority )
selectionPriority = int( selectionPriority )
mergingPriority = int( mergingPriority )

removeInputMerge = ast.literal_eval( removeInputMerge )
removeInputSelection = ast.literal_eval( removeInputSelection )

pr.resolveSteps()

###########################################
# LHCb conventions implied by the above
###########################################

localTestFlag = ast.literal_eval( localTestFlag )
validationFlag = ast.literal_eval( validationFlag )

if localTestFlag:
  pr.testFlag = True
  pr.publishFlag = False
  pr.prodsToLaunch = [1]

pr.outConfigName = pr.configName

w1 = ast.literal_eval( w1 )
w2 = ast.literal_eval( w2 )
w3 = ast.literal_eval( w3 )
w4 = ast.literal_eval( w4 )
w5 = ast.literal_eval( w5 )

if not w1 and not w2 and not w3 and not w4 and not w5:
  gLogger.error( 'Vladimir, I told you to select at least one workflow!' )
  DIRACexit( 2 )

if w1:
  pr.prodsTypeList = ['MCSimulation', 'MCStripping', 'MCMerge']
  pr.outputSEs = ['Tier1-BUFFER', 'Tier1-BUFFER', 'Tier1_MC-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) - 1 ),
                     xrange( len( pr.stepsList ) - 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, removeInputSelection, removeInputMerge]
  pr.priorities = [MCPriority, selectionPriority, mergingPriority]
  pr.cpus = [100000, selectionCPU, mergingCPU]
  pr.outputFileSteps = [str( len( pr.stepsListDict ) - 2 ), '', '']
  pr.targets = [targets, '', '']
  pr.groupSizes = [1, selectionGroupSize, mergingGroupSize]
  pr.plugins = ['', selectionPlugin, mergingPlugin]
  pr.inputDataPolicies = ['', 'download', 'download']
  pr.bkQueries = ['', 'fromPreviousProd', 'fromPreviousProd']
  pr.multicore = [MCmulticoreFlag, selmulticoreFlag, mergemulticoreFlag]

elif w2:
  pr.prodsTypeList = ['MCSimulation', 'MCReconstruction', 'MCMerge']
  pr.outputSEs = ['Tier1-BUFFER', 'Tier1-BUFFER', 'Tier1_MC-DST']

  brunelStepIndex = 1
  for sld in pr.stepsListDict:
    if sld['ApplicationName'].lower() == 'brunel':
      break
    brunelStepIndex += 1

  pr.stepsInProds = [range( 1, brunelStepIndex ),
                     xrange( brunelStepIndex, len( pr.stepsListDict ) ),
                     [len( pr.stepsListDict )]]
  pr.outputFileSteps = [str( len( pr.stepsInProds[0] ) ),
                        str( len( pr.stepsInProds[1] ) ),
                        '1']

  pr.removeInputsFlags = [False, removeInputSelection, removeInputMerge]
  pr.priorities = [MCPriority, selectionPriority, mergingPriority]
  pr.cpus = [100000, selectionCPU, mergingCPU]
  pr.targets = [targets, '', '']
  pr.groupSizes = [1, selectionGroupSize, mergingGroupSize]
  pr.plugins = ['', selectionPlugin, mergingPlugin]
  pr.inputDataPolicies = ['', 'download', 'download']
  pr.bkQueries = ['', 'fromPreviousProd', 'fromPreviousProd']
  pr.multicore = [MCmulticoreFlag, selmulticoreFlag, mergemulticoreFlag]

elif w3:
  pr.prodsTypeList = ['MCSimulation', 'MCReconstruction']
  pr.outputSEs = ['Tier1-BUFFER', 'Tier1_MC-DST']

  if pr.stepsListDict[-1]['ApplicationName'].lower() == 'lhcb':
    gLogger.error( "This request contains a merge step, I can't submit it with this workflow" )
    DIRACexit( 2 )

  brunelStepIndex = 1
  for sld in pr.stepsListDict:
    if sld['ApplicationName'].lower() == 'brunel':
      break
    brunelStepIndex += 1

  pr.stepsInProds = [range( 1, brunelStepIndex ),
                     xrange( brunelStepIndex, len( pr.stepsListDict ) + 1 )]
  pr.outputFileSteps = [str( len( pr.stepsInProds[0] ) ),
                        str( len( pr.stepsInProds[1] ) )]

  pr.removeInputsFlags = [False, removeInputSelection]
  pr.priorities = [MCPriority, selectionPriority]
  pr.cpus = [100000, selectionCPU]
  pr.targets = [targets, '']
  pr.groupSizes = [1, selectionGroupSize]
  pr.plugins = ['', selectionPlugin]
  pr.inputDataPolicies = ['', 'download']
  pr.bkQueries = ['', 'fromPreviousProd']
  pr.multicore = [MCmulticoreFlag, selmulticoreFlag]

elif w4:
  pr.prodsTypeList = ['MCSimulation', 'MCMerge']
  pr.outputSEs = ['Tier1-BUFFER', 'Tier1_MC-DST']
  pr.stepsInProds = [range( 1, len( pr.stepsList ) ),
                     [len( pr.stepsList )]]
  pr.removeInputsFlags = [False, removeInputMerge]
  pr.priorities = [MCPriority, mergingPriority]
  pr.cpus = [100000, mergingCPU]
  pr.outputFileSteps = [str( len( pr.stepsListDict ) - 1 ), '']
  pr.targets = [targets, '']
  pr.groupSizes = [1, mergingGroupSize]
  pr.plugins = ['', mergingPlugin]
  pr.inputDataPolicies = ['', 'download']
  pr.bkQueries = ['', 'fromPreviousProd']
  pr.multicore = [MCmulticoreFlag, mergemulticoreFlag]

elif w5:
  pr.prodsTypeList = ['MCSimulation']
  pr.outputSEs = ['Tier1_MC-DST']
  if pr.stepsListDict[-1]['ApplicationName'].lower() == 'lhcb':
    gLogger.error( "This request contains a merge step, I can't submit it with this workflow" )
    DIRACexit( 2 )

  pr.stepsInProds = [range( 1, len( pr.stepsList ) + 1 )]
  pr.removeInputsFlags = [False]
  pr.priorities = [MCPriority]
  pr.cpus = [100000]
  pr.outputFileSteps = [str( len( pr.stepsListDict ) )]
  pr.targets = [targets]
  pr.groupSizes = [1]
  pr.plugins = ['']
  pr.inputDataPolicies = ['']
  pr.bkQueries = ['']
  pr.multicore = [MCmulticoreFlag]

# In case we want just to test, we publish in the certification/test part of the BKK
if currentSetup == 'LHCb-Certification' or pr.testFlag:
  pr.outConfigName = 'certification'
  pr.configVersion = 'test'

if pr.testFlag:
  pr.extend = '10'
  mergingGroupSize = '1'
  MCCpu = '50000'
  pr.previousProdID = 0  # set this for, e.g., launching only merging

# Validation implies few things, like saving all the outputs, and adding a GAUSSHIST
if validationFlag:
  pr.outConfigName = 'validation'
  # Adding GAUSSHIST to the list of outputs to be produced (by the first step, which is Gauss)
  if 'GAUSSHIST' not in pr.stepsListDict[0]['fileTypesOut']:
    pr.stepsListDict[0]['fileTypesOut'].append( 'GAUSSHIST' )
  pr.outputFileSteps = [''] * len( pr.prodsTypeList )


res = pr.buildAndLaunchRequest()
if not res['OK']:
  gLogger.error( "Errors with submission: %s" % res['Message'] )
  DIRACexit( 2 )
else:
  gLogger.always( "Submitted %s" % str( res['Value'] ) )
