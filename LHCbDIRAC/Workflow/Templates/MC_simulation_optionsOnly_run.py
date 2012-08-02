"""  The MC Simulation Template creates workflows for the following simulation
     use-cases:
      WORKFLOW1: Simulation+MCMerge+Selection+Merge
      WORKFLOW2: Simulation+MCMerge+Selection
      WORKFLOW3: Simulation+MCMerge
      WORKFLOW4: Simulation
     
"""

"""
Recipe:
. Put here only the options: OK
  the initializations should go out
  also the stepsList creation
. Get the list of Bk steps (in a dictionary) : OK
. Translate the Bk steps in production steps : OK
  use TemplatesUtilities._splitIntoProductionSteps() (ready and tested): OK
. Make the correlation between production steps and productions to be created: OK
  this would require user input
. Create the production objects requested
"""

__RCSID__ = "$Id$"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
import string
from DIRAC.Core.Base import Script
Script.parseCommandLine()

import DIRAC

import LHCbDIRAC.Workflow.Templates.TemplatesUtilities

from DIRAC import gLogger, gConfig
gLogger = gLogger.getSubLogger( 'MC_Simulation_run.py' )

#################################################################################
# Below here is the actual production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

###########################################
# Configurable and fixed parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

w = '{{w#----->WORKFLOW: choose one below#}}'
w1 = '{{w1#-WORKFLOW1: Simulation+Selection+Merge#False}}'
w2 = '{{w2#-WORKFLOW2: Simulation+Selection#False}}'
w3 = '{{w3#-WORKFLOW3: Simulation+MCMerge#False}}'
w4 = '{{w4#-WORKFLOW4: Simulation#False}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True for validation prod#False}}'

configName = '{{BKConfigName#GENERAL: BK configuration name e.g. MC #MC}}'
configVersion = '{{BKConfigVersion#GENERAL: BK configuration version e.g. MC09, 2009, 2010#MC11a}}'
outputFileMask = '{{WorkflowOutputDataFileMask#GENERAL: Workflow file extensions to save (comma separated) e.g. DST,DIGI#ALLSTREAMS.DST}}'

banTier1s = '{{WorkflowBanTier1s#GENERAL: Workflow ban Tier-1 sites for jobs Boolean True/False#True}}'
outputMode = '{{outputMode#GENERAL: Workflow upload workflow output#Any}}'
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc43-opt, ANY#i686-slc5-gcc43-opt}}'
targetSite = '{{TargetSite#GENERAL: Set a target site (blank for everything)#}}'

events = '{{MCNumberOfEvents#PROD-MC: Number of events per job#1000}}'
cpu = '{{MCMaxCPUTime#PROD-MC: Max CPU time in secs#1000000}}'
priority = '{{MCPriority#PROD-MC: Production priority#4}}'
extend = '{{MCExtend#PROD-MC: extend production by this many jobs#100}}'
extraOptions = '{{extraOptions#GENERAL: extra options as python dict stepNumber:options#}}'

selectionPlugin = '{{selectionPlugin#PROD-Selection: plugin e.g. Standard, BySize#BySize}}'
selectionGroupSize = '{{selectionGroupSize#PROD-Selection: input files total size (we\'ll use protocol access)#20}}'
selectionPriority = '{{selectionPriority#PROD-Selection: Job Priority e.g. 8 by default#6}}'
selectionExtraOptions = '{{selectionExtraOptions#PROD-Selection: selection extra options (leave blank for default)#}}'
selectionCPU = '{{selectionCPU#PROD-Selection: Max CPU time in secs#100000}}'
removeInputSelection = '{{removeInputSelection#PROD-Selection: remove inputs#True}}'

mergingPlugin = '{{MergingPlugin#PROD-Merging: plugin e.g. Standard, BySize#BySize}}'
mergingGroupSize = '{{MergingGroupSize#PROD-Merging: Group Size e.g. BySize = GB file size#5}}'
mergingPriority = '{{MergingPriority#PROD-Merging: Job Priority e.g. 8 by default#8}}'
mergingCPU = '{{mergingCPU#PROD-Merging: Max CPU time in secs#100000}}'
removeInputMerge = '{{removeInputMerge#PROD-Merging: remove inputs#True}}'

replicationFlag = '{{TransformationEnable#PROD-Replication: flag Boolean True/False#True}}'
replicationPlugin = '{{ReplicationPlugin#PROD-Replication: ReplicationPlugin#LHCbMCDSTBroadcastRandom}}'

evtType = '{{eventType}}'
#Often MC requests are defined with many subrequests but we want to retain
#the parent ID for viewing on the production monitoring page. If a parent
#request is defined then this is used.
requestID = '{{ID}}'
parentReq = '{{_parent}}'
eventNumberTotal = '{{EventNumberTotal}}'

if not parentReq:
  parentReq = requestID

###########################################
# LHCb conventions implied by the above
###########################################

replicationFlag = eval( replicationFlag )
banTier1s = eval( banTier1s )
certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
if extraOptions:
  extraOptions = eval( extraOptions )

w1 = eval( w1 )
w2 = eval( w2 )
w3 = eval( w3 )
w4 = eval( w4 )

if not w1 and not w2 and not w3 and not w4:
  gLogger.error( 'I told you to select at least one workflow!' )
  DIRAC.exit( 2 )

simulationTracking = 0
mergingTracking = 0
selectionTracking = 0

if w1:
  mergingTracking = 1
elif w2:
  selectionTracking = 1
elif w3:
  mergingTracking = 1
elif w4:
  simulationTracking = 1

if certificationFlag or localTestFlag:
  testFlag = True
  replicationFlag = False
  if certificationFlag:
    publishFlag = True
  if localTestFlag:
    publishFlag = False
    merging = False
    selection = False
else:
  publishFlag = True
  testFlag = False

diracProd = DiracProduction()

defaultOutputSE = 'Tier1_MC-DST'
mergedDataSE = 'Tier1_MC_M-DST'

BKscriptFlag = False
# If we don't even publish the production, we assume we want to see if the BK scripts are OK 
if not publishFlag:
  BKscriptFlag = True

#In case we want just to test, we publish in the certification/test part of the BKK
if testFlag:
  configName = 'certification'
  configVersion = 'test'
  events = '3'
  extend = '10'
  mergingGroupSize = '1'
  cpu = '50000'


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

simulationSteps = []
selectionSteps = []
mergingSteps = []

prodsList = []

if w1:
  simulationSteps = stepsDictList[:-2]
  selectionSteps = stepsDictList[-2:-1]
  if mergingPlugin == 'ByRunFileTypeSizeWithFlush':
    mergingSteps = stepsDictList[-1:]
  else:
    mergingSteps = LHCbDIRAC.Workflow.Templates.TemplatesUtilities._splitIntoProductionSteps( stepsDictList[-1:] )
  prodsList.append( ( 'MCSimulation', simulationSteps, None, False,
                      simulationTracking, defaultOutputSE, priority, cpu ) )
  prodsList.append( ( 'DataStripping', selectionSteps, 'fromPreviousProd', removeInputSelection,
                      selectionTracking, defaultOutputSE, selectionPriority, selectionCPU ) )
  for s in mergingSteps:
    prodsList.append( ( 'Merge', [s], 'fromPreviousProd', removeInputMerge,
                        mergingTracking, mergedDataSE, mergingPriority, mergingCPU ) )
elif w2:
  simulationSteps = stepsDictList[:-1]
  selectionSteps = stepsDictList[-1:]
  prodsList.append( ( 'MCSimulation', simulationSteps, None, False,
                      simulationTracking, defaultOutputSE, priority, cpu ) )
  prodsList.append( ( 'DataStripping', selectionSteps, 'fromPreviousProd', removeInputSelection,
                      selectionTracking, defaultOutputSE, selectionPriority, selectionCPU ) )
elif w3:
  simulationSteps = stepsDictList[:-1]
  if mergingPlugin == 'ByRunFileTypeSizeWithFlush':
    mergingSteps = stepsDictList[-1:]
  else:
    mergingSteps = LHCbDIRAC.Workflow.Templates.TemplatesUtilities._splitIntoProductionSteps( stepsDictList[-1:] )
  prodsList.append( ( 'MCSimulation', simulationSteps, None, False,
                      simulationTracking, defaultOutputSE, priority, cpu ) )
  for s in mergingSteps:
    prodsList.append( ( 'Merge', [s], 'fromPreviousProd', removeInputMerge,
                        mergingTracking, mergedDataSE, mergingPriority, mergingCPU ) )
elif w4:
  simulationSteps = stepsDictList
  prodsList.append( ( 'MCSimulation', simulationSteps, None, False,
                      simulationTracking, mergedDataSE, priority, cpu ) )

prodID = 0
for prodType, stepsList, bkQuery, removeInput, tracking, outputSE, priority, cpu in prodsList:
  prod = LHCbDIRAC.Workflow.Templates.TemplatesUtilities.buildProduction( prodType = prodType,
                                                                          stepsList = stepsList,
                                                                          requestID = requestID,
                                                                          prodDesc = '{{pDsc}}',
                                                                          configName = configName,
                                                                          configVersion = configVersion,
                                                                          dataTakingConditions = '{{simDesc}}',
                                                                          appendName = appendName,
                                                                          extraOptions = extraOptions,
                                                                          outputSE = outputSE,
                                                                          eventType = '{{eventType}}',
                                                                          events = events,
                                                                          priority = priority,
                                                                          cpu = cpu,
                                                                          sysConfig = sysConfig,
                                                                          generatorName = '{{Generator}}',
                                                                          outputMode = outputMode,
                                                                          outputFileMask = outputFileMask,
                                                                          targetSite = targetSite,
                                                                          banTier1s = banTier1s,
                                                                          removeInputData = removeInput,
                                                                          bkQuery = bkQuery,
                                                                          previousProdID = prodID )
  prodID = LHCbDIRAC.Workflow.Templates.TemplatesUtilities.launchProduction( prod, publishFlag, testFlag, requestID, parentReq,
                                                                             extend, tracking, BKscriptFlag, diracProd,
                                                                             logger = gLogger )

#################################################################################
# This is the start of the MC production definition (if requested)
#################################################################################

#if simulationSteps:
#  MCProd = Production()
#  if sysConfig:
#    MCProd.setJobParameters( { 'SystemConfig': sysConfig } )
#
#  MCProd.setProdType( 'MCSimulation' )
#  wkfName = 'Request_{{ID}}_MC_{{simDesc}}_{{pDsc}}_EventType{{eventType}}_{{MCNumberOfEvents}}Events'
#
#  MCProd.setWorkflowName( '%s_%s' % ( wkfName, appendName ) )
#  MCProd.setBKParameters( configName, configVersion, '{{pDsc}}', '{{simDesc}}' )
#  MCProd.setEventType( '{{eventType}}' )
#  MCProd.setNumberOfEvents( events )
#
#  for step in simulationSteps:
#    if step['ApplicationName'] == 'Gauss':
#      inputData = ''
#    else:
#      inputData = 'previousStep'
#    try:
#      ep = extraOptions[step['StepId']]
#    except IndexError:
#      ep = ''
#    MCProd.addApplicationStep( stepDict = step,
#                               outputSE = defaultOutputSE,
#                               optionsLine = ep,
#                               inputData = inputData )
#
#  MCProd.setWorkflowDescription( 'prodDescription' )
#  MCProd.addFinalizationStep( ['UploadOutputData',
#                               'FailoverRequest',
#                               'UploadLogFile'] )
#  MCProd.setJobParameters( { 'CPUTime': cpu } )
#
#  MCProd.setProdGroup( '{{pDsc}}' )
#  MCProd.setProdPriority( priority )
#  if outputsCERN:
#    MCProd.setOutputMode( 'Any' )
#  else:
#    MCProd.setOutputMode( 'Local' )
#  MCProd.setFileMask( outputFileMask )
#
#  if targetSite:
#    MCProd.setTargetSite( targetSite )
#
#  if banTier1s:
#    MCProd.banTier1s()
#
#  if publishFlag == False and testFlag:
#    gLogger.info( 'MC test will be launched locally with number of events set to %s.' % ( events ) )
#    try:
#      result = MCProd.runLocal()
#      if result['OK']:
#        gLogger.info( 'Template finished successfully' )
#        DIRAC.exit( 0 )
#      else:
#        gLogger.error( 'Something wrong with execution!' )
#        DIRAC.exit( 2 )
#    except Exception, x:
#      gLogger.error( 'MCProd test failed with exception:\n%s' % ( x ) )
#      DIRAC.exit( 2 )
#
#  result = MCProd.create( publish = publishFlag,
#                          requestID = int( requestID ),
#                          reqUsed = simulationTracking,
#                          transformation = False,
#                          bkScript = BKscriptFlag,
#                          parentRequestID = int( parentReq )
#                          )
#
#  if not result['OK']:
#    gLogger.error( 'Error during MCProd creation:\n%s\ncheck that the wkf name is unique.' % ( result['Message'] ) )
#    DIRAC.exit( 2 )
#
#  if publishFlag:
#    prodID = result['Value']
#    msg = 'MC production %s successfully created ' % ( prodID )
#
#    if extend:
#      diracProd.extendProduction( prodID, extend, printOutput = True )
#      msg += ', extended by %s jobs' % extend
#
#    if testFlag:
#      diracProd.production( prodID, 'manual', printOutput = True )
#      msg = msg + 'and started in manual mode.'
#    else:
#      diracProd.production( prodID, 'automatic', printOutput = True )
#      msg = msg + 'and started in automatic mode.'
#    gLogger.info( msg )
#
#  else:
#    prodID = 1
#    gLogger.info( 'MC production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, prodID ) )
#

