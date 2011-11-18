########################################################################
# $HeadURL$
########################################################################

"""  The MC Simulation Template creates workflows for the following simulation
     use-cases:
      WORKFLOW1: Simulation+MCMerge+Selection+Merge
      WORKFLOW2: Simulation+MCMerge+Selection
      WORKFLOW3: Simulation+MCMerge
      WORKFLOW4: Simulation
     
"""

__RCSID__ = "$Id$"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
import string

from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gLogger
gLogger = gLogger.getSubLogger( 'MC_Simulation_run.py' )

#################################################################################
# Below here is the actual production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

###########################################
# Configurable and fixed parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

w = '{{w#----->WORKFLOW: choose one below#}}'
w1 = '{{w1#-WORKFLOW1: Simulation+MCMerge+Selection+Merge#False}}'
w2 = '{{w2#-WORKFLOW2: Simulation+MCMerge+Selection#False}}'
w3 = '{{w3#-WORKFLOW3: Simulation+MCMerge#False}}'
w4 = '{{w4#-WORKFLOW4: Simulation#False}}'


certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True for validation prod#False}}'

configName = '{{BKConfigName#GENERAL: BK configuration name e.g. MC #MC}}'
configVersion = '{{BKConfigVersion#GENERAL: BK configuration version e.g. MC09, 2009, 2010#MC10}}'
outputFileMask = '{{WorkflowOutputDataFileMask#GENERAL: Workflow file extensions to save (comma separated) e.g. DST,DIGI#ALLSTREAMS.DST}}'

banTier1s = '{{WorkflowBanTier1s#GENERAL: Workflow ban Tier-1 sites for jobs Boolean True/False#True}}'
outputsCERN = '{{WorkflowCERNOutputs#GENERAL: Workflow upload workflow output to CERN#False}}'
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc43-opt, ANY#slc4_ia32_gcc34}}'

events = '{{MCNumberOfEvents#PROD-MC: Number of events per job#1000}}'
cpu = '{{MCMaxCPUTime#PROD-MC: Max CPU time in secs#1000000}}'
priority = '{{MCPriority#PROD-MC: Production priority#4}}'
extend = '{{MCExtend#PROD-MC: extend production by this many jobs#100}}'
gaussExtraOptions = '{{gaussExtraOptions#PROD-MC: Gauss extra options (leave blank for default)#}}'
brunelExtraOptions = '{{brunelExtraOptions#PROD-MC: Brunel extra options (leave blank for default)#}}'
daVinciExtraOptions = '{{daVinciExtraOptions#PROD-MC: DaVinci extra options (leave blank for default)#}}'

mergingPlugin = '{{MergingPlugin#PROD-Merging: plugin e.g. Standard, BySize#BySize}}'
mergingGroupSize = '{{MergingGroupSize#PROD-Merging: Group Size e.g. BySize = GB file size#5}}'
mergingPriority = '{{MergingPriority#PROD-Merging: Job Priority e.g. 8 by default#8}}'


replicationFlag = '{{TransformationEnable#PROD-Replication: flag Boolean True/False#True}}'
replicationPlugin = '{{ReplicationPlugin#PROD-Replication: ReplicationPlugin#LHCbMCDSTBroadcast}}'

evtType = '{{eventType}}'
#Often MC requests are defined with many subrequests but we want to retain
#the parent ID for viewing on the production monitoring page. If a parent
#request is defined then this is used.
requestID = '{{ID}}'
parentReq = '{{_parent}}'
eventNumberTotal = '{{EventNumberTotal}}'

if not parentReq:
  parentReq = requestID

BKClient = BookkeepingClient()

###########################################
# LHCb conventions implied by the above
###########################################

w1 = eval( w1 )
w2 = eval( w2 )
w3 = eval( w3 )
w4 = eval( w4 )

if not w1 and not w2 and not w3 and not w4:
  gLogger.error( 'I told you to select at least one workflow!' )
  DIRAC.exit( 2 )

if w1 or w2:
  gLogger.error( 'Not yet completely implemented, sorry! (ask Fed)' )
  DIRAC.exit( 2 )
else:
  oneStep = '{{p1App}}'
  twoSteps = '{{p2App}}'
  threeSteps = '{{p3App}}'
  fourSteps = '{{p4App}}'
  fiveSteps = '{{p5App}}'
  sixSteps = '{{p6App}}'
  sevenSteps = '{{p7App}}'


MCMerging = False
selection = False
selectionMerging = False
simulationTracking = 0
MCMergingTracking = 0
selectionTracking = 0
mergeSelectionTracking = 0

if w1:
  MCMerging = True
  selection = True
  selectionMerging = True
  mergeSelectionTracking = 1
elif w2:
  MCMerging = True
  selection = True
  selectionTracking = 1
elif w3:
  MCMerging = True
  MCMergingTracking = 1
elif w4:
  simulationTracking = 1

replicationFlag = eval( replicationFlag )
banTier1s = eval( banTier1s )
outputsCERN = eval( outputsCERN )
certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
validationFlag = eval( validationFlag )

if certificationFlag or localTestFlag:
  testFlag = True
  replicationFlag = False
  if certificationFlag:
    publishFlag = True
  if localTestFlag:
    publishFlag = False
    MCMerging = False
    selection = False
    selectionMerging = False
else:
  publishFlag = True
  testFlag = False

if outputsCERN:
  defaultOutputSE = 'CERN-RDST'
  brunelDataSE = 'CERN_MC_M-DST'
  daVinciDataSE = 'CERN_MC_M-DST'
else:
  defaultOutputSE = 'Tier1-RDST'
  brunelDataSE = 'Tier1_MC_M-DST'
  daVinciDataSE = 'Tier1_MC_M-DST'


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
  replicationFlag = False

if sevenSteps:
  gLogger.error( 'Nine steps specified, not sure what to do! Exiting...' )
  DIRAC.exit( 2 )

MC5 = False
MC4 = False
MC3 = False
MC2 = False
MC1 = False

#if ( eightSteps and w1 ) or ( sevenSteps and w2 ) or ( sixSteps and w3 ) or ( fiveSteps and w4 ):
if ( sixSteps and w3 ) or ( fiveSteps and w4 ):
  MC5 = True
#if ( sevenSteps and w1 ) or ( sixSteps and w2 ) or ( fiveSteps and w3 ) or ( fourSteps and w4 ):
if ( sixSteps and w2 ) or ( fiveSteps and w3 ) or ( fourSteps and w4 ):
  MC4 = True
if ( sixSteps and w1 ) or ( fiveSteps and w2 ) or ( fourSteps and w3 ) or ( threeSteps and w4 ):
  MC3 = True
if ( fiveSteps and w1 ) or ( fourSteps and w2 ) or ( threeSteps and w3 ) or ( twoSteps and w4 ):
  MC2 = True
if ( fourSteps and w1 ) or ( threeSteps and w2 ) or ( twoSteps and w3 ) or ( oneStep and w4 ):
  MC1 = True


if MC5 or MC4 or MC3 or MC2 or MC1:

  prodDescription = 'MC: Gauss'
  MCEnabled = True

  if oneStep != 'Gauss':
    gLogger.error( 'oneStep = %d', oneStep )
    DIRAC.exit( 2 )

  gaussStep = int( '{{p1Step}}' )
  gaussName = '{{p1Name}}'
  gaussVisibility = '{{p1Vis}}'
  gaussCDb = '{{p1CDb}}'
  gaussDDDb = '{{p1DDDb}}'
  gaussOptions = '{{p1Opt}}'
  gaussVersion = '{{p1Ver}}'
  gaussEP = '{{p1EP}}'

  gaussOutput = BKClient.getStepOutputFiles( gaussStep )
  if not gaussOutput:
    gLogger.error( 'Error getting res from BKK: %s', gaussOutput['Message'] )
    DIRAC.exit( 2 )

  gaussOutputList = [x[0].lower() for x in gaussOutput['Value']['Records']]
  if len( gaussOutputList ) > 1:
    gLogger.error( 'More than 1 gauss output: %s', gaussOutputList )
    DIRAC.exit( 2 )
  else:
    gaussType = gaussOutputList[0]


if MC5 or MC4 or MC3 or MC2:

  prodDescription = prodDescription + '->Boole'
  if twoSteps != 'Boole':
    gLogger.error( 'twoSteps = %d', twoSteps )
    DIRAC.exit( 2 )

  booleStep = int( '{{p2Step}}' )
  booleName = '{{p2Name}}'
  booleVisibility = '{{p2Vis}}'
  booleCDb = '{{p2CDb}}'
  booleDDDb = '{{p2DDDb}}'
  booleOptions = '{{p2Opt}}'
  booleVersion = '{{p2Ver}}'
  booleEP = '{{p2EP}}'

  booleInput = BKClient.getStepInputFiles( booleStep )
  if not booleInput:
    gLogger.error( 'Error getting res from BKK: %s', booleInput['Message'] )
    DIRAC.exit( 2 )

  booleInputList = [x[0].lower() for x in booleInput['Value']['Records']]
  if len( booleInputList ) > 1:
    gLogger.error( 'More than 1 boole output: %s', booleInputList )
    DIRAC.exit( 2 )
  else:
    booleInputType = booleInputList[0]

  booleOutput = BKClient.getStepOutputFiles( booleStep )
  if not booleOutput:
    gLogger.error( 'Error getting res from BKK: %s', booleOutput['Message'] )
    DIRAC.exit( 2 )

  booleOutputList = [x[0].lower() for x in booleOutput['Value']['Records']]
  if len( booleOutputList ) > 1:
    gLogger.error( 'More than 1 boole output: %s', booleOutputList )
    DIRAC.exit( 2 )
  else:
    booleType = booleOutputList[0]

if MC5 or MC4 or MC3:

  if threeSteps == 'Moore':
    prodDescription = prodDescription + '->Moore'
    mooreStep = int( '{{p3Step}}' )
    mooreName = '{{p3Name}}'
    mooreVisibility = '{{p3Vis}}'
    mooreCDb = '{{p3CDb}}'
    mooreDDDb = '{{p3DDDb}}'
    mooreOptions = '{{p3Opt}}'
    mooreVersion = '{{p3Ver}}'
    mooreEP = '{{p3EP}}'

    mooreInput = BKClient.getStepInputFiles( mooreStep )
    if not mooreInput:
      gLogger.error( 'Error getting res from BKK: %s', mooreInput['Message'] )
      DIRAC.exit( 2 )

    mooreInputList = [x[0].lower() for x in mooreInput['Value']['Records']]
    if len( mooreInputList ) > 1:
      gLogger.error( 'More than 1 moore output: %s', mooreInputList )
      DIRAC.exit( 2 )
    else:
      mooreInputType = mooreInputList[0]

    mooreOutput = BKClient.getStepOutputFiles( mooreStep )
    if not mooreOutput:
      gLogger.error( 'Error getting res from BKK: %s', mooreOutput['Message'] )
      DIRAC.exit( 2 )

    mooreOutputList = [x[0].lower() for x in mooreOutput['Value']['Records']]
    if len( mooreOutputList ) > 1:
      gLogger.error( 'More than 1 moore output: %s', mooreOutputList )
      DIRAC.exit( 2 )
    else:
      mooreType = mooreOutputList[0]

  elif threeSteps == 'Brunel':
    prodDescription = prodDescription + '->Brunel'
    brunelStep = int( '{{p3Step}}' )
    brunelName = '{{p3Name}}'
    brunelVisibility = '{{p3Vis}}'
    brunelCDb = '{{p3CDb}}'
    brunelDDDb = '{{p3DDDb}}'
    brunelOptions = '{{p3Opt}}'
    brunelVersion = '{{p3Ver}}'
    brunelEP = '{{p3EP}}'

    brunelInput = BKClient.getStepInputFiles( brunelStep )
    if not brunelInput:
      gLogger.error( 'Error getting res from BKK: %s', brunelInput['Message'] )
      DIRAC.exit( 2 )

    brunelInputList = [x[0].lower() for x in brunelInput['Value']['Records']]
    if len( brunelInputList ) > 1:
      gLogger.error( 'More than 1 brunel output: %s', brunelInputList )
      DIRAC.exit( 2 )
    else:
      brunelInputType = brunelInputList[0]

    brunelOutput = BKClient.getStepOutputFiles( brunelStep )
    if not brunelOutput:
      gLogger.error( 'Error getting res from BKK: %s', brunelOutput['Message'] )
      DIRAC.exit( 2 )

    brunelOutputList = [x[0].lower() for x in brunelOutput['Value']['Records']]
    if len( brunelOutputList ) > 1:
      gLogger.error( 'More than 1 brunel output: %s', brunelOutputList )
      DIRAC.exit( 2 )
    else:
      brunelType = brunelOutputList[0]

  else:
    gLogger.error( 'threeSteps = %d', threeSteps )
    DIRAC.exit( 2 )

if MC5 or MC4:

  if fourSteps == 'Brunel':
    prodDescription = prodDescription + '->Brunel'
    brunelStep = int( '{{p4Step}}' )
    brunelName = '{{p4Name}}'
    brunelVisibility = '{{p4Vis}}'
    brunelCDb = '{{p4CDb}}'
    brunelDDDb = '{{p4DDDb}}'
    brunelOptions = '{{p4Opt}}'
    brunelVersion = '{{p4Ver}}'
    brunelEP = '{{p4EP}}'

    brunelInput = BKClient.getStepInputFiles( brunelStep )
    if not brunelInput:
      gLogger.error( 'Error getting res from BKK: %s', brunelInput['Message'] )
      DIRAC.exit( 2 )

    brunelInputList = [x[0].lower() for x in brunelInput['Value']['Records']]
    if len( brunelInputList ) > 1:
      gLogger.error( 'More than 1 brunel output: %s', brunelInputList )
      DIRAC.exit( 2 )
    else:
      brunelInputType = brunelInputList[0]

    brunelOutput = BKClient.getStepOutputFiles( brunelStep )
    if not brunelOutput:
      gLogger.error( 'Error getting res from BKK: %s', brunelOutput['Message'] )
      DIRAC.exit( 2 )

    brunelOutputList = [x[0].lower() for x in brunelOutput['Value']['Records']]
    if len( brunelOutputList ) > 1:
      gLogger.error( 'More than 1 brunel output: %s', brunelOutputList )
      DIRAC.exit( 2 )
    else:
      brunelType = brunelOutputList[0]

  elif fourSteps == 'DaVinci':
    prodDescription = prodDescription + '->DaVinci'
    davinciStep = int( '{{p4Step}}' )
    davinciName = '{{p4Name}}'
    davinciVisibility = '{{p4Vis}}'
    davinciCDb = '{{p4CDb}}'
    davinciDDDb = '{{p4DDDb}}'
    davinciOptions = '{{p4Opt}}'
    davinciVersion = '{{p4Ver}}'
    davinciEP = '{{p4EP}}'

    davinciInput = BKClient.getStepInputFiles( davinciStep )
    if not davinciInput:
      gLogger.error( 'Error getting res from BKK: %s', davinciInput['Message'] )
      DIRAC.exit( 2 )

    davinciInputList = [x[0].lower() for x in davinciInput['Value']['Records']]
    if len( davinciInputList ) > 1:
      gLogger.error( 'More than 1 davinci output: %s', davinciInputList )
      DIRAC.exit( 2 )
    else:
      davinciInputType = davinciInputList[0]

    davinciOutput = BKClient.getStepOutputFiles( davinciStep )
    if not davinciOutput:
      gLogger.error( 'Error getting res from BKK: %s', davinciOutput['Message'] )
      DIRAC.exit( 2 )

    davinciOutputList = [x[0].lower() for x in davinciOutput['Value']['Records']]
    if len( davinciOutputList ) > 1:
      gLogger.error( 'More than 1 davinci output: %s', davinciOutputList )
      DIRAC.exit( 2 )
    else:
      davinciType = davinciOutputList[0]
  else:
    gLogger.error( 'fourSteps = %d', fourSteps )
    DIRAC.exit( 2 )

if MC5:

  prodDescription = prodDescription + '->DaVinci'
  davinciStep = int( '{{p5Step}}' )
  davinciName = '{{p5Name}}'
  davinciVisibility = '{{p5Vis}}'
  davinciCDb = '{{p5CDb}}'
  davinciDDDb = '{{p5DDDb}}'
  davinciOptions = '{{p5Opt}}'
  davinciVersion = '{{p5Ver}}'
  davinciEP = '{{p5EP}}'

  davinciInput = BKClient.getStepInputFiles( davinciStep )
  if not davinciInput:
    gLogger.error( 'Error getting res from BKK: %s', davinciInput['Message'] )
    DIRAC.exit( 2 )

  davinciInputList = [x[0].lower() for x in davinciInput['Value']['Records']]
  if len( davinciInputList ) > 1:
    gLogger.error( 'More than 1 davinci output: %s', davinciInputList )
    DIRAC.exit( 2 )
  else:
    davinciInputType = davinciInputList[0]

  davinciOutput = BKClient.getStepOutputFiles( davinciStep )
  if not davinciOutput:
    gLogger.error( 'Error getting res from BKK: %s', davinciOutput['Message'] )
    DIRAC.exit( 2 )

  davinciOutputList = [x[0].lower() for x in davinciOutput['Value']['Records']]
  if len( davinciOutputList ) > 1:
    gLogger.error( 'More than 1 davinci output: %s', davinciOutputList )
    DIRAC.exit( 2 )
  else:
    davinciType = davinciOutputList[0]


if MCMerging:
  prodDescription = prodDescription + ' + MC Merging'

  if MC5:
    mcMergingApp = '{{p6App}}'
    mcMergingStep = int( '{{p6Step}}' )
    mcMergingName = '{{p6Name}}'
    mcMergingVisibility = '{{p6Vis}}'
    mcMergingCDb = '{{p6CDb}}'
    mcMergingDDDb = '{{p6DDDb}}'
    mcMergingOptions = '{{p6Opt}}'
    mcMergingVersion = '{{p6Ver}}'
    mcMergingEP = '{{p6EP}}'

  elif MC4:
    mcMergingApp = '{{p5App}}'
    mcMergingStep = int( '{{p5Step}}' )
    mcMergingName = '{{p5Name}}'
    mcMergingVisibility = '{{p5Vis}}'
    mcMergingCDb = '{{p5CDb}}'
    mcMergingDDDb = '{{p5DDDb}}'
    mcMergingOptions = '{{p5Opt}}'
    mcMergingVersion = '{{p5Ver}}'
    mcMergingEP = '{{p5EP}}'

  elif MC3:
    mcMergingApp = '{{p4App}}'
    mcMergingStep = int( '{{p4Step}}' )
    mcMergingName = '{{p4Name}}'
    mcMergingVisibility = '{{p4Vis}}'
    mcMergingCDb = '{{p4CDb}}'
    mcMergingDDDb = '{{p4DDDb}}'
    mcMergingOptions = '{{p4Opt}}'
    mcMergingVersion = '{{p4Ver}}'
    mcMergingEP = '{{p4EP}}'

  elif MC2:
    mcMergingApp = '{{p3App}}'
    mcMergingStep = int( '{{p3Step}}' )
    mcMergingName = '{{p3Name}}'
    mcMergingVisibility = '{{p3Vis}}'
    mcMergingCDb = '{{p3CDb}}'
    mcMergingDDDb = '{{p3DDDb}}'
    mcMergingOptions = '{{p3Opt}}'
    mcMergingVersion = '{{p3Ver}}'
    mcMergingEP = '{{p3EP}}'

  elif MC1:
    mcMergingApp = '{{p2App}}'
    mcMergingStep = int( '{{p2Step}}' )
    mcMergingName = '{{p2Name}}'
    mcMergingVisibility = '{{p2Vis}}'
    mcMergingCDb = '{{p2CDb}}'
    mcMergingDDDb = '{{p2DDDb}}'
    mcMergingOptions = '{{p2Opt}}'
    mcMergingVersion = '{{p2Ver}}'
    mcMergingEP = '{{p2EP}}'

  mcMergingInput = BKClient.getStepInputFiles( mcMergingStep )
  if not mcMergingInput:
    gLogger.error( 'Error getting res from BKK: %s', mcMergingInput['Message'] )
    DIRAC.exit( 2 )

  mcMergingInputList = [x[0].lower() for x in mcMergingInput['Value']['Records']]
  if len( mcMergingInputList ) > 1:
    gLogger.error( 'More than 1 mcMerging output: %s', mcMergingInputList )
    DIRAC.exit( 2 )
  else:
    mcMergingInputType = mcMergingInputList[0]

  mcMergingOutput = BKClient.getStepOutputFiles( mcMergingStep )
  if not mcMergingOutput:
    gLogger.error( 'Error getting res from BKK: %s', mcMergingOutput['Message'] )
    DIRAC.exit( 2 )

  mcMergingOutputList = [x[0].lower() for x in mcMergingOutput['Value']['Records']]
  if len( mcMergingOutputList ) > 1:
    gLogger.error( 'More than 1 mcMerging output: %s', mcMergingOutputList )
    DIRAC.exit( 2 )
  else:
    mcMergingType = mcMergingOutputList[0]

if selection:
  prodDescription = prodDescription + ' + Selection'

  if MC5:
    selectionStep = int( '{{p7Step}}' )
    selectionName = '{{p7Name}}'
    selectionVisibility = '{{p7Vis}}'
    selectionCDb = '{{p7CDb}}'
    selectionDDDb = '{{p7DDDb}}'
    selectionOptions = '{{p7Opt}}'
    selectionVersion = '{{p7Ver}}'
    selectionEP = '{{p7EP}}'

  if MC5:
    selectionStep = int( '{{p6Step}}' )
    selectionName = '{{p6Name}}'
    selectionVisibility = '{{p6Vis}}'
    selectionCDb = '{{p6CDb}}'
    selectionDDDb = '{{p6DDDb}}'
    selectionOptions = '{{p6Opt}}'
    selectionVersion = '{{p6Ver}}'
    selectionEP = '{{p6EP}}'

  if MC3:
    selectionStep = int( '{{p5Step}}' )
    selectionName = '{{p5Name}}'
    selectionVisibility = '{{p5Vis}}'
    selectionCDb = '{{p5CDb}}'
    selectionDDDb = '{{p5DDDb}}'
    selectionOptions = '{{p5Opt}}'
    selectionVersion = '{{p5Ver}}'
    selectionEP = '{{p5EP}}'

  if MC2:
    selectionStep = int( '{{p4Step}}' )
    selectionName = '{{p4Name}}'
    selectionVisibility = '{{p4Vis}}'
    selectionCDb = '{{p4CDb}}'
    selectionDDDb = '{{p4DDDb}}'
    selectionOptions = '{{p4Opt}}'
    selectionVersion = '{{p4Ver}}'
    selectionEP = '{{p4EP}}'

  if MC1:
    selectionStep = int( '{{p3Step}}' )
    selectionName = '{{p3Name}}'
    selectionVisibility = '{{p3Vis}}'
    selectionCDb = '{{p3CDb}}'
    selectionDDDb = '{{p3DDDb}}'
    selectionOptions = '{{p3Opt}}'
    selectionVersion = '{{p3Ver}}'
    selectionEP = '{{p3EP}}'

  selectionOutput = BKClient.getStepOutputFiles( selectionStep )
  if not selectionOutput:
    gLogger.error( 'Error getting res from BKK: %s', selectionOutput['Message'] )
    DIRAC.exit( 2 )

  selectionOutputList = [x[0].lower() for x in selectionOutput['Value']['Records']]
  if len( selectionOutputList ) > 1:
    gLogger.error( 'More than 1 selection output: %s', selectionOutputList )
    DIRAC.exit( 2 )
  else:
    selectionType = selectionOutputList[0]

if selectionMerging:
  prodDescription = prodDescription + ' + SelectionMerging'

  if MC5:
    gLogger.error( 'Can\'t, sorry!' )
    DIRAC.exit( 2 )

  if MC5:
    selectionMergingStep = int( '{{p7Step}}' )
    selectionMergingName = '{{p7Name}}'
    selectionMergingVisibility = '{{p7Vis}}'
    selectionMergingCDb = '{{p7CDb}}'
    selectionMergingDDDb = '{{p7DDDb}}'
    selectionMergingOptions = '{{p7Opt}}'
    selectionMergingVersion = '{{p7Ver}}'
    selectionMergingEP = '{{p7EP}}'

  if MC3:
    selectionMergingStep = int( '{{p6Step}}' )
    selectionMergingName = '{{p6Name}}'
    selectionMergingVisibility = '{{p6Vis}}'
    selectionMergingCDb = '{{p6CDb}}'
    selectionMergingDDDb = '{{p6DDDb}}'
    selectionMergingOptions = '{{p6Opt}}'
    selectionMergingVersion = '{{p6Ver}}'
    selectionMergingEP = '{{p6EP}}'

  if MC2:
    selectionMergingStep = int( '{{p5Step}}' )
    selectionMergingName = '{{p5Name}}'
    selectionMergingVisibility = '{{p5Vis}}'
    selectionMergingCDb = '{{p5CDb}}'
    selectionMergingDDDb = '{{p5DDDb}}'
    selectionMergingOptions = '{{p5Opt}}'
    selectionMergingVersion = '{{p5Ver}}'
    selectionMergingEP = '{{p5EP}}'

  if MC1:
    selectionMergingStep = int( '{{p4Step}}' )
    selectionMergingName = '{{p4Name}}'
    selectionMergingVisibility = '{{p4Vis}}'
    selectionMergingCDb = '{{p4CDb}}'
    selectionMergingDDDb = '{{p4DDDb}}'
    selectionMergingOptions = '{{p4Opt}}'
    selectionMergingVersion = '{{p4Ver}}'
    selectionMergingEP = '{{p4EP}}'

  selectionMergingOutput = BKClient.getStepOutputFiles( selectionMergingStep )
  if not selectionMergingOutput:
    gLogger.error( 'Error getting res from BKK: %s', selectionMergingOutput['Message'] )
    DIRAC.exit( 2 )

  selectionMergingOutputList = [x[0].lower() for x in selectionMergingOutput['Value']['Records']]
  if len( selectionMergingOutputList ) > 1:
    gLogger.error( 'More than 1 selectionMerging output: %s', selectionMergingOutputList )
    DIRAC.exit( 2 )
  else:
    selectionMergingType = selectionMergingOutputList[0]

if outputFileMask:
  maskList = [m.lower() for m in outputFileMask.replace( ' ', '' ).split( ',' )]
  outputFileMask = string.join( maskList, ';' )

if validationFlag:
  configName = 'validation'

#defaultEvtOpts = gConfig.getValue( '/Operations/Gauss/EvtOpts', '$DECFILESROOT/options/{{eventType}}.opts' )
#if not defaultEvtOpts in gaussOptions:
#  gaussOptions += ';%s' % defaultEvtOpts

#defaultGenOpts = gConfig.getValue( '/Operations/Gauss/Gen{{Generator}}Opts', '$LBBCVEGPYROOT/options/{{Generator}}.py' )
#if not defaultGenOpts in gaussOptions:
#  gaussOptions += ';%s' % defaultGenOpts

if not MCEnabled:
  gLogger.info( 'No MC requested...?' )
else:

  MCProd = Production()

  if sysConfig:
    try:
      MCProd.setJobParameters( { 'SystemConfig': sysConfig } )
    except:
      MCProd.setSystemConfig( sysConfig )

  MCProd.setProdType( 'MCSimulation' )
  wkfName = 'Request_{{ID}}_MC_{{simDesc}}_{{pDsc}}_EventType{{eventType}}_{{MCNumberOfEvents}}Events'

  MCProd.setWorkflowName( '%s_%s' % ( wkfName, appendName ) )
  MCProd.setBKParameters( configName, configVersion, '{{pDsc}}', '{{simDesc}}' )
  MCProd.setDBTags( '{{p1CDb}}', '{{p1DDDb}}' )
  MCProd.setSimulationEvents( events, eventNumberTotal )

  if MC5 or MC4 or MC3 or MC2 or MC1:

    MCProd.addGaussStep( gaussVersion, '{{Generator}}', events, gaussOptions, eventType = '{{eventType}}',
                         extraPackages = gaussEP, condDBTag = gaussCDb, ddDBTag = gaussDDDb,
                         outputSE = defaultOutputSE, appType = gaussType, extraOpts = gaussExtraOptions,
                         stepID = gaussStep, stepName = gaussName, stepVisible = gaussVisibility )

  if MC5 or MC4 or MC3 or MC2:

    MCProd.addBooleStep( booleVersion, booleType, booleOptions, extraPackages = booleEP,
                         condDBTag = booleCDb, ddDBTag = booleDDDb, outputSE = defaultOutputSE,
                         stepID = booleStep, stepName = booleName, stepVisible = booleVisibility )

  if MC5 or MC4 or MC3:

    if threeSteps == 'Moore':
      MCProd.addMooreStep( mooreVersion, mooreType, mooreOptions, extraPackages = mooreEP, inputDataType = mooreInputType,
                           condDBTag = mooreCDb, ddDBTag = mooreDDDb, outputSE = defaultOutputSE,
                           stepID = mooreStep, stepName = mooreName, stepVisible = mooreVisibility )

    elif threeSteps == 'Brunel':
      MCProd.addBrunelStep( brunelVersion, brunelType, brunelOptions, extraPackages = brunelEP, inputDataType = brunelInputType,
                             outputSE = brunelDataSE, condDBTag = brunelCDb, ddDBTag = brunelDDDb, extraOpts = brunelExtraOptions,
                             stepID = brunelStep, stepName = brunelName, stepVisible = brunelVisibility )

  if MC5 or MC4:

    if fourSteps == 'Brunel':
      MCProd.addBrunelStep( brunelVersion, brunelType, brunelOptions, extraPackages = brunelEP, inputDataType = brunelInputType,
                            outputSE = brunelDataSE, condDBTag = brunelCDb, ddDBTag = brunelDDDb, extraOpts = brunelExtraOptions,
                            stepID = brunelStep, stepName = brunelName, stepVisible = brunelVisibility )
    if fourSteps == 'DaVinci':
      MCProd.addDaVinciStep( davinciVersion, davinciType, davinciOptions, extraPackages = davinciEP, inputDataType = davinciInputType,
                             dataType = 'MC', extraOpts = daVinciExtraOptions,
                             outputSE = daVinciDataSE, condDBTag = davinciCDb, ddDBTag = davinciDDDb,
                             stepID = davinciStep, stepName = davinciName, stepVisible = davinciVisibility )

  if MC5:
    MCProd.addDaVinciStep( davinciVersion, davinciType, davinciOptions, extraPackages = davinciEP, inputDataType = davinciInputType,
                           dataType = 'MC', extraOpts = daVinciExtraOptions,
                           outputSE = daVinciDataSE, condDBTag = davinciCDb, ddDBTag = davinciDDDb,
                           stepID = davinciStep, stepName = davinciName, stepVisible = davinciVisibility )

  gLogger.info( prodDescription )
  MCProd.setWorkflowDescription( prodDescription )
  try:
    MCProd.addFinalizationStep( ['UploadOutputData',
                                 'FailoverRequest',
                                 'UploadLogFile'] )
    MCProd.setJobParameters( { 'CPUTime': cpu } )
  except:
    MCProd.addFinalizationStep()
    MCProd.setCPUTime( cpu )

  MCProd.setProdGroup( '{{pDsc}}' )
  MCProd.setProdPriority( priority )
  MCProd.setOutputMode( 'Any' )
  MCProd.setFileMask( outputFileMask )

  if banTier1s:
    MCProd.banTier1s()

  if publishFlag == False and testFlag:
    gLogger.info( 'MC test will be launched locally with number of events set to %s.' % ( events ) )
    try:
      result = MCProd.runLocal()
      if result['OK']:
        DIRAC.exit( 0 )
      else:
        DIRAC.exit( 2 )
    except Exception, x:
      gLogger.error( 'MCProd test failed with exception:\n%s' % ( x ) )
      DIRAC.exit( 2 )

  result = MCProd.create( publish = publishFlag,
                          requestID = int( requestID ),
                          reqUsed = simulationTracking,
                          transformation = False,
                          bkScript = BKscriptFlag,
                          parentRequestID = int( parentReq )
                          )

  if not result['OK']:
    gLogger.error( 'Error during MCProd creation:\n%s\ncheck that the wkf name is unique.' % ( result['Message'] ) )
    DIRAC.exit( 2 )

  if publishFlag:
    diracProd = DiracProduction()

    prodID = result['Value']
    msg = 'MC MCProd %s successfully created ' % ( prodID )

    if extend:
      diracProd.extendProduction( prodID, extend, printOutput = True )
      msg += ', extended by %s jobs' % extend

    if testFlag:
      diracProd.production( prodID, 'manual', printOutput = True )
      msg = msg + 'and started in manual mode.'
    else:
      diracProd.production( prodID, 'automatic', printOutput = True )
      msg = msg + 'and started in automatic mode.'
    gLogger.info( msg )

  else:
    prodID = 1
    gLogger.info( 'MC production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, prodID ) )


#################################################################################
# This is the start of the MC merging production definition (if requested)
#################################################################################

if not MCMerging:
  gLogger.info( 'No MC merging requested' )
else:

  inputBKQuery = {
                  'FileType'                 : mcMergingType.upper(),
                  'EventType'                : evtType,
                  'ProductionID'             : int( prodID )
                  }

  MCMergeProd = Production()
  if sysConfig:
    try:
      MCMergeProd.setJobParameters( {"SystemConfig": sysConfig } )
    except:
      MCMergeProd.setSystemConfig( sysConfig )

  MCMergeProd.setProdType( 'Merge' )
  mergingName = 'Request_{{ID}}_%sMerging_{{pDsc}}_EventType%s_Prod%s_Files%sGB' % ( mcMergingType, evtType, prodID, mergingGroupSize )
  MCMergeProd.setWorkflowName( mergingName )
  MCMergeProd.setWorkflowDescription( 'MC workflow for merging outputs from a previous production.' )
  MCMergeProd.setBKParameters( configName, configVersion, '{{pDsc}}', '{{simDesc}}' )
  MCMergeProd.setDBTags( mcMergingCDb, mcMergingDDDb )
  if mcMergingApp == 'LHCb':
    MCMergeProd.addMergeStep( mcMergingVersion, eventType = '{{eventType}}', inputDataType = mcMergingInputType,
                              inputData = mcMergingType, condDBTag = mcMergingCDb, ddDBTag = mcMergingDDDb,
                              stepID = mcMergingStep, stepName = mcMergingName, stepVisible = mcMergingVisibility )
  elif mcMergingApp == 'DaVinci':
    MCMergeProd.addDaVinciStep( mcMergingVersion, 'mcMerging', mcMergingOptions, extraPackages = mcMergingEP, eventType = '{{eventType}}',
                                inputDataType = mcMergingInputType, inputProduction = prodID, outputSE = daVinciDataSE,
                                stepID = mcMergingStep, stepName = mcMergingName, stepVisible = mcMergingVisibility )
  else:
    gLogger.error( "No LHCb nor DaVinci in MC Merging...?" )
    DIRAC.exit( 2 )

  MCMergeProd.setInputBKSelection( inputBKQuery )
  try:
    MCMergeProd.addFinalizationStep( ['UploadOutputData',
                                      'FailoverRequest',
                                      'RemoveInputData',
                                      'UploadLogFile'] )
    MCMergeProd.setJobParameters( {"InputDataPolicy": 'download' } )
  except:
    MCMergeProd.addFinalizationStep( removeInputData = True )
    MCMergeProd.setInputDataPolicy( 'donwload' )
  MCMergeProd.setJobFileGroupSize( mergingGroupSize )
  MCMergeProd.setProdGroup( '{{pDsc}}' )
  MCMergeProd.setProdPriority( mergingPriority )

  #MCMergeProd.setFileMask( finalAppType.lower() )
  MCMergeProd.setProdPlugin( mergingPlugin )

  result = MCMergeProd.create( 
                              publish = publishFlag,
                              bkScript = BKscriptFlag,
                              requestID = int( requestID ),
                              reqUsed = MCMergingTracking,
                              transformation = replicationFlag,
                              parentRequestID = int( parentReq ),
                              transformationPlugin = replicationPlugin
                              )
  if not result['OK']:
    gLogger.error( 'Error during merging production creation:\n%s\n' % ( result['Message'] ) )
    DIRAC.exit( 2 )

  if publishFlag:
    prodID = result['Value']
    msg = 'Merging production %s successfully created ' % ( prodID )
    if testFlag:
      diracProd.production( prodID, 'manual', printOutput = True )
      msg = msg + 'and started in manual mode.'
    else:
      diracProd.production( prodID, 'automatic', printOutput = True )
      msg = msg + 'and started in automatic mode.'
    gLogger.info( msg )

  else:
    prodID = 1
    gLogger.info( 'Merging production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, prodID ) )


#
#if not replicationFlag:
#  gLogger.info( 'No transformation requested' )
#else:
#  #FIXME: do!
#  print 'DO SOMETHING'

#################################################################################
# End of the template.
#################################################################################
