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
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation

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
outputsCERN = '{{WorkflowCERNOutputs#GENERAL: Workflow upload workflow output to CERN#False}}'
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc43-opt, ANY#i686-slc5-gcc43-opt}}'
targetSite = '{{TargetSite#GENERAL: Set a target site (blank for everything)#}}'

events = '{{MCNumberOfEvents#PROD-MC: Number of events per job#1000}}'
cpu = '{{MCMaxCPUTime#PROD-MC: Max CPU time in secs#1000000}}'
priority = '{{MCPriority#PROD-MC: Production priority#4}}'
extend = '{{MCExtend#PROD-MC: extend production by this many jobs#100}}'
gaussExtraOptions = '{{gaussExtraOptions#PROD-MC: Gauss extra options (leave blank for default)#}}'
brunelExtraOptions = '{{brunelExtraOptions#PROD-MC: Brunel extra options (leave blank for default)#}}'
daVinciExtraOptions = '{{daVinciExtraOptions#PROD-MC: DaVinci extra options (leave blank for default)#}}'

selectionPlugin = '{{selectionPlugin#PROD-Selection: plugin e.g. Standard, BySize#BySize}}'
selectionGroupSize = '{{selectionGroupSize#PROD-Selection: input files total size (we\'ll use protocol access)#20}}'
selectionPriority = '{{selectionPriority#PROD-Selection: Job Priority e.g. 8 by default#6}}'
selectionExtraOptions = '{{selectionExtraOptions#PROD-Selection: selection extra options (leave blank for default)#}}'

mergingPlugin = '{{MergingPlugin#PROD-Merging: plugin e.g. Standard, BySize#BySize}}'
mergingGroupSize = '{{MergingGroupSize#PROD-Merging: Group Size e.g. BySize = GB file size#5}}'
mergingPriority = '{{MergingPriority#PROD-Merging: Job Priority e.g. 8 by default#8}}'

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

#if w1 or w2:
#  gLogger.error( 'Not yet completely implemented, sorry! (ask Federico)' )
#  DIRAC.exit( 2 )
#else:
oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'
threeSteps = '{{p3App}}'
fourSteps = '{{p4App}}'
fiveSteps = '{{p5App}}'
sixSteps = '{{p6App}}'
sevenSteps = '{{p7App}}'
eightSteps = '{{p8App}}'

selection = False
merging = False
simulationTracking = 0
mergingTracking = 0
selectionTracking = 0

if w1:
  merging = True
  selection = True
  mergingTracking = 1
elif w2:
  selection = True
  selectionTracking = 1
elif w3:
  merging = True
  mergingTracking = 1
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
    merging = False
    selection = False
else:
  publishFlag = True
  testFlag = False

if publishFlag:
  diracProd = DiracProduction()

if outputsCERN:
  defaultOutputSE = 'CERN_MC-DST'
  brunelDataSE = 'CERN_MC-DST'
  daVinciDataSE = 'CERN_MC-DST'
  mergedDataSE = 'CERN_MC_M-DST'
else:
  defaultOutputSE = 'Tier1_MC-DST'
  brunelDataSE = 'Tier1_MC-DST'
  daVinciDataSE = 'Tier1_MC-DST'
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

if eightSteps:
  gLogger.error( 'Eight steps specified, not sure what to do! Exiting...' )
  DIRAC.exit( 2 )

MC5 = False
MC4 = False
MC3 = False
MC2 = False
MC1 = False

if ( sevenSteps and w1 ) or ( sixSteps and w2 ) or ( sixSteps and w3 ) or ( fiveSteps and w4 ):
  MC5 = True
if ( sixSteps and w1 ) or ( fiveSteps and w2 ) or ( fiveSteps and w3 ) or ( fourSteps and w4 ):
  MC4 = True
if ( fiveSteps and w1 ) or ( fourSteps and w2 ) or ( fourSteps and w3 ) or ( threeSteps and w4 ):
  MC3 = True
if ( fourSteps and w1 ) or ( threeSteps and w2 ) or ( threeSteps and w3 ) or ( twoSteps and w4 ):
  MC2 = True
if ( threeSteps and w1 ) or ( twoSteps and w2 ) or ( twoSteps and w3 ) or ( oneStep and w4 ):
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
  gaussPP = '{{p1Pass}}'

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

  MCOutput = gaussType


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
  boolePP = '{{p2Pass}}'

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

  MCOutput = booleType

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
    moorePP = '{{p3Pass}}'

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

    MCOutput = mooreType

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
    brunelPP = '{{p3Pass}}'

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

    MCOutput = brunelType

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
    brunelPP = '{{p4Pass}}'

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

    MCOutput = brunelType

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
    davinciPP = '{{p4Pass}}'

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

    MCOutput = davinciType

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
  davinciPP = '{{p5Pass}}'

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

  MCOutput = davinciType


if selection:
  prodDescription = prodDescription + ' + Selection'

  if MC5:
    selectionStep = int( '{{p6Step}}' )
    selectionName = '{{p6Name}}'
    selectionVisibility = '{{p6Vis}}'
    selectionCDb = '{{p6CDb}}'
    selectionDDDb = '{{p6DDDb}}'
    selectionOptions = '{{p6Opt}}'
    selectionVersion = '{{p6Ver}}'
    selectionEP = '{{p6EP}}'
    selectionPP = '{{p6Pass}}'

  if MC4:
    selectionStep = int( '{{p5Step}}' )
    selectionName = '{{p5Name}}'
    selectionVisibility = '{{p5Vis}}'
    selectionCDb = '{{p5CDb}}'
    selectionDDDb = '{{p5DDDb}}'
    selectionOptions = '{{p5Opt}}'
    selectionVersion = '{{p5Ver}}'
    selectionEP = '{{p5EP}}'
    selectionPP = '{{p5Pass}}'

  if MC3:
    selectionStep = int( '{{p4Step}}' )
    selectionName = '{{p4Name}}'
    selectionVisibility = '{{p4Vis}}'
    selectionCDb = '{{p4CDb}}'
    selectionDDDb = '{{p4DDDb}}'
    selectionOptions = '{{p4Opt}}'
    selectionVersion = '{{p4Ver}}'
    selectionEP = '{{p4EP}}'
    selectionPP = '{{p4Pass}}'

  if MC2:
    selectionStep = int( '{{p3Step}}' )
    selectionName = '{{p3Name}}'
    selectionVisibility = '{{p3Vis}}'
    selectionCDb = '{{p3CDb}}'
    selectionDDDb = '{{p3DDDb}}'
    selectionOptions = '{{p3Opt}}'
    selectionVersion = '{{p3Ver}}'
    selectionEP = '{{p3EP}}'
    selectionPP = '{{p3Pass}}'

  if MC1:
    selectionStep = int( '{{p2Step}}' )
    selectionName = '{{p2Name}}'
    selectionVisibility = '{{p2Vis}}'
    selectionCDb = '{{p2CDb}}'
    selectionDDDb = '{{p2DDDb}}'
    selectionOptions = '{{p2Opt}}'
    selectionVersion = '{{p2Ver}}'
    selectionEP = '{{p2EP}}'
    selectionPP = '{{p2Pass}}'

  selectionInput = BKClient.getStepInputFiles( selectionStep )
  if not selectionInput:
    gLogger.error( 'Error getting res from BKK: %s', selectionInput['Message'] )
    DIRAC.exit( 2 )

  selectionInputList = [x[0].lower() for x in selectionInput['Value']['Records']]
  if len( selectionInputList ) > 1:
    gLogger.error( 'More than 1 selection output: %s', selectionInputList )
    DIRAC.exit( 2 )
  else:
    selectionInputType = selectionInputList[0]

  selectionOutput = BKClient.getStepOutputFiles( selectionStep )
  if not selectionOutput:
    gLogger.error( 'Error getting res from BKK: %s', selectionOutput['Message'] )
    DIRAC.exit( 2 )

  selectionOutputList = [x[0].lower() for x in selectionOutput['Value']['Records']]
  if len( selectionOutputList ) > 1:
    gLogger.error( 'More than 1 selection output: %s', selectionOutputList )
    DIRAC.exit( 2 )
  else:
    selectionOutputType = selectionOutputList[0]


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



if merging:
  prodDescription = prodDescription + ' + Merging'

  if ( MC5 and not selection ) or ( MC4 and selection ):
    mergingApp = '{{p6App}}'
    mergingStep = int( '{{p6Step}}' )
    mergingName = '{{p6Name}}'
    mergingVisibility = '{{p6Vis}}'
    mergingCDb = '{{p6CDb}}'
    mergingDDDb = '{{p6DDDb}}'
    mergingOptions = '{{p6Opt}}'
    mergingVersion = '{{p6Ver}}'
    mergingEP = '{{p6EP}}'
    mergingPP = '{{p6Pass}}'

  elif ( MC4 and not selection ) or ( MC3 and selection ):
    mergingApp = '{{p5App}}'
    mergingStep = int( '{{p5Step}}' )
    mergingName = '{{p5Name}}'
    mergingVisibility = '{{p5Vis}}'
    mergingCDb = '{{p5CDb}}'
    mergingDDDb = '{{p5DDDb}}'
    mergingOptions = '{{p5Opt}}'
    mergingVersion = '{{p5Ver}}'
    mergingEP = '{{p5EP}}'
    mergingPP = '{{p5Pass}}'

  elif ( MC3 and not selection ) or ( MC2 and selection ):
    mergingApp = '{{p4App}}'
    mergingStep = int( '{{p4Step}}' )
    mergingName = '{{p4Name}}'
    mergingVisibility = '{{p4Vis}}'
    mergingCDb = '{{p4CDb}}'
    mergingDDDb = '{{p4DDDb}}'
    mergingOptions = '{{p4Opt}}'
    mergingVersion = '{{p4Ver}}'
    mergingEP = '{{p4EP}}'
    mergingPP = '{{p4Pass}}'

  elif ( MC2 and not selection ) or ( MC1 and selection ):
    mergingApp = '{{p3App}}'
    mergingStep = int( '{{p3Step}}' )
    mergingName = '{{p3Name}}'
    mergingVisibility = '{{p3Vis}}'
    mergingCDb = '{{p3CDb}}'
    mergingDDDb = '{{p3DDDb}}'
    mergingOptions = '{{p3Opt}}'
    mergingVersion = '{{p3Ver}}'
    mergingEP = '{{p3EP}}'
    mergingPP = '{{p3Pass}}'

  elif MC1 and not selection:
    mergingApp = '{{p2App}}'
    mergingStep = int( '{{p2Step}}' )
    mergingName = '{{p2Name}}'
    mergingVisibility = '{{p2Vis}}'
    mergingCDb = '{{p2CDb}}'
    mergingDDDb = '{{p2DDDb}}'
    mergingOptions = '{{p2Opt}}'
    mergingVersion = '{{p2Ver}}'
    mergingEP = '{{p2EP}}'
    mergingPP = '{{p2Pass}}'

  else:
    gLogger.error( 'There\'s something wrong...' )
    DIRAC.exit( 2 )


  mergingInput = BKClient.getStepInputFiles( mergingStep )
  if not mergingInput:
    gLogger.error( 'Error getting res from BKK: %s', mergingInput['Message'] )
    DIRAC.exit( 2 )

  mergingInputList = [x[0].lower() for x in mergingInput['Value']['Records']]
  if len( mergingInputList ) > 1:
    gLogger.error( 'More than 1 merging output: %s', mergingInputList )
    DIRAC.exit( 2 )
  else:
    mergingInputType = mergingInputList[0]

  mergingOutput = BKClient.getStepOutputFiles( mergingStep )
  if not mergingOutput:
    gLogger.error( 'Error getting res from BKK: %s', mergingOutput['Message'] )
    DIRAC.exit( 2 )

  mergingOutputList = [x[0].lower() for x in mergingOutput['Value']['Records']]
  if len( mergingOutputList ) > 1:
    gLogger.error( 'More than 1 merging output: %s', mergingOutputList )
    DIRAC.exit( 2 )
  else:
    mergingType = mergingOutputList[0]







#################################################################################
# This is the start of the MC production definition (if requested)
#################################################################################


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
                         stepID = gaussStep, stepName = gaussName, stepVisible = gaussVisibility,
                         stepPass = gaussPP )

  if MC5 or MC4 or MC3 or MC2:

    MCProd.addBooleStep( booleVersion, booleType, booleOptions, extraPackages = booleEP,
                         condDBTag = booleCDb, ddDBTag = booleDDDb, outputSE = defaultOutputSE,
                         stepID = booleStep, stepName = booleName, stepVisible = booleVisibility,
                         stepPass = boolePP )

  if MC5 or MC4 or MC3:

    if threeSteps == 'Moore':
      MCProd.addMooreStep( mooreVersion, mooreType, mooreOptions, extraPackages = mooreEP, inputDataType = mooreInputType,
                           condDBTag = mooreCDb, ddDBTag = mooreDDDb, outputSE = defaultOutputSE,
                           stepID = mooreStep, stepName = mooreName, stepVisible = mooreVisibility,
                           stepPass = moorePP )

    elif threeSteps == 'Brunel':
      MCProd.addBrunelStep( brunelVersion, brunelType, brunelOptions, extraPackages = brunelEP, inputDataType = brunelInputType,
                             outputSE = brunelDataSE, condDBTag = brunelCDb, ddDBTag = brunelDDDb, extraOpts = brunelExtraOptions,
                             stepID = brunelStep, stepName = brunelName, stepVisible = brunelVisibility,
                             stepPass = brunelPP )

  if MC5 or MC4:

    if fourSteps == 'Brunel':
      MCProd.addBrunelStep( brunelVersion, brunelType, brunelOptions, extraPackages = brunelEP, inputDataType = brunelInputType,
                            outputSE = brunelDataSE, condDBTag = brunelCDb, ddDBTag = brunelDDDb, extraOpts = brunelExtraOptions,
                            stepID = brunelStep, stepName = brunelName, stepVisible = brunelVisibility,
                            stepPass = brunelPP )
    if fourSteps == 'DaVinci':
      MCProd.addDaVinciStep( davinciVersion, davinciType, davinciOptions, extraPackages = davinciEP, inputDataType = davinciInputType,
                             dataType = 'MC', extraOpts = daVinciExtraOptions,
                             outputSE = daVinciDataSE, condDBTag = davinciCDb, ddDBTag = davinciDDDb,
                             stepID = davinciStep, stepName = davinciName, stepVisible = davinciVisibility,
                             stepPass = davinciPP )

  if MC5:
    MCProd.addDaVinciStep( davinciVersion, davinciType, davinciOptions, extraPackages = davinciEP, inputDataType = davinciInputType,
                           dataType = 'MC', extraOpts = daVinciExtraOptions,
                           outputSE = daVinciDataSE, condDBTag = davinciCDb, ddDBTag = davinciDDDb,
                           stepID = davinciStep, stepName = davinciName, stepVisible = davinciVisibility,
                           stepPass = davinciPP )

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
  if outputsCERN:
    MCProd.setOutputMode( 'Any' )
  else:
    MCProd.setOutputMode( 'Local' )
  MCProd.setFileMask( outputFileMask )

  if targetSite:
    MCProd.setTargetSite( targetSite )

  if banTier1s:
    MCProd.banTier1s()

  if publishFlag == False and testFlag:
    gLogger.info( 'MC test will be launched locally with number of events set to %s.' % ( events ) )
    try:
      result = MCProd.runLocal()
      if result['OK']:
        gLogger.info( 'Template finished successfully' )
        DIRAC.exit( 0 )
      else:
        gLogger.error( 'Something wrong with execution!' )
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
    prodID = result['Value']
    msg = 'MC production %s successfully created ' % ( prodID )

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
# This is the start of the selection production definition (if requested)
#################################################################################


if not selection:
  gLogger.info( 'No selection requested' )
else:
  inputBKQuery = {
                  'FileType'                 : selectionInputType.upper(),
                  'EventType'                : evtType,
                  'ProductionID'             : int( prodID )
                  }

  selectionProd = Production()
  if sysConfig:
    try:
      selectionProd.setJobParameters( {"SystemConfig": sysConfig } )
    except:
      selectionProd.setSystemConfig( sysConfig )

  selectionProd.setProdType( 'DataStripping' )
  selectionName = 'Request_{{ID}}_%sMerging_{{pDsc}}_EventType%s_Prod%s_Files%sGB' % ( selectionInputType, evtType, prodID, mergingGroupSize )
  selectionProd.setWorkflowName( '%s_%s' % ( selectionName, appendName ) )
  selectionProd.setWorkflowDescription( 'MC workflow selection from a previous production.' )
  selectionProd.setBKParameters( configName, configVersion, '{{pDsc}}', '{{simDesc}}' )
  selectionProd.setDBTags( selectionCDb, selectionDDDb )

  selectionProd.addDaVinciStep( selectionVersion, selectionOutputType, selectionOptions, extraPackages = selectionEP,
                                inputDataType = selectionInputType, dataType = 'MC', extraOpts = selectionExtraOptions,
                                outputSE = daVinciDataSE, condDBTag = selectionCDb, ddDBTag = selectionDDDb,
                                stepID = selectionStep, stepName = selectionName, stepVisible = selectionVisibility,
                                stepPass = selectionPP )


  selectionProd.setInputBKSelection( inputBKQuery )
  try:
    selectionProd.addFinalizationStep( ['UploadOutputData',
                                        'FailoverRequest',
                                        'RemoveInputData',
                                        'UploadLogFile'] )
    selectionProd.setJobParameters( {"InputDataPolicy": 'protocol' } )
  except:
    selectionProd.addFinalizationStep( removeInputData = True )
    selectionProd.setInputDataPolicy( 'protocol' )

  selectionProd.setJobFileGroupSize( mergingGroupSize )
  selectionProd.setProdGroup( '{{pDsc}}' )
  selectionProd.setProdPriority( selectionPriority )

  #selectionProd.setFileMask( finalAppType.lower() )
  selectionProd.setProdPlugin( selectionPlugin )

  result = selectionProd.create( 
                                publish = publishFlag,
                                bkScript = BKscriptFlag,
                                requestID = int( requestID ),
                                reqUsed = mergingTracking,
                                transformation = False,
                                parentRequestID = int( parentReq ),
                              )
  if not result['OK']:
    gLogger.error( 'Error during selection production creation:\n%s\n' % ( result['Message'] ) )
    DIRAC.exit( 2 )

  if publishFlag:
    prodID = result['Value']
    msg = 'Selection production %s successfully created ' % ( prodID )
    if testFlag:
      diracProd.production( prodID, 'manual', printOutput = True )
      msg = msg + 'and started in manual mode.'
    else:
      diracProd.production( prodID, 'automatic', printOutput = True )
      msg = msg + 'and started in automatic mode.'
    gLogger.info( msg )

  else:
    prodID = 1
    gLogger.info( 'Selection production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, prodID ) )






#################################################################################
# This is the start of the merging production definition (if requested)
#################################################################################

if not merging:
  gLogger.info( 'No merging requested' )
else:

  inputBKQuery = {
                  'FileType'                 : mergingInputType.upper(),
                  'EventType'                : evtType,
                  'ProductionID'             : int( prodID )
                  }

  mergingProd = Production()
  if sysConfig:
    try:
      mergingProd.setJobParameters( {"SystemConfig": sysConfig } )
    except:
      mergingProd.setSystemConfig( sysConfig )

  mergingProd.setProdType( 'Merge' )
  mergingName = 'Request_{{ID}}_%sMerging_{{pDsc}}_EventType%s_Prod%s_Files%sGB' % ( mergingInputType, evtType, prodID, mergingGroupSize )
  mergingProd.setWorkflowName( '%s_%s' % ( mergingName, appendName ) )
  mergingProd.setWorkflowDescription( 'MC workflow for merging outputs from a previous production.' )
  mergingProd.setBKParameters( configName, configVersion, '{{pDsc}}', '{{simDesc}}' )
  mergingProd.setDBTags( mergingCDb, mergingDDDb )
  if mergingApp == 'LHCb':
    mergingProd.addMergeStep( mergingVersion, mergingOptions, extraPackages = mergingEP, eventType = '{{eventType}}',
                              inputDataType = mergingInputType, inputData = '',
                              condDBTag = mergingCDb, ddDBTag = mergingDDDb, outputSE = mergedDataSE,
                              stepID = mergingStep, stepName = mergingName, stepVisible = mergingVisibility,
                              stepPass = mergingPP )
  elif mergingApp == 'DaVinci':
    mergingProd.addDaVinciStep( mergingVersion, 'merge', mergingOptions, extraPackages = mergingEP, eventType = '{{eventType}}',
                                inputDataType = mergingInputType, inputData = '',
                                condDBTag = mergingCDb, ddDBTag = mergingDDDb, outputSE = mergedDataSE,
                                stepID = mergingStep, stepName = mergingName, stepVisible = mergingVisibility,
                                stepPass = mergingPP )
  else:
    gLogger.error( "No LHCb nor DaVinci in MC Merging...?" )
    DIRAC.exit( 2 )

  mergingProd.setInputBKSelection( inputBKQuery )
  try:
    mergingProd.addFinalizationStep( ['UploadOutputData',
                                      'FailoverRequest',
                                      'RemoveInputData',
                                      'UploadLogFile'] )
    mergingProd.setJobParameters( {"InputDataPolicy": 'download' } )
  except:
    mergingProd.addFinalizationStep( removeInputData = True )
    mergingProd.setInputDataPolicy( 'download' )

  mergingProd.setJobFileGroupSize( mergingGroupSize )
  mergingProd.setProdGroup( '{{pDsc}}' )
  mergingProd.setProdPriority( mergingPriority )

  #mergingProd.setFileMask( finalAppType.lower() )
  mergingProd.setProdPlugin( mergingPlugin )

  result = mergingProd.create( 
                              publish = publishFlag,
                              bkScript = BKscriptFlag,
                              requestID = int( requestID ),
                              reqUsed = mergingTracking,
                              transformation = False,
                              parentRequestID = int( parentReq ),
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




#################################################################################
# This is the start of the replication transformation definition (if requested)
#################################################################################


if not replicationFlag:
  gLogger.info( 'No replication requested' )
else:
  replicatedType = ''
  if MCEnabled:
    replicatedType = MCOutput
  if selection:
    replicatedType = selectionOutputType
  if merging:
    replicatedType = mergingType

  transBKQuery = {
                  'FileType'                 : replicatedType.upper(),
                  'ProductionID'             : int( prodID )
                  }

  transformation = Transformation()
  transformation.setType( 'Replication' )
  transformation.setTransformationName( 'ReplicationForProd' + str( prodID ) + '-Request' + '{{pDsc}}' + '-FileType=' + replicatedType + appendName )
  transformation.setTransformationGroup( '{{pDsc}}' )
  transformation.setDescription( 'ReplicationForProd' + str( prodID ) + '-Request' + '{{pDsc}}' + '-FileType=' + replicatedType + appendName )
  transformation.setLongDescription( 'ReplicationForProd' + str( prodID ) + '-Request' + '{{pDsc}}' + '-FileType=' + replicatedType + appendName )
  transformation.setPlugin( replicationPlugin )
  transformation.setBkQuery( transBKQuery )
  transformation.setAdditionalParam( 'TransformationFamily', parentReq )

  result = transformation.addTransformation()

  if not result['OK']:
    gLogger.error( 'Error during replication production creation:\n%s\n' % ( result['Message'] ) )
    DIRAC.exit( 2 )

  if publishFlag:
    prodID = result['Value']
    msg = 'Replication production %s successfully created ' % ( str( prodID ) )
    if testFlag:
      transformation.setStatus( 'Active' )
      transformation.setAgentType( 'Manual' )
      msg = msg + 'and started in manual mode.'
    else:
      transformation.setStatus( 'Active' )
      transformation.setAgentType( 'Automatic' )
      msg = msg + 'and started in automatic mode.'
    gLogger.info( msg )

  else:
    prodID = 1
    gLogger.info( 'Replication production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, prodID ) )



#################################################################################
# End of the template.
#################################################################################
