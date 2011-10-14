########################################################################
# $HeadURL$
########################################################################

"""  The MC Simulation Template creates workflows for the following simulation
     use-cases:
     
     - Gauss [ + Merging, + Transformation ]
     - Gauss -> Boole [ + Merging, + Transformation ]
     - Gauss -> Boole -> Brunel [ + Merging, + Transformation ]
     - Gauss -> Boole -> Moore -> Brunel [ + Merging, + Transformation ]
     
     with the following parameters being configurable via the interface:
     
     - number of events
     - CPU time in seconds
     - final output file type 
     - number of jobs to extend the initial MC production 
     - resulting WMS priority for each job
     - BK config name and version
     - system configuration
     - output file mask (e.g. to retain intermediate step output)
     - merging priority, plugin and group size
     - whether or not to ban Tier-1 sites as destination
     - whether or not a merging production should be created
     - whether or not a transformation should be created
     - whether outputs should be uploaded to CERN only for testing
     - string to append to the production name
     
     The template explicitly forces the tags of the CondDB / DDDB to be set
     at each step.  
     
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

from DIRAC import gLogger, gConfig
gLogger = gLogger.getSubLogger( 'MC_Simulation_run.py' )

#################################################################################
# Below here is the actual production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

###########################################
# Configurable and fixed parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'

configName = '{{BKConfigName#GENERAL: BK configuration name e.g. MC #MC}}'
configVersion = '{{BKConfigVersion#GENERAL: BK configuration version e.g. MC09, 2009, 2010#MC10}}'

banTier1s = '{{WorkflowBanTier1s#GENERAL: Workflow ban Tier-1 sites for jobs Boolean True/False#True}}'
outputFileMask = '{{WorkflowOutputDataFileMask#GENERAL: Workflow file extensions to save (comma separated) e.g. DST,DIGI#ALLSTREAMS.DST}}'
outputsCERN = '{{WorkflowCERNOutputs#GENERAL: Workflow upload workflow output to CERN#False}}'
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc43-opt, ANY#slc4_ia32_gcc34}}'

events = '{{MCNumberOfEvents#PROD-MC: Number of events per job#1000}}'
cpu = '{{MCMaxCPUTime#PROD-MC: Max CPU time in secs#1000000}}'
priority = '{{MCPriority#PROD-MC: Production priority#4}}'
extend = '{{MCExtend#PROD-MC: extend production by this many jobs#100}}'
finalAppType = '{{MCFinalAppType#PROD-MC: final file type to produce and merge e.g. DST,XDST,GEN,SIM...#ALLSTREAMS.DST}}'
gaussExtraOptions = '{{gaussExtraOptions#PROD-MC: Gauss extra options (leave blank for default)#}}'
brunelExtraOptions = '{{brunelExtraOptions#PROD-MC: Brunel extra options (leave blank for default)#}}'
daVinciExtraOptions = '{{daVinciExtraOptions#PROD-MC: DaVinci extra options (leave blank for default)#}}'

mergingFlag = '{{MergingEnable#PROD-Merging: enable flag Boolean True/False#True}}' #True/False
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

###########################################
# LHCb conventions implied by the above
###########################################

if finalAppType.lower() == 'xdst':
  booleType = 'xdigi'
else:
  booleType = 'digi'

# This is SIM unless a one-step (+/- merging)
# Gauss production is requested, see below.
gaussAppType = 'sim'

replicationFlag = eval( replicationFlag )
mergingFlag = eval( mergingFlag )
banTier1s = eval( banTier1s )
outputsCERN = eval( outputsCERN )
certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )

if certificationFlag or localTestFlag:
  testFlag = True
  replicationFlag = False
  if certificationFlag:
    publishFlag = True
    mergingFlag = True
  if localTestFlag:
    publishFlag = False
    mergingFlag = False
else:
  publishFlag = True
  testFlag = False

defaultOutputSE = 'CERN-RDST'
brunelDataSE = 'CERN_MC_M-DST'
daVinciDataSE = 'CERN_MC_M-DST'

mcRequestTracking = 1
mcreplicationFlag = replicationFlag
if mergingFlag:
  mcRequestTracking = 0
  mcreplicationFlag = False

BKscriptFlag = False

# If we don't even publish the production, we assume we want to see if the BK scripts are OK 
if not publishFlag:
  BKscriptFlag = True

#In case we want just to test, we publish in the certification/test part of the BKK
if testFlag:
  configName = 'certification'
  configVersion = 'test'
  events = '3'
  mergingGroupSize = '1'
  mcreplicationFlag = False
  replicationFlag = False

#The below is in order to choose the right steps in the workflow automatically
#e.g. each number of steps maps to a unique number
mergingApp = 'LHCb'
mergingVersion = ''
mergingCondDB = ''
mergingDDDB = ''
mergingStepID = ''
mergingStepName = ''
mergingStepVisible = ''

oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'
threeSteps = '{{p3App}}'
fourSteps = '{{p4App}}'
fiveSteps = '{{p5App}}'
sixSteps = '{{p6App}}'
sevenSteps = '{{p7App}}'

# In case you want to set list (or a single) application for which the out data should be uploaded
# If not set, take all the produced, with the outputFileMask is always defined 
outputDataStep = ''

if sevenSteps:
  gLogger.error( 'Seven steps specified, not sure what to do! Exiting...' )
  DIRAC.exit( 2 )

if outputFileMask:
  maskList = [m.lower() for m in outputFileMask.replace( ' ', '' ).split( ',' )]
  if not finalAppType.lower() in maskList:
    maskList.append( finalAppType.lower() )
  outputFileMask = string.join( maskList, ';' )

###########################################
# Parameter passing, the bulk of the script
###########################################
production = Production()

if sysConfig:
  production.setJobParameters( { 'SystemConfig': sysConfig } )

production.setProdType( 'MCSimulation' )
wkfName = 'Request_{{ID}}_MC_{{simDesc}}_{{pDsc}}_EventType{{eventType}}_{{MCNumberOfEvents}}Events'

production.setWorkflowName( '%s_%s' % ( wkfName, appendName ) )
production.setBKParameters( configName, configVersion, '{{pDsc}}', '{{simDesc}}' )
production.setDBTags( '{{p1CDb}}', '{{p1DDDb}}' )
production.setSimulationEvents( events, eventNumberTotal )

decided = False

#To make editing the resulting script easier in all cases separate options from long function calls
gaussOpts = '{{p1Opt}}'
defaultEvtOpts = gConfig.getValue( '/Operations/Gauss/EvtOpts', '$DECFILESROOT/options/{{eventType}}.opts' )
if not defaultEvtOpts in gaussOpts:
  gaussOpts += ';%s' % defaultEvtOpts

#defaultGenOpts = gConfig.getValue( '/Operations/Gauss/Gen{{Generator}}Opts', '$LBBCVEGPYROOT/options/{{Generator}}.py' )
#if not defaultGenOpts in gaussOpts:
#  gaussOpts += ';%s' % defaultGenOpts

booleOpts = '{{p2Opt}}'
#Having Moore and Brunel at the third step means other Opts are defined later.

#Now try to guess what the request is actually asking for.



if sixSteps:
  if not sixSteps.lower() == mergingApp.lower():
    gLogger.error( 'Six steps requested but last is not %s merging, not sure what to do! Exiting...' % ( mergingApp ) )
    DIRAC.exit( 2 )

  prodDescription = 'A six step workflow Gauss->Boole->Moore->Brunel->DaVinci + Merging'
  production.addGaussStep( '{{p1Ver}}', '{{Generator}}', events, gaussOpts, eventType = '{{eventType}}',
                          extraPackages = '{{p1EP}}', condDBTag = '{{p1CDb}}', ddDBTag = '{{p1DDDb}}',
                          outputSE = defaultOutputSE, appType = gaussAppType, extraOpts = gaussExtraOptions,
                          stepID = '{{p1Step}}', stepName = '{{p1Name}}', stepVisible = '{{p1Vis}}' )
  production.addBooleStep( '{{p2Ver}}', booleType, booleOpts, extraPackages = '{{p2EP}}',
                          condDBTag = '{{p2CDb}}', ddDBTag = '{{p2DDDb}}', outputSE = defaultOutputSE,
                          stepID = '{{p2Step}}', stepName = '{{p2Name}}', stepVisible = '{{p2Vis}}' )
  mooreOpts = '{{p3Opt}}'
  production.addMooreStep( '{{p3Ver}}', booleType, mooreOpts, extraPackages = '{{p3EP}}', inputDataType = booleType,
                          condDBTag = '{{p3CDb}}', ddDBTag = '{{p3DDDb}}', outputSE = defaultOutputSE,
                          stepID = '{{p3Step}}', stepName = '{{p3Name}}', stepVisible = '{{p3Vis}}' )
  brunelOpts = '{{p4Opt}}'
  production.addBrunelStep( '{{p4Ver}}', 'dst', brunelOpts, extraPackages = '{{p4EP}}', inputDataType = booleType,
                           outputSE = brunelDataSE, condDBTag = '{{p4CDb}}', ddDBTag = '{{p4DDDb}}', extraOpts = brunelExtraOptions,
                           stepID = '{{p4Step}}', stepName = '{{p4Name}}', stepVisible = '{{p4Vis}}' )
  daVinciOpts = '{{p5Opt}}'
  production.addDaVinciStep( '{{p5Ver}}', finalAppType.lower(), daVinciOpts, extraPackages = '{{p5EP}}', inputDataType = 'dst',
                            dataType = 'MC', extraOpts = daVinciExtraOptions,
                            outputSE = daVinciDataSE, condDBTag = '{{p5CDb}}', ddDBTag = '{{p5DDDb}}',
                            stepID = '{{p5Step}}', stepName = '{{p5Name}}', stepVisible = '{{p5Vis}}' )
  mergingVersion = '{{p6Ver}}'
  mergingCondDB = '{{p6CDb}}'
  mergingDDDB = '{{p6DDDb}}'
  mergingStepID = '{{p6Step}}'
  mergingStepName = '{{p6Name}}'
  mergingStepVisible = '{{p6Vis}}'
  decided = True

if fiveSteps and not decided:
  if not mergingFlag and not localTestFlag:
    gLogger.error( 'Five steps requested (without merging flag being set to True) not sure what to do! Exiting...' )
    DIRAC.exit( 2 )
  if not fiveSteps.lower() == mergingApp.lower():
    gLogger.error( 'Five steps requested but last is not %s merging, not sure what to do! Exiting...' % ( mergingApp ) )
    DIRAC.exit( 2 )

  production.addGaussStep( '{{p1Ver}}', '{{Generator}}', events, gaussOpts, eventType = '{{eventType}}',
                          extraPackages = '{{p1EP}}', condDBTag = '{{p1CDb}}', ddDBTag = '{{p1DDDb}}',
                          outputSE = defaultOutputSE, appType = gaussAppType, extraOpts = gaussExtraOptions,
                          stepID = '{{p1Step}}', stepName = '{{p1Name}}', stepVisible = '{{p1Vis}}' )
  production.addBooleStep( '{{p2Ver}}', booleType, booleOpts, extraPackages = '{{p2EP}}',
                          condDBTag = '{{p2CDb}}', ddDBTag = '{{p2DDDb}}', outputSE = defaultOutputSE,
                          stepID = '{{p2Step}}', stepName = '{{p2Name}}', stepVisible = '{{p2Vis}}' )
  if threeSteps.lower() == 'moore' and fourSteps.lower() == 'brunel':
    prodDescription = 'A five step workflow Gauss->Boole->Moore->Brunel + Merging'
    mooreOpts = '{{p3Opt}}'
    production.addMooreStep( '{{p3Ver}}', booleType, mooreOpts, extraPackages = '{{p3EP}}', inputDataType = booleType,
                            condDBTag = '{{p3CDb}}', ddDBTag = '{{p3DDDb}}', outputSE = defaultOutputSE,
                            stepID = '{{p3Step}}', stepName = '{{p3Name}}', stepVisible = '{{p3Vis}}' )
    brunelOpts = '{{p4Opt}}'
    production.addBrunelStep( '{{p4Ver}}', finalAppType.lower(), brunelOpts, extraPackages = '{{p4EP}}', inputDataType = booleType,
                              outputSE = brunelDataSE, condDBTag = '{{p4CDb}}', ddDBTag = '{{p4DDDb}}', extraOpts = brunelExtraOptions,
                              stepID = '{{p4Step}}', stepName = '{{p4Name}}', stepVisible = '{{p4Vis}}' )
  elif threeSteps.lower() == 'brunel' and fourSteps.lower() == 'davinci':
    prodDescription = 'A five step workflow Gauss->Boole->Brunel->DaVinci + Merging'
    brunelOpts = '{{p3Opt}}'
    production.addBrunelStep( '{{p3Ver}}', finalAppType.lower(), brunelOpts, extraPackages = '{{p3EP}}', inputDataType = booleType,
                             outputSE = brunelDataSE, condDBTag = '{{p3CDb}}', ddDBTag = '{{p3DDDb}}',
                             stepID = '{{p3Step}}', stepName = '{{p3Name}}', stepVisible = '{{p3Vis}}' )
    daVinciOpts = '{{p4Opt}}'
    production.addDaVinciStep( '{{p4Ver}}', finalAppType.lower(), daVinciOpts, extraPackages = '{{p4EP}}', inputDataType = 'dst',
                              dataType = 'MC',
                              outputSE = daVinciDataSE, condDBTag = '{{p4CDb}}', ddDBTag = '{{p4DDDb}}',
                             stepID = '{{p4Step}}', stepName = '{{p4Name}}', stepVisible = '{{p4Vis}}' )
  mergingVersion = '{{p5Ver}}'
  merginCondDB = '{{p5CDb}}'
  mergingDDDB = '{{p5DDDb}}'
  mergingStepID = '{{p5Step}}'
  mergingStepName = '{{p5Name}}'
  mergingStepVisible = '{{p5Vis}}'
  decided = True

if fourSteps and not decided:
  if not mergingFlag and not localTestFlag:
    gLogger.error( 'Four steps requested (without merging flag being set to True) not sure what to do! Exiting...' )
    DIRAC.exit( 2 )
  if not fourSteps.lower() == mergingApp.lower():
    gLogger.error( 'Four steps requested but last is not %s merging, not sure what to do! Exiting...' % ( mergingApp ) )
    DIRAC.exit( 2 )

  production.addGaussStep( '{{p1Ver}}', '{{Generator}}', events, gaussOpts, eventType = '{{eventType}}',
                          extraPackages = '{{p1EP}}', condDBTag = '{{p1CDb}}', ddDBTag = '{{p1DDDb}}',
                          outputSE = defaultOutputSE, appType = gaussAppType, extraOpts = gaussExtraOptions,
                          stepID = '{{p1Step}}', stepName = '{{p1Name}}', stepVisible = '{{p1Vis}}' )
  if twoSteps == 'Boole' and threeSteps == 'Brunel':
    prodDescription = 'A four steps workflow Gauss->Boole->Brunel + Merging'
    production.addBooleStep( '{{p2Ver}}', booleType, booleOpts, extraPackages = '{{p2EP}}',
                            condDBTag = '{{p2CDb}}', ddDBTag = '{{p2DDDb}}', outputSE = defaultOutputSE,
                            stepID = '{{p2Step}}', stepName = '{{p2Name}}', stepVisible = '{{p2Vis}}' )
    brunelOpts = '{{p3Opt}}'
    production.addBrunelStep( '{{p3Ver}}', finalAppType.lower(), brunelOpts, extraPackages = '{{p3EP}}', inputDataType = booleType,
                             outputSE = brunelDataSE, condDBTag = '{{p3CDb}}', ddDBTag = '{{p3DDDb}}', extraOpts = brunelExtraOptions,
                             stepID = '{{p3Step}}', stepName = '{{p3Name}}', stepVisible = '{{p3Vis}}' )

  elif twoSteps == 'Boole' and threeSteps == 'Moore':
    prodDescription = 'A four steps workflow Gauss->Boole->Moore + Merging'
    production.addBooleStep( '{{p2Ver}}', booleType, booleOpts, extraPackages = '{{p2EP}}',
                            condDBTag = '{{p2CDb}}', ddDBTag = '{{p2DDDb}}', outputSE = defaultOutputSE,
                            stepID = '{{p2Step}}', stepName = '{{p2Name}}', stepVisible = '{{p2Vis}}' )
    mooreOpts = '{{p3Opt}}'
    production.addMooreStep( '{{p3Ver}}', finalAppType.lower(), mooreOpts, extraPackages = '{{p3EP}}', inputDataType = booleType,
                            condDBTag = '{{p3CDb}}', ddDBTag = '{{p3DDDb}}', outputSE = defaultOutputSE,
                            stepID = '{{p3Step}}', stepName = '{{p3Name}}', stepVisible = '{{p3Vis}}' )

  else:
    gLogger.error( 'Not sure what to do! Exiting...' )
    DIRAC.exit( 2 )

  mergingVersion = '{{p4Ver}}'
  merginCondDB = '{{p4CDb}}'
  mergingDDDB = '{{p4DDDb}}'
  mergingStepID = '{{p4Step}}'
  mergingStepName = '{{p4Name}}'
  mergingStepVisible = '{{p4Vis}}'
  decided = True



if threeSteps and not decided:
  if mergingFlag:
    gLogger.error( 'Three steps requested (with merging flag being set to True) not sure what to do! Exiting...' )
    DIRAC.exit( 2 )

  production.addGaussStep( '{{p1Ver}}', '{{Generator}}', events, gaussOpts, eventType = '{{eventType}}',
                          extraPackages = '{{p1EP}}', condDBTag = '{{p1CDb}}', ddDBTag = '{{p1DDDb}}',
                          outputSE = defaultOutputSE, appType = gaussAppType, extraOpts = gaussExtraOptions,
                          stepID = '{{p1Step}}', stepName = '{{p1Name}}', stepVisible = '{{p1Vis}}' )

  if twoSteps == 'Boole' and threeSteps == 'Brunel':
    prodDescription = 'A three steps workflow Gauss->Boole->Brunel'
    production.addBooleStep( '{{p2Ver}}', booleType, booleOpts, extraPackages = '{{p2EP}}',
                            condDBTag = '{{p2CDb}}', ddDBTag = '{{p2DDDb}}', outputSE = defaultOutputSE,
                            stepID = '{{p2Step}}', stepName = '{{p2Name}}', stepVisible = '{{p2Vis}}' )
    brunelOpts = '{{p3Opt}}'
    production.addBrunelStep( '{{p3Ver}}', finalAppType.lower(), brunelOpts, extraPackages = '{{p3EP}}', inputDataType = booleType,
                             outputSE = brunelDataSE, condDBTag = '{{p3CDb}}', ddDBTag = '{{p3DDDb}}', extraOpts = brunelExtraOptions,
                             stepID = '{{p3Step}}', stepName = '{{p3Name}}', stepVisible = '{{p3Vis}}' )

  elif twoSteps == 'Boole' and threeSteps == 'Moore':
    prodDescription = 'A four steps workflow Gauss->Boole->Moore'
    production.addBooleStep( '{{p2Ver}}', booleType, booleOpts, extraPackages = '{{p2EP}}',
                            condDBTag = '{{p2CDb}}', ddDBTag = '{{p2DDDb}}', outputSE = defaultOutputSE,
                            stepID = '{{p2Step}}', stepName = '{{p2Name}}', stepVisible = '{{p2Vis}}' )
    mooreOpts = '{{p3Opt}}'
    production.addMooreStep( '{{p3Ver}}', finalAppType.lower(), mooreOpts, extraPackages = '{{p3EP}}', inputDataType = booleType,
                            condDBTag = '{{p3CDb}}', ddDBTag = '{{p3DDDb}}', outputSE = defaultOutputSE,
                            stepID = '{{p3Step}}', stepName = '{{p3Name}}', stepVisible = '{{p3Vis}}' )

  else:
    gLogger.error( 'Not sure what to do! Exiting...' )
    DIRAC.exit( 2 )

  decided = True


if twoSteps and not decided:
  gLogger.error( '2 steps requested, not sure what to do! Exiting...' )
  DIRAC.exit( 2 )








if oneStep and not decided:
  prodDescription = 'Assuming one step workflow of Gauss only without merging'
  gaussAppType = finalAppType
  production.addGaussStep( '{{p1Ver}}', '{{Generator}}', events, gaussOpts, eventType = '{{eventType}}',
                          extraPackages = '{{p1EP}}', condDBTag = '{{p1CDb}}', ddDBTag = '{{p1DDDb}}',
                          outputSE = defaultOutputSE, appType = gaussAppType, extraOpts = gaussExtraOptions,
                          stepID = '{{p1Step}}', stepName = '{{p1Name}}', stepVisible = '{{p1Vis}}' )
  mergingFlag = False
  decided = True


# Finally, in case none of the above were eligible.
if not decided:
  gLogger.error( 'None of the understood application configurations were understood by this template. Exiting...' )
  DIRAC.exit( 2 )


prodDescription = '%s for BK %s %s event type %s with %s events per job and final\
                   application file type %s.' % ( prodDescription, configName, configVersion, evtType, events, finalAppType )
gLogger.info( prodDescription )
production.setWorkflowDescription( prodDescription )
production.addFinalizationStep( ['UploadOutputData',
                                 'FailoverRequest',
                                 'UploadLogFile'] )
production.setJobParameters( { 'CPUTime': cpu } )
production.setProdGroup( '{{pDsc}}' )
production.setProdPriority( priority )
production.setOutputMode( 'Any' )
production.setFileMask( outputFileMask )

if banTier1s:
  production.banTier1s()

#################################################################################
# End of production API script, now what to do with the production object
#################################################################################

if publishFlag == False and testFlag:
  gLogger.info( 'MC Production test will be launched locally with number of events set to %s.' % ( events ) )
  try:
    result = production.runLocal()
    if result['OK']:
      DIRAC.exit( 0 )
    else:
      DIRAC.exit( 2 )
  except Exception, x:
    gLogger.error( 'Production test failed with exception:\n%s' % ( x ) )
    DIRAC.exit( 2 )

result = production.create( 
                           publish = publishFlag,
                           requestID = int( requestID ),
                           reqUsed = mcRequestTracking,
                           transformation = mcreplicationFlag,
                           bkScript = BKscriptFlag,
                           parentRequestID = int( parentReq )
                           )

if not result['OK']:
  gLogger.error( 'Error during production creation:\n%s\ncheck that the wkf name is unique.' % ( result['Message'] ) )
  DIRAC.exit( 2 )

if publishFlag:
  diracProd = DiracProduction()

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

if not mergingFlag:
  gLogger.info( 'No merging requested for production ID %s.' % ( prodID ) )
  DIRAC.exit( 0 )

#################################################################################
# This is the start of the merging production definition (if requested)
#################################################################################

inputBKQuery = { 'SimulationConditions'     : 'All',
                 'DataTakingConditions'     : 'All',
                 'ProcessingPass'           : 'All',
                 'FileType'                 : finalAppType.upper(),
                 'EventType'                : evtType,
                 'ConfigName'               : 'All',
                 'ConfigVersion'            : 'All',
                 'ProductionID'             : int( prodID ),
                 'DataQualityFlag'          : 'All'}

mergeProd = Production()
if sysConfig:
  mergeProd.setJobParameters( {"SystemConfig": sysConfig } )

mergeProd.setProdType( 'Merge' )
mergingName = 'Request_{{ID}}_%sMerging_{{pDsc}}_EventType%s_Prod%s_Files%sGB' % ( finalAppType, evtType, prodID, mergingGroupSize )
mergeProd.setWorkflowName( mergingName )
mergeProd.setWorkflowDescription( 'MC workflow for merging outputs from a previous production.' )
mergeProd.setBKParameters( configName, configVersion, '{{pDsc}}', '{{simDesc}}' )
mergeProd.setDBTags( mergingCondDB, mergingDDDB )
mergeProd.addMergeStep( mergingVersion, eventType = '{{eventType}}', inputDataType = finalAppType.lower(),
                       inputData = [], condDBTag = mergingCondDB, ddDBTag = mergingDDDB,
                       stepID = mergingStepID, stepName = mergingStepName, stepVisible = mergingStepVisible )
mergeProd.addFinalizationStep( ['UploadOutputData',
                                'FailoverRequest',
                                'RemoveInputData',
                                'UploadLogFile'] )
mergeProd.setInputBKSelection( inputBKQuery )
mergeProd.setJobParameters( {"InputDataPolicy": 'download' } )
mergeProd.setJobFileGroupSize( mergingGroupSize )
mergeProd.setProdGroup( '{{pDsc}}' )
mergeProd.setProdPriority( mergingPriority )

#mergeProd.setFileMask( finalAppType.lower() )
mergeProd.setProdPlugin( mergingPlugin )

result = mergeProd.create( 
                          publish = publishFlag,
                          bkScript = BKscriptFlag,
                          requestID = int( requestID ),
                          reqUsed = 1,
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

if not replicationFlag:
  gLogger.info( 'No transformation requested for production ID %s.' % ( prodID ) )
  DIRAC.exit( 0 )

#################################################################################
# End of the template.
#################################################################################
