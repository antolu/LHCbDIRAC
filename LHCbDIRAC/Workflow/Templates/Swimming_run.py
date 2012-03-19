########################################################################
########################################################################

"""   The Swimming.py Template will handle swimming cases, with or without merging, 
      with or without replication

"""

__RCSID__ = "$Id$"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################

from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gLogger
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

gLogger = gLogger.getSubLogger( 'Swimming_run.py' )
BKClient = BookkeepingClient()

#################################################################################
# Below here is the production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
#from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation

###########################################
# Configurable parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True to create validation productions#False}}'

unifyMooreAndDV = '{{productionsCreated#GENERAL: Moore and DaVinci within the same job#True}}'

# workflow params for all productions
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc43-opt#ANY}}'
useOracle = '{{useOracle#GENERAL: Use Oracle#False}}'

# workflow params for all productions
destination = '{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch#ALL}}'
startRun = '{{RecoRunStart#GENERAL: run start, to set the start run#0}}'
endRun = '{{RecoRunEnd#GENERAL: run end, to set the end of the range#0}}'

#swimming (Moore) params
swimm_priority = '{{priority#PROD-swimming-Moore: priority#5}}'
swimmCPU = '{{swimmMaxCPUTime#PROD-swimming-Moore: Max CPU time in secs#1000000}}'
swimmPlugin = '{{swimmPluginType#PROD-swimming-Moore: plugin name#LHCbStandard}}'
swimmFilesPerJob = '{{swimmFilesPerJob#PROD-swimming-Moore: Group size or number of files per job#1}}'
unmergedStreamSE = '{{swimmStreamSE#PROD-swimming-Moore: output data SE (un-merged streams)#Tier1-DST}}'
swimmAncestorProd = '{{swimmAncestorProd#PROD-swimming-Moore: ancestor production if any#0}}'
swimmIDPolicy = '{{swimmIDPolicy#PROD-swimming-Moore: policy for input data access (download or protocol)#download}}'
swimmEOpts = '{{swimmEO#PROD-swimming-Moore: extra options#from Configurables import Swimming;Swimming().OutputFile="@{outputData}"}}'

#swimming (DaVinci) params
swimmCPU_DV = '{{swimmMaxCPUTime-DV#PROD-swimming-DaVinci: Max CPU time in secs#1000000}}'
swimmPlugin_DV = '{{swimmPluginType-DV#PROD-swimming-DaVinci: plugin name#LHCbStandard}}'
swimmFilesPerJob_DV = '{{swimmFilesPerJob-DV#PROD-swimming-DaVinci: Group size or number of files per job#1}}'
unmergedStreamSE_DV = '{{swimmStreamSE-DV#PROD-swimming-DaVinci: output data SE (un-merged streams)#Tier1-DST}}'
swimmAncestorProd_DV = '{{swimmAncestor-DVProd#PROD-swimming-DaVinci: ancestor production if any#0}}'
swimmIDPolicy_DV = '{{swimmIDPolicy-DV#PROD-swimming-DaVinci: policy for input data access (download or protocol)#download}}'
swimmEOpts_DV = '{{swimmEO_DV#PROD-swimming-DaVinci: extra options#from Configurables import Swimming;Swimming().OutputFile="@{outputData}";from Swimming.Configuration import ConfigureDaVinci;ConfigureDaVinci()}}'


#merging params
mergePriority = '{{MergePriority#PROD-Merging: priority#9}}'
mergePlugin = '{{MergePlugin#PROD-Merging: plugin#BySize}}'
mergeRemoveInputsFlag = '{{MergeRemoveFlag#PROD-Merging: remove input data flag True/False#True}}'
mergeCPU = '{{MergeMaxCPUTime#PROD-Merging: Max CPU time in secs#300000}}'
mergeFileSize = '{{MergeFileSize#PROD-Merging: Size (in GB) of the merged files#5}}'
mergeIDPolicy = '{{MergeIDPolicy#PROD-Merging: policy for input data access (download or protocol)#download}}'
mergedStreamSE = '{{MergeStreamSE#PROD-Merging: output data SE (merged streams)#Tier1_M-DST}}'
mergeEOpts = '{{mergeEO#PROD-Merging: extra options#}}'


###########################################
# Fixed and implied parameters 
###########################################

currentReqID = int( '{{ID}}' )
prodGroup = '{{pDsc}}'
#used in case of a test e.g. certification etc.
bkConfigName = '{{configName}}'
bkConfigVersion = '{{configVersion}}'
recoRunNumbers = '{{inProductionID}}'

#Other parameters from the request page
inDQFlag = '{{inDataQualityFlag}}'
dataTakingCond = '{{simDesc}}'
processingPass = '{{inProPass}}'
eventType = '{{eventType}}'
tck = '{{inTCKs}}'

mergeRemoveInputsFlag = eval( mergeRemoveInputsFlag )
certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
validationFlag = eval( validationFlag )
useOracle = eval( useOracle )
unifyMooreAndDV = eval( unifyMooreAndDV )

swimmEnabled = False
mergingEnabled = False

oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'
threeSteps = '{{p3App}}'
fourSteps = '{{p4App}}'

#####################################################
# Guessing what the request is for:
#####################################################

error = False
if fourSteps:
  gLogger.error( 'Four steps specified, not sure what to do! Exiting...' )
  error = True
elif threeSteps:
  if oneStep.lower() == 'moore' and twoSteps.lower() == 'davinci' and threeSteps.lower() in ( 'davinci', 'lhcb' ):
    gLogger.info( "swimming production + merging is requested..." )

    swimmEnabled = True

    swimmDQFlag = inDQFlag
    swimmStep = int( '{{p1Step}}' )
    swimmName = '{{p1Name}}'
    swimmVisibility = '{{p1Vis}}'
    swimmCDb = '{{p1CDb}}'
    swimmDDDb = '{{p1DDDb}}'
    swimmOptions = '{{p1Opt}}'
    swimmPass = '{{p1Pass}}'
    if useOracle:
      if not 'useoracle.py' in swimmOptions.lower():
        swimmOptions = swimmOptions + ';$APPCONFIGOPTS/UseOracle.py'
    swimmVersion = '{{p1Ver}}'
    swimmEP = '{{p1EP}}'
    swimmOF = ''

    swimmDVStep = int( '{{p2Step}}' )
    swimmDVName = '{{p2Name}}'
    swimmDVVisibility = '{{p2Vis}}'
    swimmDVCDb = '{{p2CDb}}'
    swimmDVDDDb = '{{p2DDDb}}'
    swimmDVOptions = '{{p2Opt}}'
    swimmDVPass = '{{p2Pass}}'
    swimmDVOF = ''
    if useOracle:
      if not 'useoracle.py' in swimmDVOptions.lower():
        swimmDVOptions = swimmDVOptions + ';$APPCONFIGOPTS/UseOracle.py'
    swimmDVVersion = '{{p2Ver}}'
    swimmDVEP = '{{p2EP}}'

    swimmFileType = '{{inFileType}}'

    mergingEnabled = True
    mergeApp = '{{p3App}}'
    mergeStep = int( '{{p3Step}}' )
    mergeName = '{{p3Name}}'
    mergeVisibility = '{{p3Vis}}'
    mergeCDb = '{{p3CDb}}'
    mergeDDDb = '{{p3DDDb}}'
    mergeOptions = '{{p3Opt}}'
    mergePass = '{{p3Pass}}'
    mergeOF = ''
    if mergeApp.lower() == 'davinci':
      if useOracle:
        if not 'useoracle.py' in mergeOptions.lower():
          mergeOptions = mergeOptions + ';$APPCONFIGOPTS/UseOracle.py'
    mergeVersion = '{{p3Ver}}'
    mergeEP = '{{p3EP}}'

  else:
    gLogger.error( 'First step is %s, second is %s, third is %s' % ( oneStep, twoSteps, threeSteps ) )
    error = True


elif twoSteps:
  if oneStep.lower() == 'moore' and twoSteps.lower() == 'davinci':
    gLogger.info( "swimming production without merging is requested..." )

    swimmEnabled = True

    swimmDQFlag = inDQFlag
    swimmStep = int( '{{p1Step}}' )
    swimmName = '{{p1Name}}'
    swimmVisibility = '{{p1Vis}}'
    swimmCDb = '{{p1CDb}}'
    swimmDDDb = '{{p1DDDb}}'
    swimmOptions = '{{p1Opt}}'
    swimmPass = '{{p1Pass}}'
    if useOracle:
      if not 'useoracle.py' in swimmOptions.lower():
        swimmOptions = swimmOptions + ';$APPCONFIGOPTS/UseOracle.py'
    swimmVersion = '{{p1Ver}}'
    swimmEP = '{{p1EP}}'
    swimmOF = ''

    swimmDVStep = int( '{{p2Step}}' )
    swimmDVName = '{{p2Name}}'
    swimmDVVisibility = '{{p2Vis}}'
    swimmDVCDb = '{{p2CDb}}'
    swimmDVDDDb = '{{p2DDDb}}'
    swimmDVOptions = '{{p2Opt}}'
    swimmDVPass = '{{p2Pass}}'
    if useOracle:
      if not 'useoracle.py' in swimmDVOptions.lower():
        swimmDVOptions = swimmDVOptions + ';$APPCONFIGOPTS/UseOracle.py'
    swimmDVVersion = '{{p2Ver}}'
    swimmDVEP = '{{p2EP}}'
    swimmDVOF = ''

    swimmFileType = '{{inFileType}}'

  else:
    gLogger.error( 'First step is %s, second is %s' % ( oneStep, twoSteps ) )
    error = True

elif oneStep:
  gLogger.error( 'One step only specified, not sure what to do! Exiting...' )
  error = True

if error:
  DIRAC.exit( 2 )





if certificationFlag or localTestFlag:
  testFlag = True
  if certificationFlag:
    publishFlag = True
  if localTestFlag:
    publishFlag = False
    mergingEnabled = False
else:
  publishFlag = True
  testFlag = False

recoInputDataList = []
swimmInputDataList = []

evtsPerJob = '-1'

if not publishFlag:
  swimmTestData = 'LFN:/lhcb/LHCb/Collision11/CHARMCOMPLETEEVENT.DST/00012586/0000/00012586_00000706_1.charmcompleteevent.dst'
  swimmInputDataList.append( swimmTestData )
  swimmIDPolicy = 'protocol'
  evtsPerJob = '5'
  BKscriptFlag = True
else:
  BKscriptFlag = False

if testFlag:
  outBkConfigName = 'certification'
  outBkConfigVersion = 'test'
  startRun = '75336'
  endRun = '75340'
  recoCPU = '100000'
  dataTakingCond = 'Beam3500GeV-VeloClosed-MagDown'
  processingPass = 'Real Data/Reco12/Stripping17'
  swimmFileType = 'CHARMCOMPLETEEVENT.DST'
  eventType = '90000000'
else:
  outBkConfigName = bkConfigName
  outBkConfigVersion = bkConfigVersion

if validationFlag:
  outBkConfigName = 'validation'

#################################################################################
# swimming
#################################################################################

if swimmEnabled:

  if not swimmOF:
    swimmOF = 'swimming'

  #################################################################################
  # swimming BK Query
  #################################################################################

  swimmInput = BKClient.getStepInputFiles( swimmStep )
  if not swimmInput:
    gLogger.error( 'Error getting res from BKK: %s', swimmInput['Message'] )
    DIRAC.exit( 2 )

  swimmInputList = [x[0].lower().strip() for x in swimmInput['Value']['Records']]
  if len( swimmInputList ) == 1:
    swimmInput = swimmInputList[0].strip()
  else:
    gLogger.error( 'Multiple inputs to swimming...?', swimmInput['Value']['Records'] )
    DIRAC.exit( 2 )

  swimmOutput = BKClient.getStepOutputFiles( swimmStep )
  if not swimmOutput:
    gLogger.error( 'Error getting res from BKK: %s', swimmOutput['Message'] )
    DIRAC.exit( 2 )

  swimmOutputList = [x[0].lower().strip() for x in swimmOutput['Value']['Records']]
  if len( swimmOutputList ) > 1:
    gLogger.error( 'Multiple outputs to swimming...?', swimmInput['Value']['Records'] )
    DIRAC.exit( 2 )
  else:
    swimmType = swimmOutputList[0].strip()
    swimmEO = []

  swimmDVInput = BKClient.getStepInputFiles( swimmDVStep )
  if not swimmDVInput:
    gLogger.error( 'Error getting res from BKK: %s', swimmDVInput['Message'] )
    DIRAC.exit( 2 )

  swimmDVInputList = [x[0].lower().strip() for x in swimmDVInput['Value']['Records']]
  if len( swimmDVInputList ) == 1:
    swimmDVInput = swimmDVInputList[0].strip()
  else:
    gLogger.error( 'Multiple inputs to swimming DV...?', swimmDVInput['Value']['Records'] )
    DIRAC.exit( 2 )

  swimmDVOutput = BKClient.getStepOutputFiles( swimmDVStep )
  if not swimmDVOutput:
    gLogger.error( 'Error getting res from BKK: %s', swimmDVOutput['Message'] )
    DIRAC.exit( 2 )

  swimmDVOutputList = [x[0].lower().strip() for x in swimmDVOutput['Value']['Records']]
  if len( swimmDVOutputList ) > 1:
    gLogger.error( 'Multiple outputs to swimming DV...?', swimmDVInput['Value']['Records'] )
    DIRAC.exit( 2 )
  else:
    swimmDVType = swimmDVOutputList[0].strip()
    swimmDVEO = []

  swimmInputBKQuery = {
                        'DataTakingConditions'     : dataTakingCond,
                        'ProcessingPass'           : processingPass,
                        'FileType'                 : swimmFileType.upper(),
                        'EventType'                : eventType,
                        'ConfigName'               : bkConfigName,
                        'ConfigVersion'            : bkConfigVersion,
                        'ProductionID'             : 0,
                        'DataQualityFlag'          : swimmDQFlag,
                        'Visible'                  : 'Yes',
                        'TCK'                      : tck
                        }


  if int( endRun ) and int( startRun ):
    if int( endRun ) < int( startRun ):
      gLogger.error( 'Your end run "%s" should be less than your start run "%s"!' % ( endRun, startRun ) )
      DIRAC.exit( 2 )

  if int( startRun ):
    swimmInputBKQuery['StartRun'] = int( startRun )
  if int( endRun ):
    swimmInputBKQuery['EndRun'] = int( endRun )

  #################################################################################
  # Create the swimming production
  #################################################################################

  production = Production()

  if not destination.lower() in ( 'all', 'any' ):
    gLogger.info( 'Forcing destination site %s for production' % ( destination ) )
    production.setTargetSite( destination )

  if sysConfig:
    production.setJobParameters( { 'SystemConfig': sysConfig } )

  production.setJobParameters( { 'CPUTime': swimmCPU } )
  production.setProdType( 'DataSwimming' )
  wkfName = 'Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID ) #Rest can be taken from the details in the monitoring
  production.setWorkflowName( 'SWIMMING_%s_%s' % ( wkfName, appendName ) )
  production.setWorkflowDescription( "%s real data swimming production." % ( prodGroup ) )
  production.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
  production.setInputBKSelection( swimmInputBKQuery )
  production.setDBTags( swimmCDb, swimmDDDb )
  production.setJobParameters( { 'InputDataPolicy': swimmIDPolicy } )
  production.setProdPlugin( swimmPlugin )

  production.addMooreStep( swimmVersion, swimmType, swimmOptions, eventType = eventType, extraPackages = swimmEP,
                           inputDataType = swimmInput.lower(), inputData = swimmInputDataList, numberOfEvents = evtsPerJob,
                           outputSE = unmergedStreamSE, extraOpts = swimmEOpts,
                           stepID = swimmStep, stepName = swimmName, stepVisible = swimmVisibility, stepPass = swimmPass,
                           optionsFormat = swimmOF )

  if unifyMooreAndDV:
    production.addDaVinciStep( swimmDVVersion, swimmDVType, swimmDVOptions, eventType = eventType, extraPackages = swimmDVEP,
                               inputDataType = swimmDVInput.lower(), numberOfEvents = evtsPerJob,
                               outputSE = unmergedStreamSE_DV, extraOpts = swimmEOpts_DV,
                               stepID = swimmDVStep, stepName = swimmDVName, stepVisible = swimmDVVisibility, stepPass = swimmDVPass,
                               optionsFormat = swimmDVOF )


  production.addFinalizationStep( 'UploadOutputData',
                                 'FailoverRequest',
                                 'UploadLogFile' )
  production.setProdGroup( prodGroup )
  production.setProdPriority( swimm_priority )
  production.setJobFileGroupSize( swimmFilesPerJob )
#  production.setFileMask(  )


  #################################################################################
  # Publishing of the swimming production
  #################################################################################

  if ( not publishFlag ) and ( testFlag ):

    gLogger.info( 'Production test will be launched with number of events set to %s.' % ( evtsPerJob ) )
    try:
      result = production.runLocal()
      if result['OK']:
        gLogger.info( 'Template finished successfully' )
        DIRAC.exit( 0 )
      else:
        gLogger.error( 'Something wrong with execution!' )
        DIRAC.exit( 2 )
    except Exception, x:
      gLogger.error( 'Production test failed with exception:\n%s' % ( x ) )
      DIRAC.exit( 2 )

  result = production.create( 
                             publish = publishFlag,
                             bkQuery = swimmInputBKQuery,
                             groupSize = swimmFilesPerJob,
                             bkScript = BKscriptFlag,
                             requestID = currentReqID,
                             reqUsed = 0,
                             transformation = False
                             )
  if not result['OK']:
    gLogger.error( 'Production creation failed with result:\n%s\ntemplate is exiting...' % ( result ) )
    DIRAC.exit( 2 )

  if publishFlag:
    diracProd = DiracProduction()

    swimmProdID = result['Value']

    msg = 'swimming production %s successfully created ' % ( swimmProdID )

    if testFlag:
      diracProd.production( swimmProdID, 'manual', printOutput = True )
      msg = msg + 'and started in manual mode.'
    else:
      diracProd.production( swimmProdID, 'automatic', printOutput = True )
      msg = msg + 'and started in automatic mode.'
    gLogger.info( msg )

  else:
    swimmProdID = 1
    gLogger.info( 'swimming production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, swimmProdID ) )


  if not unifyMooreAndDV:

    if not swimmDVOF:
      swimmDVOF = 'swimming'

    #################################################################################
    # swimming-DV BK Query
    #################################################################################

    swimmDVInputBKQuery = {
                          'FileType'                 : swimmDVInput.upper(),
                          'ProductionID'             : swimmProdID,
                          }

    #################################################################################
    # Create the swimming DV production
    #################################################################################

    DVProduction = Production()

    if not destination.lower() in ( 'all', 'any' ):
      gLogger.info( 'Forcing destination site %s for DV Production' % ( destination ) )
      DVProduction.setTargetSite( destination )

    if sysConfig:
      DVProduction.setJobParameters( { 'SystemConfig': sysConfig } )

    DVProduction.setJobParameters( { 'CPUTime': swimmCPU_DV } )
    DVProduction.setProdType( 'DataStripping' )
    wkfName = 'Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID ) #Rest can be taken from the details in the monitoring
    DVProduction.setWorkflowName( 'SWIMMING_DV_%s_%s' % ( wkfName, appendName ) )
    DVProduction.setWorkflowDescription( "%s real data swimming DV Production." % ( prodGroup ) )
    DVProduction.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
    DVProduction.setInputBKSelection( swimmDVInputBKQuery )
    DVProduction.setDBTags( swimmDVCDb, swimmDVDDDb )
    DVProduction.setJobParameters( { 'InputDataPolicy': swimmIDPolicy_DV } )
    DVProduction.setProdPlugin( swimmPlugin_DV )

    DVProduction.addDaVinciStep( swimmDVVersion, swimmDVType, swimmDVOptions, eventType = eventType, extraPackages = swimmDVEP,
                                   inputDataType = swimmDVInput.lower(), numberOfEvents = evtsPerJob, inputData = [],
                                   outputSE = unmergedStreamSE, extraOpts = swimmEOpts_DV,
                                   stepID = swimmDVStep, stepName = swimmDVName, stepVisible = swimmDVVisibility, stepPass = swimmDVPass,
                                   optionsFormat = swimmDVOF )


    DVProduction.addFinalizationStep( 'UploadOutputData',
                                      'FailoverRequest',
                                      'UploadLogFile' )
    DVProduction.setProdGroup( prodGroup )
    DVProduction.setProdPriority( swimm_priority )
    DVProduction.setJobFileGroupSize( swimmFilesPerJob_DV )
  #  DVProduction.setFileMask(  )

    #################################################################################
    # Publishing of the swimming DVProduction
    #################################################################################

    if ( not publishFlag ) and ( testFlag ):

      gLogger.info( 'Production test will be launched with number of events set to %s.' % ( evtsPerJob ) )
      try:
        result = DVProduction.runLocal()
        if result['OK']:
          gLogger.info( 'Template finished successfully' )
          DIRAC.exit( 0 )
        else:
          gLogger.error( 'Something wrong with execution!' )
          DIRAC.exit( 2 )
      except Exception, x:
        gLogger.error( 'Production test failed with exception:\n%s' % ( x ) )
        DIRAC.exit( 2 )

    result = DVProduction.create( 
                                 publish = publishFlag,
                                 bkQuery = swimmDVInputBKQuery,
                                 groupSize = swimmFilesPerJob_DV,
                                 bkScript = BKscriptFlag,
                                 requestID = currentReqID,
                                 reqUsed = 0,
                                 transformation = False
                               )
    if not result['OK']:
      gLogger.error( 'Production creation failed with result:\n%s\ntemplate is exiting...' % ( result ) )
      DIRAC.exit( 2 )

    if publishFlag:
      diracProd = DiracProduction()

      swimmDVProdID = result['Value']

      msg = 'swimming DV Production %s successfully created ' % ( swimmDVProdID )

      if testFlag:
        diracProd.production( swimmDVProdID, 'manual', printOutput = True )
        msg = msg + 'and started in manual mode.'
      else:
        diracProd.production( swimmDVProdID, 'automatic', printOutput = True )
        msg = msg + 'and started in automatic mode.'
      gLogger.info( msg )

    else:
      swimmDVProdID = 1
      gLogger.info( 'swimming DVProduction creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, swimmProdID ) )



#################################################################################
# Merging
#################################################################################

if mergingEnabled:

#  if not swimmEnabled:
#    swimmProdID = #put something here

  mergeInput = BKClient.getStepInputFiles( mergeStep )
  if not mergeInput:
    gLogger.error( 'Error getting res from BKK: %s', mergeInput['Message'] )
    DIRAC.exit( 2 )

  mergeInputList = [x[0].lower().strip() for x in mergeInput['Value']['Records']]

  mergeOutput = BKClient.getStepOutputFiles( mergeStep )
  if not mergeOutput:
    gLogger.error( 'Error getting res from BKK: %s', mergeOutput['Message'] )
    DIRAC.exit( 2 )

  mergeOutputList = [x[0].lower().strip() for x in mergeOutput['Value']['Records']]


  if swimmEnabled:
    if mergeInputList != swimmDVOutputList:
      gLogger.error( 'MergeInput %s != swimmDVOutput %s' % ( mergeInputList, swimmDVOutputList ) )
      DIRAC.exit( 2 )

  if mergeInputList != mergeOutputList:
    gLogger.error( 'MergeInput %s != mergeOutput %s' % ( mergeInputList, mergeOutputList ) )
    DIRAC.exit( 2 )

  mergeProductionList = []

  for mergeStream in mergeOutputList:
#    if mergeStream.lower() in onlyCERN:
#      mergeSE = 'CERN_M-DST'

    mergeStream = mergeStream.upper()

    if not mergeOF:
      mergeOF = 'merge'

    #################################################################################
    # Merging BK Query
    #################################################################################

    if unifyMooreAndDV:
      mergeInput = swimmProdID
    else:
      mergeInput = swimmDVProdID

    mergeBKQuery = {
                    'ProductionID'             : mergeInput,
                    'FileType'                 : mergeStream
                    }
      #below should be integrated in the ProductionOptions utility
#    if mergeApp.lower() == 'davinci':
#      dvExtraOptions = "from Configurables import RecordStream;"
#      dvExtraOptions += "FileRecords = RecordStream(\"FileRecords\");"
#      dvExtraOptions += "FileRecords.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'REC\'\""

    ###########################################
    # Create the merging production
    ###########################################


    mergeProd = Production()
    mergeProd.setJobParameters( { 'CPUTime': mergeCPU } )
    mergeProd.setProdType( 'Merge' )
    wkfName = 'Merging_Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID )
    mergeProd.setWorkflowName( '%s_%s_%s' % ( mergeStream.split( '.' )[0], wkfName, appendName ) )

    if sysConfig:
      mergeProd.setJobParameters( { 'SystemConfig': sysConfig } )

    mergeProd.setWorkflowDescription( 'Stream merging workflow for %s files from input production %s' % ( mergeStream, swimmProdID ) )
    mergeProd.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
    mergeProd.setDBTags( mergeCDb, mergeDDDb )

    if mergeApp.lower() == 'davinci':
      mergeProd.addDaVinciStep( mergeVersion, 'merge', mergeOptions, extraPackages = mergeEP, eventType = eventType,
                                inputDataType = mergeStream.lower(), extraOpts = mergeEOpts,
                                inputProduction = swimmProdID, inputData = [], outputSE = mergedStreamSE,
                                stepID = mergeStep, stepName = mergeName, stepVisible = mergeVisibility, stepPass = mergePass,
                                optionsFormat = mergeOF )
    elif mergeApp.lower() == 'lhcb':
      mergeProd.addMergeStep( mergeVersion, mergeOptions, swimmProdID, eventType, mergeEP, inputData = [],
                              inputDataType = mergeStream.lower(), outputSE = mergedStreamSE, extraOpts = mergeEOpts,
                              condDBTag = mergeCDb, ddDBTag = mergeDDDb, dataType = 'Data',
                              stepID = mergeStep, stepName = mergeName, stepVisible = mergeVisibility, stepPass = mergePass,
                              optionsFormat = mergeOF )
    else:
      gLogger.error( 'Merging is not DaVinci nor LHCb and is %s' % mergeApp )
      DIRAC.exit( 2 )

    if mergeRemoveInputsFlag:
      mergeProd.addFinalizationStep( 'UploadOutputData',
                                     'FailoverRequest',
                                     'RemoveInputData',
                                     'UploadLogFile' )
    else:
      mergeProd.addFinalizationStep( 'UploadOutputData',
                                     'FailoverRequest',
                                     'UploadLogFile' )
    mergeProd.setInputBKSelection( mergeBKQuery )
    mergeProd.setJobParameters( { 'InputDataPolicy': mergeIDPolicy } )
    mergeProd.setProdGroup( prodGroup )
    mergeProd.setProdPriority( mergePriority )
    mergeProd.setJobFileGroupSize( mergeFileSize )
    mergeProd.setFileMask( mergeStream.lower() )
    mergeProd.setProdPlugin( mergePlugin )

    result = mergeProd.create( 
                              publish = publishFlag,
                              bkScript = BKscriptFlag,
                              requestID = currentReqID,
                              reqUsed = 1,
                              transformation = False
                              )
    if not result['OK']:
      gLogger.error( 'Production creation failed with result:\n%s\ntemplate is exiting...' % ( result ) )
      DIRAC.exit( 2 )

    if publishFlag:
      diracProd = DiracProduction()

      prodID = result['Value']
      msg = 'Merging production %s for %s successfully created ' % ( prodID, mergeStream )

      if testFlag:
        diracProd.production( prodID, 'manual', printOutput = True )
        msg = msg + 'and started in manual mode.'
      else:
        diracProd.production( prodID, 'automatic', printOutput = True )
        msg = msg + 'and started in automatic mode.'
      gLogger.info( msg )

      mergeProductionList.append( int( prodID ) )

    else:
      prodID = 1
      gLogger.info( 'Merging production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, prodID ) )

gLogger.info( 'Template finished successfully.' )
DIRAC.exit( 0 )

#################################################################################
# End of the template.
#################################################################################
