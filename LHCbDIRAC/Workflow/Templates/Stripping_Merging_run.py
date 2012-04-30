"""   The Stripping_Merging.py Template will handle stripping cases, with or without merging, 
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

gLogger = gLogger.getSubLogger( 'Stripping_Merging_run.py' )
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

# workflow params for all productions
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc43-opt#x86_64-slc5-gcc46-opt}}'

# workflow params for all productions
destination = '{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch#ALL}}'
startRun = '{{RecoRunStart#GENERAL: run start, to set the start run#0}}'
endRun = '{{RecoRunEnd#GENERAL: run end, to set the end of the range#0}}'
evtsPerJob = '{{NumberEvents#GENERAL: number of events per job (set to something small for a test)#-1}}'
strippRuns = '{{strippRuns#GENERAL: discrete list of run numbers (do not mix with start/endrun)#}}'

#stripp params
stripp_priority = '{{priority#PROD-Stripping: priority#5}}'
strippCPU = '{{StrippMaxCPUTime#PROD-Stripping: Max CPU time in secs#1000000}}'
strippPlugin = '{{StrippPluginType#PROD-Stripping: plugin name#ByRunWithFlush}}'
strippFilesPerJob = '{{StrippFilesPerJob#PROD-Stripping: Group size or number of files per job#2}}'
strippTransFlag = '{{StrippTransformation#PROD-Stripping: distribute output data True/False (False if merging)#False}}'
unmergedStreamSE = '{{StrippStreamSE#PROD-Stripping: output data SE (un-merged streams)#Tier1-DST}}'
strippAncestorProd = '{{StrippAncestorProd#PROD-Stripping: ancestor production if any#0}}'
strippIDPolicy = '{{strippIDPolicy#PROD-Stripping: policy for input data access (download or protocol)#download}}'
strippEOpts = '{{strippEO#PROD-Stripping: extra options#}}'

#merging params
mergeDQFlag = '{{MergeDQFlag#PROD-Merging: DQ Flag e.g. OK,UNCHECKED#OK}}'
mergePriority = '{{MergePriority#PROD-Merging: priority#8}}'
mergePlugin = '{{MergePlugin#PROD-Merging: plugin#MergeByRunWithFlush}}'
mergeRemoveInputsFlag = '{{MergeRemoveFlag#PROD-Merging: remove input data flag True/False#True}}'
mergeCPU = '{{MergeMaxCPUTime#PROD-Merging: Max CPU time in secs#300000}}'
mergeFileSize = '{{MergeFileSize#PROD-Merging: Size (in GB) of the merged files#5}}'
mergeIDPolicy = '{{MergeIDPolicy#PROD-Merging: policy for input data access (download or protocol)#download}}'
mergedStreamSE = '{{MergeStreamSE#PROD-Merging: output data SE (merged streams)#Tier1_M-DST}}'
mergeEOpts = '{{MergeEO#PROD-Merging: extra options#}}'

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

mergeRemoveInputsFlag = eval( mergeRemoveInputsFlag )
certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
validationFlag = eval( validationFlag )
strippTransFlag = eval( strippTransFlag )

strippEnabled = False
mergingEnabled = False

oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'
threeSteps = '{{p3App}}'

#####################################################
# Guessing what the request is for:
#####################################################

error = False
if threeSteps:
  gLogger.error( 'Three steps specified, not sure what to do! Exiting...' )
  error = True
elif twoSteps:
  if oneStep.lower() == 'davinci' and twoSteps.lower() in ( 'lhcb', 'davinci' ):
    gLogger.info( "Stripping/streaming production + merging is requested..." )

    strippEnabled = True
    strippDQFlag = inDQFlag
    strippStep = int( '{{p1Step}}' )
    strippName = '{{p1Name}}'
    strippVisibility = '{{p1Vis}}'
    strippCDb = '{{p1CDb}}'
    strippDDDb = '{{p1DDDb}}'
    strippOptions = '{{p1Opt}}'
    stripPass = '{{p1Pass}}'
    stripOF = BKClient.getAvailableSteps( {'StepId':int( '{{p1Step}}' )} )['Value']['Records'][0][12]

    strippVersion = '{{p1Ver}}'
    strippEP = '{{p1EP}}'
    strippFileType = '{{inFileType}}'

    mergingEnabled = True
    mergeApp = '{{p2App}}'
    mergeStep = int( '{{p2Step}}' )
    mergeName = '{{p2Name}}'
    mergeVisibility = '{{p2Vis}}'
    mergeCDb = '{{p2CDb}}'
    mergeDDDb = '{{p2DDDb}}'
    mergeOptions = '{{p2Opt}}'
    mergePass = '{{p2Pass}}'
    mergeOF = BKClient.getAvailableSteps( {'StepId':int( '{{p2Step}}' )} )['Value']['Records'][0][12]
    mergeVersion = '{{p2Ver}}'
    mergeEP = '{{p2EP}}'

  else:
    gLogger.error( 'First step is %s, second is %s' % ( oneStep, twoSteps ) )
    error = True

elif oneStep:
  #FIXME
  #merge single cases?
  if oneStep.lower() == 'davinci':
    gLogger.info( "Stripping/streaming production without merging is requested..." )
    strippEnabled = True
    strippDQFlag = inDQFlag
    strippStep = int( '{{p1Step}}' )
    strippName = '{{p1Name}}'
    strippVisibility = '{{p1Vis}}'
    strippCDb = '{{p1CDb}}'
    strippDDDb = '{{p1DDDb}}'
    strippOptions = '{{p1Opt}}'
    stripPass = '{{p1Pass}}'
    stripOF = BKClient.getAvailableSteps( {'StepId':int( '{{p1Step}}' )} )['Value']['Records'][0][12]
    strippVersion = '{{p1Ver}}'
    strippEP = '{{p1EP}}'
    strippFileType = '{{inFileType}}'

  elif oneStep.lower() == 'lhcb':
    mergingEnabled = True
    mergeApp = '{{p1App}}'
    mergeStep = int( '{{p1Step}}' )
    mergeName = '{{p1Name}}'
    mergeVisibility = '{{p1Vis}}'
    mergeCDb = '{{p1CDb}}'
    mergeDDDb = '{{p1DDDb}}'
    mergeOptions = '{{p1Opt}}'
    mergePass = '{{p1Pass}}'
    mergeOF = BKClient.getAvailableSteps( {'StepId':int( '{{p1Step}}' )} )['Value']['Records'][0][12]
    mergeVersion = '{{p1Ver}}'
    mergeEP = '{{p1EP}}'


  else:
    gLogger.error( 'Stripping/streaming step is NOT daVinci and is %s' % oneStep )
    error = True

if error:
  DIRAC.exit( 2 )





if certificationFlag or localTestFlag:
  testFlag = True
  if certificationFlag:
    publishFlag = True
    mergingFlag = True
  if localTestFlag:
    publishFlag = False
    mergingFlag = False
else:
  publishFlag = True
  testFlag = False

recoInputDataList = []
strippInputDataList = []
mergeInputDataList = []

if not publishFlag:
  strippTestData = 'LFN:/lhcb/data/2010/SDST/00008375/0001/00008375_00016947_1.sdst'
  strippInputDataList.append( strippTestData )
#  strippTestDataRAW = 'LFN:/lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/75338/075338_0000000069.raw'
#  strippInputDataList.append( strippTestDataRAW )
  strippIDPolicy = 'protocol'
  evtsPerJob = '2000'
  #mergeInputDataList = ['put something here']
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
  processingPass = 'Reco08/Stripping12b'
  strippFileType = 'SDST'
  eventType = '90000000'
else:
  outBkConfigName = bkConfigName
  outBkConfigVersion = bkConfigVersion

if validationFlag:
  outBkConfigName = 'validation'

#################################################################################
# Stripping
#################################################################################

if strippEnabled:

  if not stripOF:
    stripOF = 'stripping'

  #################################################################################
  # Stripping BK Query
  #################################################################################

  strippInput = BKClient.getStepInputFiles( strippStep )
  if not strippInput:
    gLogger.error( 'Error getting res from BKK: %s', strippInput['Message'] )
    DIRAC.exit( 2 )

  strippInputList = [x[0].lower() for x in strippInput['Value']['Records']]
  if len( strippInputList ) == 1:
    strippInput = strippInputList[0]
  else:
    gLogger.error( 'Multiple inputs to stripping...?', strippInput['Message'] )
    DIRAC.exit( 2 )

  strippOutput = BKClient.getStepOutputFiles( strippStep )
  if not strippOutput:
    gLogger.error( 'Error getting res from BKK: %s', strippOutput['Message'] )
    DIRAC.exit( 2 )

  strippOutputList = [x[0].lower() for x in strippOutput['Value']['Records']]
  if len( strippOutputList ) > 1:
    strippType = 'stripping'
    strippEO = strippOutputList
  else:
    strippType = strippOutputList[0]
    strippEO = []

  histFlag = False
  for sOL in strippOutputList:
    if 'HIST' in sOL:
      histFlag = True

  strippDQFlag = strippDQFlag.replace( ',', ';;;' ).replace( ' ', '' )

  strippInputBKQuery = {
                        'DataTakingConditions'     : dataTakingCond,
                        'ProcessingPass'           : processingPass,
                        'FileType'                 : strippFileType,
                        'EventType'                : eventType,
                        'ConfigName'               : bkConfigName,
                        'ConfigVersion'            : bkConfigVersion,
                        'ProductionID'             : 0,
                        'DataQualityFlag'          : strippDQFlag,
                        'Visible'                  : 'Yes'
                        }


  if int( endRun ) and int( startRun ):
    if int( endRun ) < int( startRun ):
      gLogger.error( 'Your end run "%s" should be less than your start run "%s"!' % ( endRun, startRun ) )
      DIRAC.exit( 2 )

  if int( startRun ):
    strippInputBKQuery['StartRun'] = int( startRun )
  if int( endRun ):
    strippInputBKQuery['EndRun'] = int( endRun )

  if strippRuns:
    strippInputBKQuery['RunNumbers'] = strippRuns.replace( ',', ';;;' ).replace( ' ', '' )


  #################################################################################
  # Create the stripping production
  #################################################################################

  production = Production( BKKClientIn = BKClient )

  if not destination.lower() in ( 'all', 'any' ):
    gLogger.info( 'Forcing destination site %s for production' % ( destination ) )
    production.setTargetSite( destination )

  if sysConfig:
    production.setJobParameters( { 'SystemConfig': sysConfig } )

  production.setJobParameters( { 'CPUTime': strippCPU } )
  production.setProdType( 'DataStripping' )
  wkfName = 'Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID ) #Rest can be taken from the details in the monitoring
  production.setWorkflowName( 'STRIPPING_%s_%s' % ( wkfName, appendName ) )
  production.setWorkflowDescription( "%s real data stripping production." % ( prodGroup ) )
  production.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
  production.setInputBKSelection( strippInputBKQuery )
  production.setDBTags( strippCDb, strippDDDb )
  production.setJobParameters( {'InputDataPolicy': strippIDPolicy } )
  production.setProdPlugin( strippPlugin )

  if strippInput.lower() == 'sdst':
    production.LHCbJob.setAncestorDepth( 2 )

  production.addDaVinciStep( strippVersion, strippType, strippOptions, eventType = eventType, extraPackages = strippEP,
                             inputDataType = strippInput.lower(), inputData = strippInputDataList, numberOfEvents = evtsPerJob,
                             dataType = 'Data',
                             outputSE = unmergedStreamSE,
                             histograms = histFlag, extraOutput = strippEO, extraOpts = strippEOpts,
                             stepID = strippStep, stepName = strippName, stepVisible = strippVisibility, stepPass = stripPass,
                             optionsFormat = stripOF )

  production.addFinalizationStep( ['UploadOutputData',
                                   'FailoverRequest',
                                   'UploadLogFile'] )
  production.setProdGroup( prodGroup )
  production.setProdPriority( stripp_priority )
  production.setJobFileGroupSize( strippFilesPerJob )
#  production.setFileMask(  )


  #################################################################################
  # Publishing of the stripping production
  #################################################################################

  if ( not publishFlag ) and ( testFlag ):

    gLogger.info( 'Production test will be launched with number of events set to %s.' % ( evtsPerJob ) )
    try:
      result = production.runLocal()
      gLogger.info( 'Template finished successfully' )
      if result['OK']:
        DIRAC.exit( 0 )
      else:
        gLogger.error( 'Something wrong with execution!' )
        DIRAC.exit( 2 )
    except Exception, x:
      gLogger.error( 'Production test failed with exception:\n%s' % ( x ) )
      DIRAC.exit( 2 )

  result = production.create( 
                             publish = publishFlag,
                             bkQuery = strippInputBKQuery,
                             groupSize = strippFilesPerJob,
                             bkScript = BKscriptFlag,
                             requestID = currentReqID,
                             reqUsed = 1,
                             transformation = strippTransFlag
                             )
  if not result['OK']:
    gLogger.error( 'Production creation failed with result:\n%s\ntemplate is exiting...' % ( result ) )
    DIRAC.exit( 2 )

  if publishFlag:
    diracProd = DiracProduction()

    strippProdID = result['Value']

    msg = 'Stripping production %s successfully created ' % ( strippProdID )

    if testFlag:
      diracProd.production( strippProdID, 'manual', printOutput = True )
      msg = msg + 'and started in manual mode.'
    else:
      diracProd.production( strippProdID, 'automatic', printOutput = True )
      msg = msg + 'and started in automatic mode.'
    gLogger.info( msg )

  else:
    strippProdID = 1
    gLogger.info( 'Stripping production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, strippProdID ) )


#################################################################################
# Merging
#################################################################################

if mergingEnabled:

  if not strippEnabled:
    strippProdID = 0#'put something here'

  mergeInput = BKClient.getStepInputFiles( mergeStep )
  if not mergeInput:
    gLogger.error( 'Error getting res from BKK: %s', mergeInput['Message'] )
    DIRAC.exit( 2 )

  mergeInputList = [x[0].lower() for x in mergeInput['Value']['Records']]

  mergeOutput = BKClient.getStepOutputFiles( mergeStep )
  if not mergeOutput:
    gLogger.error( 'Error getting res from BKK: %s', mergeOutput['Message'] )
    DIRAC.exit( 2 )

  mergeOutputList = [x[0].lower() for x in mergeOutput['Value']['Records']]

  if mergeInputList != mergeOutputList:
    gLogger.error( 'MergeInput %s != mergeOutput %s' % ( mergeInputList, mergeOutputList ) )
    DIRAC.exit( 2 )

  mergeProductionList = []

  for mergeStream in mergeOutputList:
#    if mergeStream.lower() in onlyCERN:
#      mergeSE = 'CERN_M-DST'
    mergeStream = mergeStream.upper()

    if not mergeOF:
      if mergeApp.lower() == 'davinci':
        mergeOF = 'merge'

    #################################################################################
    # Merging BK Query
    #################################################################################

    mergeDQFlag = mergeDQFlag.replace( ',', ';;;' ).replace( ' ', '' )

    mergeBKQuery = { 'ProductionID'             : strippProdID,
                     'DataQualityFlag'          : mergeDQFlag,
                     'FileType'                 : mergeStream
                    }
#    if mergeApp.lower() == 'davinci':
#      dvExtraOptions = "from Configurables import RecordStream;"
#      dvExtraOptions += "FileRecords = RecordStream(\"FileRecords\");"
#      dvExtraOptions += "FileRecords.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'REC\'\""

    mergeProd = Production( BKKClientIn = BKClient )
    mergeProd.setJobParameters( { 'CPUTime': mergeCPU } )
    mergeProd.setProdType( 'Merge' )
    wkfName = 'Merging_Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID )
    mergeProd.setWorkflowName( '%s_%s_%s' % ( mergeStream, wkfName, appendName ) )

    if sysConfig:
      mergeProd.setJobParameters( { 'SystemConfig': sysConfig } )

    mergeProd.setWorkflowDescription( 'Stream merging workflow for %s files from input production %s' % ( mergeStream, strippProdID ) )
    mergeProd.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
    mergeProd.setDBTags( mergeCDb, mergeDDDb )

    if mergeApp.lower() == 'davinci':
      mergeProd.addDaVinciStep( mergeVersion, 'merge', mergeOptions, extraPackages = mergeEP, eventType = eventType,
                                inputDataType = mergeStream,
                                extraOpts = mergeEOpts, inputData = mergeInputDataList, outputSE = mergedStreamSE,
                                stepID = mergeStep, stepName = mergeName, stepVisible = mergeVisibility, stepPass = mergePass,
                                optionsFormat = mergeOF )
    elif mergeApp.lower() == 'lhcb':
      mergeProd.addMergeStep( mergeVersion, mergeOptions, eventType, mergeEP, inputData = mergeInputDataList,
                              outputSE = mergedStreamSE, inputDataType = mergeStream, extraOpts = mergeEOpts,
                              condDBTag = mergeCDb, ddDBTag = mergeDDDb, dataType = 'Data',
                              stepID = mergeStep, stepName = mergeName, stepVisible = mergeVisibility, stepPass = mergePass,
                              optionsFormat = mergeOF )
    else:
      gLogger.error( 'Merging is not DaVinci nor LHCb and is %s' % mergeApp )
      DIRAC.exit( 2 )

    mergeProd.setJobParameters( { 'InputDataPolicy': mergeIDPolicy } )
    if mergeRemoveInputsFlag:
      mergeProd.addFinalizationStep( ['UploadOutputData',
                                      'FailoverRequest',
                                      'RemoveInputData',
                                      'UploadLogFile'] )
    else:
      mergeProd.addFinalizationStep( ['UploadOutputData',
                                      'FailoverRequest',
                                      'UploadLogFile'] )

    mergeProd.setInputBKSelection( mergeBKQuery )
    mergeProd.setProdGroup( prodGroup )
    mergeProd.setProdPriority( mergePriority )
    mergeProd.setJobFileGroupSize( mergeFileSize )
  #  mergeProd.setFileMask( mergeStream.lower() )
    mergeProd.setProdPlugin( mergePlugin )

    if ( not publishFlag ) and ( testFlag ):

      gLogger.info( 'Production test will be launched with number of events set to %s.' % ( evtsPerJob ) )
      try:
        result = mergeProd.runLocal()
        gLogger.info( 'Template finished successfully' )
        if result['OK']:
          DIRAC.exit( 0 )
        else:
          gLogger.error( 'Something wrong with execution!' )
          DIRAC.exit( 2 )
      except Exception, x:
        gLogger.error( 'Production test failed with exception:\n%s' % ( x ) )
        DIRAC.exit( 2 )

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


#################################################################################
# Create the transformations explicitly since we need to propagate the types
#################################################################################

#if not replicationFlag:
#  gLogger.info( 'Transformation flag is False, exiting prior to creating transformations.' )
#  gLogger.info( 'Template finished successfully.' )
#
#else:
#  transDict = {'DST':replicateList} # We used to also distribute the SETC files as well
#
#  for streamType, streamList in transDict.items():
#    transBKQuery = {  'ProductionID'           : mergeProductionList,
#                      'FileType'               : streamList}
#
#    transformation = Transformation()
#    transName = 'STREAM_Replication_%s_Request%s_{{pDsc}}_{{eventType}}_%s' % ( streamType, currentReqID, appendName )
#    transformation.setTransformationName( transName )
#    transformation.setTransformationGroup( prodGroup )
#    transformation.setDescription( 'Replication of streamed %s from {{pDsc}}' % ( streamType ) )
#    transformation.setLongDescription( 'Replication of streamed %s from {{pDsc}} to all Tier1s' % ( streamType ) )
#    transformation.setType( 'Replication' )
#    transformation.setPlugin( transformationPlugin )
#    transformation.setBkQuery( transBKQuery )
#    transformation.addTransformation()
#    transformation.setStatus( 'Active' )
#    transformation.setAgentType( 'Automatic' )
#    transformation.setTransformationFamily( currentReqID )
#    result = transformation.getTransformationID()
#    if not result['OK']:
#      gLogger.error( 'Problem during transformation creation with result:\n%s\nExiting...' % ( result ) )
#      DIRAC.exit( 2 )
#
#    gLogger.info( 'Transformation creation result: %s' % ( result ) )

gLogger.info( 'Template finished successfully.' )
DIRAC.exit( 0 )

#################################################################################
# End of the template.
#################################################################################
