########################################################################
########################################################################

""" WG productions: 2 3 or 4 steps:
        selection + mergeDST + nTupleCreation (opt) + nTupleMerging (opt)
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

gLogger = gLogger.getSubLogger( 'WG_productions_run.py' )
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

simulationFlag = '{{simulationFlag#GENERAL: Input Data is from simulation#False}}'

# workflow params for all productions
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc43-opt#ANY}}'
useOracle = '{{useOracle#GENERAL: Use Oracle#False}}'

# workflow params for all productions
destination = '{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch#ALL}}'
startRun = '{{wgRunstart#GENERAL: run start, to set the start run#0}}'
endRun = '{{RecoRunEnd#GENERAL: run end, to set the end of the range#0}}'
wgRuns = '{{wgRuns#GENERAL: dicrete list of run numbers (do not mix with start/endrun)#}}'

#stripp params
stripp_priority = '{{priority#PROD-Selection: priority#5}}'
strippCPU = '{{StrippMaxCPUTime#PROD-Selection: Max CPU time in secs#1000000}}'
strippPlugin = '{{StrippPluginType#PROD-Selection: plugin name#LHCbStandard}}'
strippFilesPerJob = '{{StrippFilesPerJob#PROD-Selection: Group size or number of files per job#10}}'
strippTransFlag = '{{StrippTransformation#PROD-Selection: distribute output data True/False (False if merging)#False}}'
unmergedStreamSE = '{{StrippStreamSE#PROD-Selection: output data SE (un-merged streams)#Tier1-DST}}'
strippAncestorProd = '{{StrippAncestorProd#PROD-Selection: ancestor production if any#0}}'
strippIDPolicy = '{{strippIDPolicy#PROD-Selection: policy for input data access (download or protocol)#protocol}}'
extraOptions = '{{extraOptions#PROD-Selection: extra options#}}'

#merging params
mergeDQFlag = '{{MergeDQFlag#PROD-Merging: DQ Flag e.g. OK#OK}}'
mergePriority = '{{MergePriority#PROD-Merging: priority#9}}'
mergePlugin = '{{MergePlugin#PROD-Merging: plugin#BySize}}'
mergeRemoveInputsFlag = '{{MergeRemoveFlag#PROD-Merging: remove input data flag True/False#True}}'
mergeCPU = '{{MergeMaxCPUTime#PROD-Merging: Max CPU time in secs#300000}}'
mergeFileSize = '{{MergeFileSize#PROD-Merging: Size (in GB) of the merged files#5}}'
mergeIDPolicy = '{{MergeIDPolicy#PROD-Merging: policy for input data access (download or protocol)#download}}'
mergedStreamSE = '{{MergeStreamSE#PROD-Merging: output data SE (merged streams)#Tier1_M-DST}}'

step3_ExtraOpts = '{{step3_ExtraOpts#STEP-3: extra options, if needed#}}'

step4_ExtraOpts = '{{step4_ExtraOpts#STEP-4: extra options, if needed#}}'
step4_StreamSE = '{{step4_StreamSE#STEP-4: output data SE (fourth step)#CERN-HIST}}'
step4_IDPolicy = '{{step4_IDPolicy#STEP-4: policy for input data access (download or protocol)#download}}'
step4_CPU = '{{step4_MaxCPUTime#STEP-$: Max CPU time in secs#100000}}'


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

simulation = eval( simulationFlag )
mergeRemoveInputsFlag = eval( mergeRemoveInputsFlag )
certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
validationFlag = eval( validationFlag )
useOracle = eval( useOracle )
strippTransFlag = eval( strippTransFlag )

strippEnabled = False
mergingEnabled = False
step3Enabled = False
step4Enabled = False

oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'
threeSteps = '{{p3App}}'
fourSteps = '{{p4App}}'
fiveSteps = '{{p5App}}'

evtsPerJob = '-1'

#####################################################
# Guessing what the request is for:
#####################################################

error = False

if fiveSteps:
  gLogger.error( 'Five steps specified, not sure what to do! Exiting...' )
  error = True

if fourSteps:
  if oneStep.lower() == 'davinci' and twoSteps.lower() in ( 'lhcb', 'davinci' ) and threeSteps.lower() == 'davinci' and fourSteps.lower() == 'davinci':
    gLogger.info( "Stripping/streaming production + merging + 3rd and 4th step is requested..." )

    strippEnabled = True
    strippDQFlag = inDQFlag
    strippStep = int( '{{p1Step}}' )
    strippName = '{{p1Name}}'
    strippVisibility = '{{p1Vis}}'
    strippCDb = '{{p1CDb}}'
    strippDDDb = '{{p1DDDb}}'
    strippOptions = '{{p1Opt}}'
    strippPass = '{{p1Pass}}'
    if useOracle:
      if not 'useoracle.py' in strippOptions.lower():
        strippOptions = strippOptions + ';$APPCONFIGOPTS/UseOracle.py'

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
    if mergeApp.lower() == 'davinci':
      if useOracle:
        if not 'useoracle.py' in mergeOptions.lower():
          mergeOptions = mergeOptions + ';$APPCONFIGOPTS/UseOracle.py'
    mergeVersion = '{{p2Ver}}'
    mergeEP = '{{p2EP}}'

    step3Enabled = True
    step3_App = '{{p3App}}'
    step3_Step = int( '{{p3Step}}' )
    step3_Name = '{{p3Name}}'
    step3_Visibility = '{{p3Vis}}'
    step3_CDb = '{{p3CDb}}'
    step3_DDDb = '{{p3DDDb}}'
    step3_Options = '{{p3Opt}}'
    step3_Pass = '{{p3Pass}}'
    if step3_App.lower() == 'davinci':
      if useOracle:
        if not 'useoracle.py' in step3_Options.lower():
          step3_Options = step3_Options + ';$APPCONFIGOPTS/UseOracle.py'
    step3_Version = '{{p3Ver}}'
    step3_EP = '{{p3EP}}'

    step3_Input = BKClient.getStepInputFiles( step3_Step )
    if not step3_Input:
      gLogger.error( 'Error getting res from BKK: %s', step3_Input['Message'] )
      DIRAC.exit( 2 )

    step3_InputList = [x[0].lower() for x in step3_Input['Value']['Records']]
    if len( step3_InputList ) == 1:
      step3_Input = step3_InputList[0]
    else:
      gLogger.error( 'Multiple inputs to step3_ step...?', step3_Input['Message'] )
      DIRAC.exit( 2 )

    step3_Output = BKClient.getStepOutputFiles( step3_Step )
    if not step3_Output:
      gLogger.error( 'Error getting res from BKK: %s', step3_Output['Message'] )
      DIRAC.exit( 2 )

    step3_OutputList = [x[0].lower() for x in step3_Output['Value']['Records']]
    if len( step3_OutputList ) > 1:
      step3_Type = 'stripping'
      step3_EO = step3_OutputList
    else:
      step3_Type = step3_OutputList[0]
      step3_EO = []


    step4Enabled = True
    step4_App = '{{p3App}}'
    step4_Step = int( '{{p3Step}}' )
    step4_Name = '{{p3Name}}'
    step4_Visibility = '{{p3Vis}}'
    step4_CDb = '{{p3CDb}}'
    step4_DDDb = '{{p3DDDb}}'
    step4_Options = '{{p3Opt}}'
    step4_Pass = '{{p4Pass}}'
    if step4_App.lower() == 'davinci':
      if useOracle:
        if not 'useoracle.py' in step4_Options.lower():
          step4_Options = step4_Options + ';$APPCONFIGOPTS/UseOracle.py'
    step4_Version = '{{p3Ver}}'
    step4_EP = '{{p3EP}}'

    step4_Input = BKClient.getStepInputFiles( step4_Step )
    if not step4_Input:
      gLogger.error( 'Error getting res from BKK: %s', step4_Input['Message'] )
      DIRAC.exit( 2 )

    step4_InputList = [x[0].lower() for x in step4_Input['Value']['Records']]
    if len( step4_InputList ) == 1:
      step4_Input = step4_InputList[0]
    else:
      gLogger.error( 'Multiple inputs to step4_ step...?', step4_Input['Message'] )
      DIRAC.exit( 2 )

    step4_Output = BKClient.getStepOutputFiles( step4_Step )
    if not step4_Output:
      gLogger.error( 'Error getting res from BKK: %s', step4_Output['Message'] )
      DIRAC.exit( 2 )

    step4_OutputList = [x[0].lower() for x in step4_Output['Value']['Records']]
    if len( step4_OutputList ) > 1:
      step4_Type = 'stripping'
      step4_EO = step4_OutputList
    else:
      step4_Type = step4_OutputList[0]
      step4_EO = []

  else:
    gLogger.error( 'First step is %s, second is %s, third is %s, fourth is %s' % ( oneStep, twoSteps, threeSteps, fourSteps ) )
    error = True



elif threeSteps:
  if oneStep.lower() == 'davinci' and twoSteps.lower() in ( 'lhcb', 'davinci' ) and threeSteps.lower() == 'davinci':
    gLogger.info( "Stripping/streaming production + merging + 3rd step is requested..." )

    strippEnabled = True
    strippDQFlag = inDQFlag
    strippStep = int( '{{p1Step}}' )
    strippName = '{{p1Name}}'
    strippVisibility = '{{p1Vis}}'
    strippCDb = '{{p1CDb}}'
    strippDDDb = '{{p1DDDb}}'
    strippOptions = '{{p1Opt}}'
    strippPass = '{{p1Pass}}'
    if useOracle:
      if not 'useoracle.py' in strippOptions.lower():
        strippOptions = strippOptions + ';$APPCONFIGOPTS/UseOracle.py'

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
    if mergeApp.lower() == 'davinci':
      if useOracle:
        if not 'useoracle.py' in mergeOptions.lower():
          mergeOptions = mergeOptions + ';$APPCONFIGOPTS/UseOracle.py'
    mergeVersion = '{{p2Ver}}'
    mergeEP = '{{p2EP}}'

    step3Enabled = True
    step3_App = '{{p3App}}'
    step3_Step = int( '{{p3Step}}' )
    step3_Name = '{{p3Name}}'
    step3_Visibility = '{{p3Vis}}'
    step3_CDb = '{{p3CDb}}'
    step3_DDDb = '{{p3DDDb}}'
    step3_Opts = '{{p3Opt}}'
    step3_Pass = '{{p3Pass}}'
    if step3_App.lower() == 'davinci':
      if useOracle:
        if not 'useoracle.py' in step3_Options.lower():
          step3_Options = step3_Options + ';$APPCONFIGOPTS/UseOracle.py'
    step3_Version = '{{p3Ver}}'
    step3_EP = '{{p3EP}}'

    step3_Input = BKClient.getStepInputFiles( step3_Step )
    if not step3_Input:
      gLogger.error( 'Error getting res from BKK: %s', step3_Input['Message'] )
      DIRAC.exit( 2 )

    step3_InputList = [x[0].lower() for x in step3_Input['Value']['Records']]
    if len( step3_InputList ) == 1:
      step3_Input = step3_InputList[0]
    else:
      gLogger.error( 'Multiple inputs to step3_ step...?', step3_Input['Message'] )
      DIRAC.exit( 2 )

    step3_Output = BKClient.getStepOutputFiles( step3_Step )
    if not step3_Output:
      gLogger.error( 'Error getting res from BKK: %s', step3_Output['Message'] )
      DIRAC.exit( 2 )

    step3_OutputList = [x[0].lower() for x in step3_Output['Value']['Records']]
    if len( step3_OutputList ) > 1:
      step3_Type = 'stripping'
      step3_EO = step3_OutputList
    else:
      step3_Type = step3_OutputList[0]
      step3_EO = []

  else:
    gLogger.error( 'First step is %s, second is %s, third is %s' % ( oneStep, twoSteps, threeSteps ) )
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
    strippPass = '{{p1Pass}}'
    if useOracle:
      if not 'useoracle.py' in strippOptions.lower():
        strippOptions = strippOptions + ';$APPCONFIGOPTS/UseOracle.py'

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
    if mergeApp.lower() == 'davinci':
      if useOracle:
        if not 'useoracle.py' in mergeOptions.lower():
          mergeOptions = mergeOptions + ';$APPCONFIGOPTS/UseOracle.py'
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
    strippPass = '{{p1Pass}}'
    if useOracle:
      if not 'useoracle.py' in strippOptions.lower():
        strippOptions = strippOptions + ';$APPCONFIGOPTS/UseOracle.py'
    strippVersion = '{{p1Ver}}'
    strippEP = '{{p1EP}}'
    strippFileType = '{{inFileType}}'

  else:
    gLogger.error( 'Stripping/streaming step is NOT daVinci and is %s' % oneStep )
    error = True

if error:
  DIRAC.exit( 2 )





if certificationFlag or localTestFlag:
  testFlag = True
  if certificationFlag:
    publishFlag = True
    mergingEnabled = True
  if localTestFlag:
    publishFlag = False
    mergingEnabled = False
else:
  publishFlag = True
  testFlag = False

strippInputDataList = []
mergeInputDataList = []
step4_InputDataList = []

if not publishFlag:
  strippTestData = 'LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00012707/0000/00012707_00000069_1.dimuon.dst'
  strippInputDataList.append( strippTestData )
  strippIDPolicy = 'protocol'
  evtsPerJob = '2000'
  BKscriptFlag = True
  mergeTestInputData1 = '/lhcb/validation/Collision11/BUBDBSSELECTION.DST/00012985/0000/00012985_00000036_1.BuBdBsSelection.dst'
  mergeTestInputData2 = '/lhcb/validation/Collision11/BUBDBSSELECTION.DST/00012985/0000/00012985_00000041_1.BuBdBsSelection.dst'
  mergeInputDataList.append( mergeTestInputData1 )
  mergeInputDataList.append( mergeTestInputData2 )
  mergeIDPolicy = 'protocol'
else:
  BKscriptFlag = False

if testFlag:
  outBkConfigName = 'certification'
  outBkConfigVersion = 'test'
#  startRun = '87962'
#  endRun = '87977'
  recoCPU = '100000'
  dataTakingCond = 'Beam3500GeV-VeloClosed-MagDown'
  processingPass = 'Real Data/Reco09'
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

  strippInputBKQuery = {
                        'ProcessingPass'           : processingPass,
                        'FileType'                 : strippFileType,
                        'EventType'                : eventType,
                        'ConfigName'               : bkConfigName,
                        'ConfigVersion'            : bkConfigVersion,
                        'ProductionID'             : 0,
                        'DataQualityFlag'          : strippDQFlag,
                        'Visible'                  : 'Yes'
                        }

  if simulation:
    strippInputBKQuery['SimulationConditions'] = dataTakingCond
  else:
    strippInputBKQuery['DataTakingConditions'] = dataTakingCond


  if int( endRun ) and int( startRun ):
    if int( endRun ) < int( startRun ):
      gLogger.error( 'Your end run "%s" should be less than your start run "%s"!' % ( endRun, startRun ) )
      DIRAC.exit( 2 )

  if int( startRun ):
    strippInputBKQuery['StartRun'] = int( startRun )
  if int( endRun ):
    strippInputBKQuery['EndRun'] = int( endRun )

  if wgRuns:
    strippInputBKQuery['RunNumbers'] = wgRuns.replace( ',', ';;;' ).replace( ' ', '' )

  #################################################################################
  # Create the stripping production
  #################################################################################

  production = Production()

  if not destination.lower() in ( 'all', 'any' ):
    gLogger.info( 'Forcing destination site %s for production' % ( destination ) )
    production.setTargetSite( destination )

  if sysConfig:
    production.setSystemConfig( sysConfig )

  production.setCPUTime( strippCPU )
  production.setProdType( 'DataStripping' )
  wkfName = 'Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID ) #Rest can be taken from the details in the monitoring
  production.setWorkflowName( 'STRIPPING_%s_%s' % ( wkfName, appendName ) )
  production.setWorkflowDescription( "%s real data stripping production." % ( prodGroup ) )
  production.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
  production.setInputBKSelection( strippInputBKQuery )
  production.setDBTags( strippCDb, strippDDDb )
  production.setInputDataPolicy( strippIDPolicy )
  production.setProdPlugin( strippPlugin )

  if strippInput.lower() == 'sdst':
    try:
      production.setAncestorDepth( 2 )
    except:
      production.LHCbJob.setAncestorDepth( 2 )

  production.addDaVinciStep( strippVersion, strippType, strippOptions, eventType = eventType, extraPackages = strippEP,
                             inputDataType = strippInput.lower(), inputData = strippInputDataList, numberOfEvents = evtsPerJob,
                             dataType = 'Data',
                             outputSE = unmergedStreamSE,
                             extraOpts = extraOptions,
                             histograms = histFlag, extraOutput = strippEO,
                             stepID = strippStep, stepName = strippName, stepVisible = strippVisibility, stepPass = strippPass )

  production.addFinalizationStep()
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

#  if not strippEnabled:
#    strippProdID = #put something here

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

  if strippEnabled:
    if mergeInputList != strippOutputList:
      gLogger.error( 'MergeInput %s != strippOutput %s' % ( mergeInputList, strippOutputList ) )
      DIRAC.exit( 2 )

  if mergeInputList != mergeOutputList:
    gLogger.error( 'MergeInput %s != mergeOutput %s' % ( mergeInputList, mergeOutputList ) )
    DIRAC.exit( 2 )

  mergeProductionList = []

  for mergeStream in mergeOutputList:
#    if mergeStream.lower() in onlyCERN:
#      mergeSE = 'CERN_M-DST'

    mergeStream = mergeStream.upper()

    #################################################################################
    # Merging BK Query
    #################################################################################

    mergeBKQuery = { 'ProductionID'             : strippProdID,
                     'DataQualityFlag'          : mergeDQFlag,
                     'FileType'                 : mergeStream}
      #below should be integrated in the ProductionOptions utility
    if mergeApp.lower() == 'davinci':
      dvExtraOptions = "from Configurables import RecordStream;"
      dvExtraOptions += "FileRecords = RecordStream(\"FileRecords\");"
      dvExtraOptions += "FileRecords.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'REC\'\""

    ###########################################
    # Create the merging production
    ###########################################


    mergeProd = Production()
    mergeProd.setCPUTime( mergeCPU )
    mergeProd.setProdType( 'Merge' )
    wkfName = 'Merging_Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID )
    mergeProd.setWorkflowName( '%s_%s_%s' % ( mergeStream.split( '.' )[0], wkfName, appendName ) )

    if sysConfig:
      mergeProd.setSystemConfig( sysConfig )

    mergeProd.setWorkflowDescription( 'Stream merging workflow for %s files from input production %s' % ( mergeStream, strippProdID ) )
    mergeProd.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
    mergeProd.setDBTags( mergeCDb, mergeDDDb )

    if mergeApp.lower() == 'davinci':
      mergeProd.addDaVinciStep( mergeVersion, 'merge', mergeOptions, extraPackages = mergeEP, eventType = eventType,
                                inputDataType = mergeStream.lower(), extraOpts = dvExtraOptions, numberOfEvents = evtsPerJob,
                                inputProduction = strippProdID, inputData = mergeInputDataList, outputSE = mergedStreamSE,
                                stepID = mergeStep, stepName = mergeName, stepVisible = mergeVisibility, stepPass = mergePass )
    elif mergeApp.lower() == 'lhcb':
      mergeProd.addMergeStep( mergeVersion, mergeOptions, strippProdID, eventType, mergeEP, inputData = mergeInputDataList,
                              inputDataType = mergeStream.lower(), outputSE = mergedStreamSE, numberOfEvents = evtsPerJob,
                              condDBTag = mergeCDb, ddDBTag = mergeDDDb, dataType = 'Data',
                              stepID = mergeStep, stepName = mergeName, stepVisible = mergeVisibility, stepPass = mergePass )
    else:
      gLogger.error( 'Merging is not DaVinci nor LHCb and is %s' % mergeApp )
      DIRAC.exit( 2 )

    if step3Enabled:
      mergeProd.addDaVinciStep( step3_Version, step3_Type, step3_Opts, eventType = eventType, extraPackages = step3_EP,
                                inputDataType = step3_Input.lower(), numberOfEvents = evtsPerJob,
                                dataType = 'Data', extraOpts = step3_ExtraOpts,
                                stepID = step3_Step, stepName = step3_Name, stepVisible = step3_Visibility,
                                stepPass = step3_Pass )

    mergeProd.addFinalizationStep( removeInputData = mergeRemoveInputsFlag )
    mergeProd.setInputBKSelection( mergeBKQuery )
    mergeProd.setInputDataPolicy( mergeIDPolicy )
    mergeProd.setProdGroup( prodGroup )
    mergeProd.setProdPriority( mergePriority )
    mergeProd.setJobFileGroupSize( mergeFileSize )
    mergeProd.setProdPlugin( mergePlugin )

    if ( not publishFlag ) and ( testFlag ):

      gLogger.info( 'Production test will be launched with number of events set to %s.' % ( evtsPerJob ) )
      try:
        result = mergeProd.runLocal()
        if result['OK']:
          gLogger.info( 'Template finished successfully' )
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
# step 4
#################################################################################

if step4Enabled:

  step4_Input = BKClient.getStepInputFiles( step4_Step )
  if not step4_Input:
    gLogger.error( 'Error getting res from BKK: %s', step4_Input['Message'] )
    DIRAC.exit( 2 )

  step4_InputList = [x[0].lower() for x in step4_Input['Value']['Records']]

  step4_Output = BKClient.getStepOutputFiles( step4_Step )
  if not step4_Output:
    gLogger.error( 'Error getting res from BKK: %s', step4_Output['Message'] )
    DIRAC.exit( 2 )

  step4_OutputList = [x[0].lower() for x in step4_Output['Value']['Records']]

  if strippEnabled:
    if step4_InputList != strippOutputList:
      gLogger.error( 'MergeInput %s != strippOutput %s' % ( step4_InputList, strippOutputList ) )
      DIRAC.exit( 2 )

  if step4_InputList != step4_OutputList:
    gLogger.error( 'MergeInput %s != step4_Output %s' % ( step4_InputList, step4_OutputList ) )
    DIRAC.exit( 2 )

  step4_ProductionList = []

  for step4_Stream in step4_OutputList:
#    if step4_Stream.lower() in onlyCERN:
#      step4_SE = 'CERN_M-DST'

    step4_Stream = step4_Stream.upper()

    #################################################################################
    # Merging BK Query
    #################################################################################

    step4_BKQuery = {
                     'ProductionID'             : strippProdID,
                     'DataQualityFlag'          : 'OK',
                     'FileType'                 : step4_Stream
                     }
      #below should be integrated in the ProductionOptions utility
    if step4_App.lower() == 'davinci':
      dvExtraOptions = "from Configurables import RecordStream;"
      dvExtraOptions += "FileRecords = RecordStream(\"FileRecords\");"
      dvExtraOptions += "FileRecords.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'REC\'\""

    ###########################################
    # Create the merging production
    ###########################################


    step4_Prod = Production()
    step4_Prod.setCPUTime( step4_CPU )
    step4_Prod.setProdType( 'Merge' )
    wkfName = 'Merging_Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID )
    step4_Prod.setWorkflowName( '%s_%s_%s' % ( step4_Stream.split( '.' )[0], wkfName, appendName ) )

    if sysConfig:
      step4_Prod.setSystemConfig( sysConfig )

    step4_Prod.setWorkflowDescription( 'Stream merging workflow for %s files from input production %s' % ( step4_Stream, strippProdID ) )
    step4_Prod.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
    step4_Prod.setDBTags( step4_CDb, step4_DDDb )

    if step4_App.lower() == 'davinci':
      step4_Prod.addDaVinciStep( step4_Version, 'step4_', step4_Options, extraPackages = step4_EP, eventType = eventType,
                                inputDataType = step4_Stream.lower(), extraOpts = dvExtraOptions, numberOfEvents = evtsPerJob,
                                inputProduction = strippProdID, inputData = step4_InputDataList, outputSE = step4_StreamSE,
                                stepID = step4_Step, stepName = step4_Name, stepVisible = step4_Visibility, stepPass = step4_Pass )
    elif step4_App.lower() == 'lhcb':
      step4_Prod.addMergeStep( step4_Version, step4_Options, strippProdID, eventType, step4_EP, inputData = step4_InputDataList,
                              inputDataType = step4_Stream.lower(), outputSE = step4_StreamSE, numberOfEvents = evtsPerJob,
                              condDBTag = step4_CDb, ddDBTag = step4_DDDb, dataType = 'Data',
                              stepID = step4_Step, stepName = step4_Name, stepVisible = step4_Visibility, stepPass = step4_Pass )
    else:
      gLogger.error( 'Merging is not DaVinci nor LHCb and is %s' % step4_App )
      DIRAC.exit( 2 )

    step4_Prod.addFinalizationStep( removeInputData = mergeRemoveInputsFlag )
    step4_Prod.setInputBKSelection( step4_BKQuery )
    step4_Prod.setInputDataPolicy( step4_IDPolicy )
    step4_Prod.setProdGroup( prodGroup )
    step4_Prod.setProdPriority( mergePriority )
    step4_Prod.setJobFileGroupSize( mergeFileSize )
    step4_Prod.setProdPlugin( mergePlugin )

    if ( not publishFlag ) and ( testFlag ):

      gLogger.info( 'Production test will be launched with number of events set to %s.' % ( evtsPerJob ) )
      try:
        result = step4_Prod.runLocal()
        if result['OK']:
          gLogger.info( 'Template finished successfully' )
          DIRAC.exit( 0 )
        else:
          gLogger.error( 'Something wrong with execution!' )
          DIRAC.exit( 2 )
      except Exception, x:
        gLogger.error( 'Production test failed with exception:\n%s' % ( x ) )
        DIRAC.exit( 2 )

    result = step4_Prod.create( 
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
      msg = 'Merging production %s for %s successfully created ' % ( prodID, step4_Stream )

      if testFlag:
        diracProd.production( prodID, 'manual', printOutput = True )
        msg = msg + 'and started in manual mode.'
      else:
        diracProd.production( prodID, 'automatic', printOutput = True )
        msg = msg + 'and started in automatic mode.'
      gLogger.info( msg )

      step4_ProductionList.append( int( prodID ) )

    else:
      prodID = 1
      gLogger.info( 'Merging production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, prodID ) )



gLogger.info( 'Template finished successfully.' )
DIRAC.exit( 0 )

