########################################################################
# $HeadURL$
########################################################################

"""  The General.py Template will try to create a general template, 
     to be used for all kind of requests

"""

__RCSID__ = "$Id$"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################

import re
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gConfig, gLogger
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

gLogger = gLogger.getSubLogger( 'FULL_RealData_Merging_run.py' )
BKClient = BookkeepingClient()

#################################################################################
# Below here is the production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation

###########################################
# Configurable parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'

# workflow params for all productions
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. slc4_ia32_gcc34#x86_64-slc5-gcc43-opt}}'
destination = '{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch#ALL}}'
startRun = '{{RecoRunStart#GENERAL: run start, to set the start run#0}}'
endRun = '{{RecoRunEnd#GENERAL: run end, to set the end of the range#0}}'
evtsPerJob = '{{NumberEvents#GENERAL: number of events per job (set to something small for a test)#-1}}'

#reco params
#recoEnabled = '{{recoEnabled#PROD-RECO: enabled#True}}'
recoPriority = '{{RecoPriority#PROD-RECO: priority#7}}'
recoCPU = '{{RecoMaxCPUTime#PROD-RECO: Max CPU time in secs#1000000}}'
recoPlugin = '{{RecoPluginType#PROD-RECO: production plugin name#AtomicRun}}'
recoDataSE = '{{RecoDataSE#PROD-RECO: Output Data Storage Element (for the sdst)#Tier1-RDST}}'
recoFilesPerJob = '{{RecoFilesPerJob#PROD-RECO: Group size or number of files per job#1}}'
recoTransFlag = '{{RecoTransformation#PROD-RECO: distribute output data True/False (False if merging)#False}}'
recoType = '{{RecoType#PROD-RECO: reconstructionType (e.g. FULL, EXPRESS...)#FULL}}'
#stripp params
#strippEnabled = '{{strippEnabled#PROD-stripping: enabled#True}}'
stripp_priority = '{{priority#PROD-Stripping: priority#7}}'
strippCPU = '{{StrippMaxCPUTime#PROD-Stripping: Max CPU time in secs#1000000}}'
strippPlugin = '{{StrippPluginType#PROD-Stripping: plugin name#ByRun}}'
strippFilesPerJob = '{{StrippFilesPerJob#PROD-Stripping: Group size or number of files per job#10}}'
strippTransFlag = '{{StrippTransformation#PROD-Stripping: distribute output data True/False (False if merging)#False}}'
unmergedStreamSE = '{{StrippStreamSE#PROD-Stripping: output data SE (un-merged streams)#Tier1-DST}}'
strippDQFlag = '{{strippDQFlag#PROD-Stripping: DQ flag for stripping only when also Brunel is requested#OK}}'
#merging params
#mergingEnabled = '{{mergingEnabled#PROD-Merging: enabled#True}}'
mergeDQFlag = '{{MergeDQFlag#PROD-Merging: DQ Flag e.g. OK#OK}}'
mergePriority = '{{MergePriority#PROD-Merging: priority#8}}'
mergePlugin = '{{MergePlugin#PROD-Merging: plugin#MergeByRun}}'
mergeRemoveInputsFlag = '{{MergeRemoveFlag#PROD-Merging: remove input data flag True/False#False}}'
mergeCPU = '{{MergeMaxCPUTime#PROD-Merging: Max CPU time in secs#300000}}'

#transformation params
replicationFlag = '{{TransformationEnable#Replication: flag to enable True/False#True}}'
transformationPlugin = '{{TransformationPlugin#Replication: plugin name#LHCbDSTBroadcast}}'

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
recoTransFlag = eval( recoTransFlag )
dataTakingCond = '{{simDesc}}'
processingPass = '{{inProPass}}'
eventType = '{{eventType}}'

mergeRemoveInputsFlag = eval( mergeRemoveInputsFlag )
replicationFlag = eval( replicationFlag )
certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )

recoEnabled = False 
strippEnabled = False 
mergingEnabled = False 

oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'
threeSteps = '{{p3App}}'
fourSteps = '{{p4App}}'


#mergingFlag = False

#####################################################
# Guessing what the request is for:
#####################################################

error = False
if fourSteps:
  gLogger.error( 'Four steps specified, not sure what to do! Exiting...' )
  error = True
elif threeSteps:
  if oneStep.lower() == 'brunel' and twoSteps.lower() == 'davinci' and threeSteps.lower() in ( 'davinci', 'lhcb' ):
    gLogger.info( 'Reconstruction production + stripping/streaming + merging is requested...' )

    recoEnabled = True
    recoDQFlag = inDQFlag
    recoStep = int( '{{p1Step}}' )

    strippEnabled = True
    strippDQFlag = 'OK' #NOT THE BEST to have it set like this
    strippStep = int( '{{p2Step}}' )
    
    mergingEnabled = True
    mergeStep = int( '{{p3Step}}' )
    mergeOpts = '{{p3Opt}}'

  else:
    if oneStep.lower() != 'brunel':
      gLogger.error( 'Reconstruction step is NOT brunel and is %s' % oneStep )
    if twoSteps.lower() != 'davinci':
      gLogger.error( 'Stripping/streaming step is NOT daVinci and is %s' % twoSteps )
    if threeSteps.lower() not in ( 'davinci', 'lhcb' ):
      gLogger.error( 'Merging step is NOT lhcb nor davinci and is %s' % threeSteps )
    error = True
    mergingFlag = True
elif twoSteps:
  if oneStep.lower() == 'brunel' and twoSteps.lower() == 'davinci':
    gLogger.info( "Reconstruction production + stripping/streaming without merging is requested..." )

    recoEnabled = True
    recoDQFlag = inDQFlag
    recoStep = int( '{{p1Step}}' )

    strippEnabled = True
    strippDQFlag = 'OK' #NOT THE BEST to have it set like this
    strippStep = int( '{{p2Step}}' )

  elif oneStep.lower() == 'davinci' and twoSteps.lower() in ( 'lhcb', 'davinci' ):
    gLogger.info( "Stripping/streaming production + merging is requested..." )

    strippEnabled = True
    strippDQFlag = inDQFlag
    strippStep = int( '{{p1Step}}' )

    mergingEnabled = True
    mergeStep = int( '{{p2Step}}' )

  else:
    gLogger.error( 'First step is %s, second is %s' % ( oneStep, twoSteps ) )
    error = True
elif oneStep:
  #FIXME
  #all the other single cases?
  if oneStep.lower() == 'davinci':
    gLogger.info( "Stripping/streaming production without merging is requested..." )
    strippEnabled = True
    strippDQFlag = inDQFlag
    strippStep = int( '{{p1Step}}' )

  else:
    gLogger.error( 'Stripping/streaming step is NOT daVinci and is %s' % oneStep )
    error = True

if error:
  DIRAC.exit( 2 )


  


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

recoInputDataList = []
strippInputDataList = []

if not publishFlag:
  recoTestData = 'LFN:/lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81676/081676_0000000510.raw'
  recoInputDataList.append( recoTestData )
  recoIDPolicy = 'protocol'
  strippTestData = 'LFN:/lhcb/data/2010/SDST/00008178/0000/00008178_00009199_1.sdst'
  strippInputDataList.append( strippTestData )
  BKscriptFlag = True
else:
  recoIDPolicy = 'download'
  BKscriptFlag = False

strippIDPolicy = 'protocol'


if testFlag:
  outBkConfigName = 'certification'
  outBkConfigVersion = 'test'
  evtsPerJob = '5'
  startRun = '75346'
  endRun = '75349'
  recoCPU = '100000'
  dataTakingCond = 'Beam3500GeV-VeloClosed-MagDown'
  processingPass = 'Real Data'
  recofileType = 'RAW'
  strippfileType = 'SDST'
  eventType = '90000000'
else:
  outBkConfigName = bkConfigName
  outBkConfigVersion = bkConfigVersion



#################################################################################
# Reconstruction
#################################################################################

if recoEnabled:

  #################################################################################
  # Reconstruction BK Query
  #################################################################################

  recofileType = BKClient.getStepInputFiles( recoStep )
  if not recofileType:
    gLogger.error( 'Error getting res from BKK: %s', recofileType['Message'] )
    DIRAC.exit( 2 )
  recoOutput = recofileType['Value']['Records']
    
  recoOutput = BKClient.getStepOutputFiles( recoStep )
  if not recoOutput:
    gLogger.error( 'Error getting res from BKK: %s', recoOutput['Message'] )
    DIRAC.exit( 2 )
  recoOutput = recoOutput['Value']['Records']

  recoInputBKQuery = {
                      'DataTakingConditions'     : dataTakingCond,
                      'ProcessingPass'           : processingPass,
                      'FileType'                 : recofileType,
                      'EventType'                : eventType,
                      'ConfigName'               : bkConfigName,
                      'ConfigVersion'            : bkConfigVersion,
                      'ProductionID'             : 0,
                      'DataQualityFlag'          : recoDQFlag
                      }
  
  if int( endRun ) and int( startRun ):
    if int( endRun ) < int( startRun ):
      gLogger.error( 'Your end run "%s" should be less than your start run "%s"!' % ( endRun, startRun ) )
      DIRAC.exit( 2 )
  
  if int( startRun ):
    recoInputBKQuery['StartRun'] = int( startRun )
  if int( endRun ):
    recoInputBKQuery['EndRun'] = int( endRun )
  
  #recoFileMask = 'HIST'
  #
  ##Sort out the reco file mask
  #if recoFileMask:
  #  maskList = [m.lower() for m in recoFileMask.replace( ' ', '' ).split( ',' )]
  #  if not recoOutput.lower() in maskList:
  #    maskList.append( recoOutput.lower() )
  #  recoFileMask = string.join( maskList, ';' )
  
  
  if re.search( ',', recoRunNumbers ) and not int( startRun ) and not int( endRun ):
    gLogger.info( 'Found run numbers to add to BK Query...' )
    runNumbers = [int( i ) for i in recoRunNumbers.replace( ' ', '' ).split( ',' )]
    recoInputBKQuery['RunNumbers'] = runNumbers
  
  #################################################################################
  # Create the reconstruction production
  #################################################################################
  
  production = Production()
  
  if not destination.lower() in ( 'all', 'any' ):
    gLogger.info( 'Forcing destination site %s for production' % ( destination ) )
    production.setDestination( destination )
  
  if sysConfig:
    production.setSystemConfig( sysConfig )
  
  production.setCPUTime( recoCPU )
  production.setProdType( 'DataReconstruction' )
  wkfName = 'Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID ) #Rest can be taken from the details in the monitoring
  production.setWorkflowName( '%s_%s_%s' % ( recoType, wkfName, appendName ) )
  production.setWorkflowDescription( "%s Real data %s reconstruction production." % ( prodGroup, recoType ) )
  production.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
  production.setInputBKSelection( recoInputBKQuery )
  production.setDBTags( '{{p1CDb}}', '{{p1DDDb}}' )
  
  brunelOptions = "{{p1Opt}}"
  production.addBrunelStep( "{{p1Ver}}", recoOutput.lower(), brunelOptions, extraPackages = '{{p1EP}}',
                           eventType = eventType, inputData = recoInputDataList, inputDataType = 'mdf', outputSE = recoDataSE,
                           dataType = 'Data', numberOfEvents = evtsPerJob, histograms = True,
                           stepID = '{{p1Step}}', stepName = '{{p1Name}}', stepVisible = '{{p1Vis}}' )
  
  production.addFinalizationStep()
  production.setProdGroup( prodGroup )
  production.setProdPriority( recoPriority )
  production.setProdPlugin( recoPlugin )
  production.setInputDataPolicy( recoIDPolicy )
  
  #################################################################################
  # Publishing of the reconstruction production
  #################################################################################
  
  if ( not publishFlag ) and ( testFlag ):
  
    gLogger.info( 'Production test will be launched with number of events set to %s.' % ( evtsPerJob ) )
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
                             bkQuery = recoInputBKQuery,
                             groupSize = recoFilesPerJob,
                             bkScript = BKscriptFlag,
                             requestID = currentReqID,
                             reqUsed = 1,
                             transformation = recoTransFlag
                             )
  if not result['OK']:
    gLogger.error( 'Production creation failed with result:\n%s\ntemplate is exiting...' % ( result ) )
    DIRAC.exit( 2 )
  
  if publishFlag:
    diracProd = DiracProduction()
  
    recoProdID = result['Value']
  
    msg = 'Reconstruction production %s successfully created ' % ( recoProdID )
  
    if testFlag:
      diracProd.production( recoProdID, 'manual', printOutput = True )
      msg = msg + 'and started in manual mode.'
    else:
      diracProd.production( recoProdID, 'automatic', printOutput = True )
      msg = msg + 'and started in automatic mode.'
    gLogger.info( msg )
  
  else:
    recoProdID = 1
    gLogger.info( 'Reconstruction production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, recoProdID ) )

#################################################################################
# Stripping
#################################################################################

if strippEnabled:

  #################################################################################
  # Reconstruction BK Query
  #################################################################################

  strippOutput = BKClient.getStepOutputFiles( strippStep )
  if not strippOutput:
    gLogger.error( 'Error getting res from BKK: %s', strippOutput['Message'] )
    DIRAC.exit( 2 )
  strippOutputList = [x[0] for x in strippOutput['Value']['Records']]

  strippInputBKQuery = {
                      'DataTakingConditions'     : dataTakingCond,
                      'ProcessingPass'           : processingPass,
                      'FileType'                 : strippfileType,
                      'EventType'                : eventType,
                      'ConfigName'               : bkConfigName,
                      'ConfigVersion'            : bkConfigVersion,
                      'ProductionID'             : 0,
                      'DataQualityFlag'          : strippDQFlag
                      }
  

  if int( endRun ) and int( startRun ):
    if int( endRun ) < int( startRun ):
      gLogger.error( 'Your end run "%s" should be less than your start run "%s"!' % ( endRun, startRun ) )
      DIRAC.exit( 2 )
  
  if int( startRun ):
    strippInputBKQuery['StartRun'] = int( startRun )
  if int( endRun ):
    strippInputBKQuery['EndRun'] = int( endRun )
  
  #################################################################################
  # Create the stripping production
  #################################################################################
  
  production = Production()
  
  if not destination.lower() in ( 'all', 'any' ):
    gLogger.info( 'Forcing destination site %s for production' % ( destination ) )
    production.setDestination( destination )
  
  if sysConfig:
    production.setSystemConfig( sysConfig )
  
  production.setCPUTime( strippCPU )
  production.setProdType( 'DataStripping' )
  wkfName = 'Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID ) #Rest can be taken from the details in the monitoring
  production.setWorkflowName( 'STRIPPING_%s_%s' % ( wkfName, appendName ) )
  production.setWorkflowDescription( "%s real data %s stripping production." % ( prodGroup, recoType ) )
  production.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
  production.setInputBKSelection( strippInputBKQuery )
  production.setDBTags( '{{p1CDb}}', '{{p1DDDb}}' )
  
  dvOptions = "{{p2Opt}}"
  production.addDaVinciStep( "{{p2Ver}}", "stripping", dvOptions, extraPackages = '{{p2EP}}', inputDataType = recoOutput.lower(),
                            dataType = 'Data', outputSE = unmergedStreamSE, histograms = True,
                            extraOutput = strippOutputList,
                            stepID = '{{p2Step}}', stepName = '{{p2Name}}', stepVisible = '{{p2Vis}}' )

  #################################################################################
  # Publishing of the stripping production
  #################################################################################
  
  if ( not publishFlag ) and ( testFlag ):
  
    gLogger.info( 'Production test will be launched with number of events set to %s.' % ( evtsPerJob ) )
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
  
    recoProdID = result['Value']
  
    msg = 'Stripping production %s successfully created ' % ( recoProdID )
  
    if testFlag:
      diracProd.production( recoProdID, 'manual', printOutput = True )
      msg = msg + 'and started in manual mode.'
    else:
      diracProd.production( recoProdID, 'automatic', printOutput = True )
      msg = msg + 'and started in automatic mode.'
    gLogger.info( msg )
  
  else:
    recoProdID = 1
    gLogger.info( 'Stripping production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, recoProdID ) )



#################################################################################
# Create the merging productions if there are enough workflow steps
#################################################################################

#
###################################################################################
#### TEMPORARY HACK SINCE THERE IS NO REASONABLE WAY TO GET THE LIST OF STREAMS ###
###################################################################################
#
##The below list is not yet defined in the central CS but can be to allow a bit of flexibility
##anything in the above list will trigger a merging production.
#streamsListDefault = ['SEMILEPTONIC.DST', 'RADIATIVE.DST', 'MINIBIAS.DST', 'LEPTONIC.MDST', 'EW.DST',
#                      'DIMUON.DST', 'DIELECTRON.DST', 'CHARM.MDST', 'CHARMCONTROL.DST', 'BHADRON.DST',
#                      'LEPTONICFULL.DST', 'CHARMFULL.DST', 'CALIBRATION.DST']
#
#streamsList = gConfig.getValue( '/Operations/Reconstruction/MergingStreams', streamsListDefault )
#
##The below list is not yet defined in the central CS but can be to allow a bit of flexibility
##anything in the above list will trigger default replication policy according to the computing
##model. 
replicateListDefault = ['SEMILEPTONIC.DST', 'RADIATIVE.DST', 'MINIBIAS.DST', 'LEPTONIC.MDST', 'EW.DST',
                         'DIMUON.DST', 'DIELECTRON.DST', 'CHARM.MDST', 'CHARMCONTROL.DST', 'BHADRON.DST']

replicateList = gConfig.getValue( '/Operations/Reconstruction/ReplicationStandard', replicateListDefault )
#
##This new case will be handled outside of this template (at least initially)
#onlyOneOtherSite = ['LEPTONICFULL.DST', 'CHARMFULL.DST']
#
#The use-case of not performing replication and sending a stream to CERN is handled by the below
#CS section, similarly to the above I did not add the section to the CS. 
onlyCERNDefault = ['CALIBRATION.DST']

onlyCERN = gConfig.getValue( '/Operations/Reconstruction/OnlyCERN', onlyCERNDefault )
#
#
#dstList = streamsList # call it dstList just to accommodate future hacks
#
###################################################################################
#
#
############################################
## Now remove the banned streams
############################################
#
#if not dstList: # or not setcList:
#  gLogger.error( 'Could not find any file types to merge! Exiting...' )
#  DIRAC.exit( 2 )
#
#gLogger.info( 'List of DST file types is: %s' % ( string.join( dstList, ', ' ) ) )

###########################################
# Some parameters
###########################################

if mergingEnabled:

  #below should be integrated in the ProductionOptions utility
  dvExtraOptions = "from Configurables import RecordStream;"
  dvExtraOptions += "FileRecords = RecordStream(\"FileRecords\");"
  dvExtraOptions += "FileRecords.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'REC\'\""
  
  mergeGroupSize = 5
  mergeTransFlag = False
  
  ###########################################
  # Start the productions for each file type
  ###########################################
  
  productionList = []
  
  for mergeStream in strippOutputList:
    mergeSE = 'Tier1_M-DST'
    if mergeStream.lower() in onlyCERN:
      mergeSE = 'CERN_M-DST'
  
    mergeBKQuery = { 'ProductionID'             : recoProdID,
                     'DataQualityFlag'          : mergeDQFlag,
                     'FileType'                 : mergeStream}
  
    mergeProd = Production()
    mergeProd.setCPUTime( mergeCPU )
    mergeProd.setProdType( 'Merge' )
    wkfName = 'Merging_Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID )
    mergeProd.setWorkflowName( '%s_%s_%s' % ( mergeStream.split( '.' )[0], wkfName, appendName ) )
  
    if sysConfig:
      mergeProd.setSystemConfig( sysConfig )
  
    mergeProd.setWorkflowDescription( 'Steam merging workflow for %s files from input production %s' % ( mergeStream, recoProdID ) )
    mergeProd.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
    mergeProd.setDBTags( '{{p1CDb}}', '{{p1DDDb}}' )
  
    mergeProd.addDaVinciStep( '{{p3Ver}}', 'merge', mergeOpts, extraPackages = '{{p3EP}}', eventType = eventType,
                             inputDataType = mergeStream.lower(), extraOpts = dvExtraOptions,
                             inputProduction = recoProdID, inputData = [], outputSE = mergeSE,
                             stepID = '{{p3Step}}', stepName = '{{p3Name}}', stepVisible = '{{p3Vis}}' )
  
    mergeProd.addFinalizationStep( removeInputData = mergeRemoveInputsFlag )
    mergeProd.setInputBKSelection( mergeBKQuery )
    mergeProd.setInputDataPolicy( recoIDPolicy )
    mergeProd.setProdGroup( prodGroup )
    mergeProd.setProdPriority( mergePriority )
    mergeProd.setJobFileGroupSize( mergeGroupSize )
  #  mergeProd.setFileMask('setc;%s' %(mergeStream.lower()))
    mergeProd.setFileMask( mergeStream.lower() )
    mergeProd.setProdPlugin( mergePlugin )
  
    result = mergeProd.create( 
                              publish = publishFlag,
                              bkScript = BKscriptFlag,
                              requestID = currentReqID,
                              reqUsed = 1,
                              transformation = mergeTransFlag
                              ) #,bkProcPassPrepend=processingPass)
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
  
      productionList.append( int( prodID ) )
  
    else:
      prodID = 1
      gLogger.info( 'MC production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, prodID ) )



#################################################################################
# Create the transformations explicitly since we need to propagate the types
#################################################################################

if not replicationFlag:
  gLogger.info( 'Transformation flag is False, exiting prior to creating transformations.' )
  gLogger.info( 'Template finished successfully.' )

else:
  transDict = {'DST':replicateList} # We used to also distribute the SETC files as well

  for streamType, streamList in transDict.items():
    transBKQuery = {  'ProductionID'           : productionList,
                      'FileType'               : streamList}

    transformation = Transformation()
    transName = 'STREAM_Replication_%s_Request%s_{{pDsc}}_{{eventType}}_%s' % ( streamType, currentReqID, appendName )
    transformation.setTransformationName( transName )
    transformation.setTransformationGroup( prodGroup )
    transformation.setDescription( 'Replication of streamed %s from {{pDsc}}' % ( streamType ) )
    transformation.setLongDescription( 'Replication of streamed %s from {{pDsc}} to all Tier1s' % ( streamType ) )
    transformation.setType( 'Replication' )
    transformation.setPlugin( transformationPlugin )
    transformation.setBkQuery( transBKQuery )
    transformation.addTransformation()
    transformation.setStatus( 'Active' )
    transformation.setAgentType( 'Automatic' )
    transformation.setTransformationFamily( currentReqID )
    result = transformation.getTransformationID()
    if not result['OK']:
      gLogger.error( 'Problem during transformation creation with result:\n%s\nExiting...' % ( result ) )
      DIRAC.exit( 2 )

    gLogger.info( 'Transformation creation result: %s' % ( result ) )

gLogger.info( 'Template finished successfully.' )
DIRAC.exit( 0 )

#################################################################################
# End of the template.
#################################################################################
