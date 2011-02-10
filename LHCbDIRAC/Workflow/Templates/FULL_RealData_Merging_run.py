########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Workflow/Templates/FULL_RealData_Merging_run.py $
########################################################################

"""  The FULL Real Data Reco template creates a workflow for Brunel & DaVinci with
     configurable number of events, CPU time, jobs to extend and priority.
     
     If four steps are discovered then merging is performed with two further
     DaVinci steps.
     
     The reconstruction production is published to the BK with the processing pass
     of the first two steps and it was agreed that the merging production appends
     '-Merging' to the input processing pass name but retains the two steps of the
     input production.
"""

__RCSID__ = "$Id$"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
import sys, os, string, re
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

testFlag = '{{TemplateTest#GENERAL: A flag to set whether a test script should be generated#False}}'
publishFlag = '{{WorkflowTestFlag#GENERAL: Publish production to the production system True/False#True}}'

# workflow params for all productions
appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. slc4_ia32_gcc34#x86_64-slc5-gcc43-opt}}'
destination = '{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch#ALL}}'

#reco params
recoPriority = '{{RecoPriority#PROD-RECO: priority#7}}'
recoCPU = '{{RecoMaxCPUTime#PROD-RECO: Max CPU time in secs#1000000}}'
recoPlugin = '{{RecoPluginType#PROD-RECO: production plugin name#AtomicRun}}'
recoAncestorProd = '{{RecoAncestorProd#PROD-RECO: ancestor production if any#0}}'
recoDataSE = '{{RecoDataSE#PROD-RECO: Output Data Storage Element#Tier1-RDST}}'
recoFilesPerJob = '{{RecoFilesPerJob#PROD-RECO: Group size or number of files per job#1}}'
recoFileMask = '{{RecoOutputDataFileMask#PROD-RECO: file extensions to save (comma separated)#DST,ROOT}}'
recoTransFlag = '{{RecoTransformation#PROD-RECO: distribute output data True/False (False if merging)#False}}'
recoStartRun = '{{RecoRunStart#PROD-RECO: run start, to set the start run#0}}'
recoEndRun = '{{RecoRunEnd#PROD-RECO: run end, to set the end of the range#0}}'
recoEvtsPerJob = '{{RecoNumberEvents#Reconstruction number of events per job (set to something small for a test)#-1}}'
unmergedStreamSE = '{{RecoStreamSE#Reconstruction unmerged stream SE#Tier1-DST}}'
#merging params
mergeDQFlag = '{{MergeDQFlag#Merging DQ Flag e.g. OK#OK}}'
mergePriority = '{{MergePriority#Merging production priority#8}}'
mergePlugin = '{{MergePlugin#Merging production plugin#MergeByRun}}'
mergeRemoveInputsFlag = '{{MergeRemoveFlag#Merging remove input data flag True/False#False}}'
mergeCPU = '{{MergeMaxCPUTime#Merging Max CPU time in secs#300000}}'

#transformation params
transformationFlag = '{{TransformationEnable#Transformation flag to enable True/False#True}}'
transformationPlugin = '{{TransformationPlugin#Transformation plugin name#LHCbDSTBroadcast}}'

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
recoDQFlag = '{{inDataQualityFlag}}' #UNCHECKED
recoTransFlag = eval( recoTransFlag )
#recoBKPublishing = eval( recoBKPublishing )
mergeRemoveInputsFlag = eval( mergeRemoveInputsFlag )
transformationFlag = eval( transformationFlag )
#testProduction = eval(testProduction)
testFlag = eval( testFlag )
publishFlag = eval( publishFlag )

inputDataList = []

BKscriptFlag = False

#The below are fixed for the FULL stream
recoType = "FULL"
recoAppType = "SDST"
recoIDPolicy = 'download'

if not publishFlag:
  recoTestData = 'LFN:/lhcb/data/2010/RAW/FULL/LHCb/COLLISION10/81676/081676_0000000510.raw'
  inputDataList.append( recoTestData )
  recoIDPolicy = 'protocol'
  BKscriptFlag = True

if testFlag:
  outBkConfigName = 'certification'
  outBkConfigVersion = 'test'
  recoEvtsPerJob = '5'
  recoStartRun = '75346'
  recoEndRun = '75349'
else:
  outBkConfigName = bkConfigName
  outBkConfigVersion = bkConfigVersion

recoInputBKQuery = {
                    'SimulationConditions'     : 'All',
                    'DataTakingConditions'     : '{{simDesc}}',
                    'ProcessingPass'           : '{{inProPass}}',
                    'FileType'                 : '{{inFileType}}',
                    'EventType'                : '{{eventType}}',
                    'ConfigName'               : '{{configName}}',
                    'ConfigVersion'            : '{{configVersion}}',
                    'ProductionID'             : 0,
                    'DataQualityFlag'          : recoDQFlag
                    }

if testFlag:
  recoInputBKQuery['DataTakingConditions'] = 'All'
  recoInputBKQuery['SimulationConditions'] = '{{simDesc}}'
  gLogger.notice( 'Since the test production flag is specified reverting DataTaking to Simulation Conditions in BK Query' )

if int( recoEndRun ) and int( recoStartRun ):
  if int( recoEndRun ) < int( recoStartRun ):
    gLogger.error( 'Your end run "%s" should be less than your start run "%s"!' % ( recoEndRun, recoStartRun ) )
    DIRAC.exit( 2 )

if int( recoStartRun ):
  recoInputBKQuery['StartRun'] = int( recoStartRun )
if int( recoEndRun ):
  recoInputBKQuery['EndRun'] = int( recoEndRun )

if re.search( ',', recoRunNumbers ) and not int( recoStartRun ) and not int( recoEndRun ):
  gLogger.info( 'Found run numbers to add to BK Query...' )
  runNumbers = [int( i ) for i in recoRunNumbers.replace( ' ', '' ).split( ',' )]
  recoInputBKQuery['RunNumbers'] = runNumbers

#Have to see whether it's a two or four step request and react accordingly
mergingFlag = False

oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'
threeSteps = '{{p3App}}'
fourSteps = '{{p4App}}'
fiveSteps = '{{p5App}}'

if fiveSteps:
  gLogger.error( 'Five steps specified, not sure what to do! Exiting...' )
  DIRAC.exit( 2 )

if threeSteps and not fourSteps:
  gLogger.info( 'Reconstruction production + merging is requested...' )
  mergingFlag = True

if fourSteps:
  gLogger.error( 'Reconstruction production with tagging + merging is requested but this is no longer supported! Exiting...' )
  DIRAC.exit( 2 )

#recoScriptFlag = False
#if not recoBKPublishing:
#  recoScriptFlag = True

recoFileMask = ''

#Sort out the reco file mask
if recoFileMask:
  maskList = [m.lower() for m in recoFileMask.replace( ' ', '' ).split( ',' )]
  if not recoAppType.lower() in maskList:
    maskList.append( recoAppType.lower() )
  recoFileMask = string.join( maskList, ';' )

strippingOutput = BKClient.getStepOutputFiles( int( '{{p2Step}}' ) )
if not strippingOutput:
  gLogger.error( 'Error getting res from BKK: %s', strippingOutput['Message'] )
  DIRAC.exit( 2 )

strippingOutputList = [x[0] for x in strippingOutput['Value']['Records']]

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
production.setWorkflowDescription( "%s Real data FULL reconstruction production." % ( prodGroup ) )
production.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, '{{simDesc}}' )
production.setInputBKSelection( recoInputBKQuery )
production.setDBTags( '{{p1CDb}}', '{{p1DDDb}}' )

brunelOptions = "{{p1Opt}}"
production.addBrunelStep( "{{p1Ver}}", recoAppType.lower(), brunelOptions, extraPackages = '{{p1EP}}',
                         eventType = '{{eventType}}', inputData = inputDataList, inputDataType = 'mdf', outputSE = recoDataSE,
                         dataType = 'Data', numberOfEvents = recoEvtsPerJob, histograms = True,
                         stepID = '{{p1Step}}', stepName = '{{p1Name}}', stepVisible = '{{p1Vis}}' )
dvOptions = "{{p2Opt}}"
production.addDaVinciStep( "{{p2Ver}}", "stripping", dvOptions, extraPackages = '{{p2EP}}', inputDataType = recoAppType.lower(),
                          dataType = 'Data', outputSE = unmergedStreamSE, histograms = True,
                          extraOutput = strippingOutputList,
                          stepID = '{{p2Step}}', stepName = '{{p2Name}}', stepVisible = '{{p2Vis}}' )

production.addFinalizationStep()
production.setInputBKSelection( recoInputBKQuery )
production.setProdGroup( prodGroup )
production.setFileMask( recoFileMask )
production.setProdPriority( recoPriority )
production.setProdPlugin( recoPlugin )
production.setInputDataPolicy( recoIDPolicy )

#################################################################################
# End of production API script, now what to do with the production object
#################################################################################

if ( not publishFlag ) and ( testFlag ):

  gLogger.info( 'Production test will be launched with number of events set to %s.' % ( recoEvtsPerJob ) )
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
                           derivedProduction = int( recoAncestorProd ),
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
# Create the merging productions if there are enough workflow steps
#################################################################################

if not mergingFlag:
  DIRAC.exit( 0 )

##################################################################################
### TEMPORARY HACK SINCE THERE IS NO REASONABLE WAY TO GET THE LIST OF STREAMS ###
##################################################################################

#The below list is not yet defined in the central CS but can be to allow a bit of flexibility
#anything in the above list will trigger a merging production.
streamsListDefault = ['SEMILEPTONIC.DST', 'RADIATIVE.DST', 'MINIBIAS.DST', 'LEPTONIC.MDST', 'EW.DST',
                      'DIMUON.DST', 'DIELECTRON.DST', 'CHARM.MDST', 'CHARMCONTROL.DST', 'BHADRON.DST',
                      'LEPTONICFULL.DST', 'CHARMFULL.DST', 'CALIBRATION.DST']

streamsList = gConfig.getValue( '/Operations/Reconstruction/MergingStreams', streamsListDefault )

#The below list is not yet defined in the central CS but can be to allow a bit of flexibility
#anything in the above list will trigger default replication policy according to the computing
#model. 
replicateListDefault = ['SEMILEPTONIC.DST', 'RADIATIVE.DST', 'MINIBIAS.DST', 'LEPTONIC.MDST', 'EW.DST',
                         'DIMUON.DST', 'DIELECTRON.DST', 'CHARM.MDST', 'CHARMCONTROL.DST', 'BHADRON.DST']

replicateList = gConfig.getValue( '/Operations/Reconstruction/ReplicationStandard', replicateListDefault )

#This new case will be handled outside of this template (at least initially)
onlyOneOtherSite = ['LEPTONICFULL.DST', 'CHARMFULL.DST']

#The use-case of not performing replication and sending a stream to CERN is handled by the below
#CS section, similarly to the above I did not add the section to the CS. 
onlyCERNDefault = ['CALIBRATION.DST']

onlyCERN = gConfig.getValue( '/Operations/Reconstruction/OnlyCERN', onlyCERNDefault )


dstList = streamsList # call it dstList just to accommodate future hacks

##################################################################################


###########################################
# Now remove the banned streams
###########################################

if not dstList: # or not setcList:
  gLogger.error( 'Could not find any file types to merge! Exiting...' )
  DIRAC.exit( 2 )

gLogger.info( 'List of DST file types is: %s' % ( string.join( dstList, ', ' ) ) )

###########################################
# Some parameters
###########################################

#below should be integrated in the ProductionOptions utility
dvExtraOptions = "from Configurables import RecordStream;"
dvExtraOptions += "FileRecords = RecordStream(\"FileRecords\");"
dvExtraOptions += "FileRecords.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'REC\'\""

mergeGroupSize = 5
mergeTransFlag = False

mergeOpts = '{{p3Opt}}'
taggingOpts = '{{p4Opt}}'

###########################################
# Start the productions for each file type
###########################################

productionList = []

for mergeStream in dstList:
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
  mergeProd.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, '{{simDesc}}' )
  mergeProd.setDBTags( '{{p1CDb}}', '{{p1DDDb}}' )

  mergeProd.addDaVinciStep( '{{p3Ver}}', 'merge', mergeOpts, extraPackages = '{{p3EP}}', eventType = '{{eventType}}',
                           inputDataType = mergeStream.lower(), extraOpts = dvExtraOptions,
                           inputProduction = recoProdID, inputData = [], outputSE = mergeSE,
                           stepID = '{{p3Step}}', stepName = '{{p3Name}}', stepVisible = '{{p3Vis}}' )

# N.B. Now we remove the tagging step.
#  mergeProd.addDaVinciStep('{{p4Ver}}','setc',taggingOpts,extraPackages='{{p4EP}}',inputDataType=mergeStream.lower())

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
                            ) #,bkProcPassPrepend='{{inProPass}}')
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

if not transformationFlag:
  gLogger.info( 'Transformation flag is False, exiting prior to creating transformations.' )
  gLogger.info( 'Template finished successfully.' )
  DIRAC.exit( 0 )

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
