"""  The Reconstruction monitoring template creates a workflow for Brunel & DaVinci with
     configurable number of events, CPU time, jobs to extend and priority.
     
     It can be used for EXPRESS and for FULL DataReconstruction productions, 
     with DaVinci used for monitoring purposes.  
     
     As this template has evolved it started to be used for "special" reconstructions
     in addition to the standard express stream processing. For studies that do
     not require execution or distribution outside of CERN this template is very 
     suitable.
"""

__RCSID__ = "$Id$"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import DIRAC

from DIRAC import gLogger
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

gLogger = gLogger.getSubLogger( 'RealData_Merging_run.py' )
bkClient = BookkeepingClient()

#################################################################################
# Below here is the production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

###########################################
# Configurable parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True to create validation productions#False}}'

# workflow params for all productions
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc46-opt#ANY}}'

#reco params
recoPriority = '{{RecoPriority#PROD-RECO: priority#2}}'
recoCPU = '{{RecoMaxCPUTime#PROD-RECO: Max CPU time in secs#1000000}}'
recoPlugin = '{{RecoPluginType#PROD-RECO: production plugin name#AtomicRun}}'
recoAncestorProd = '{{RecoAncestorProd#PROD-RECO: ancestor production if any#0}}'
recoDataSE = '{{RecoDataSE#PROD-RECO: Output Data Storage Element#Tier1-BUFFER}}'
recoFilesPerJob = '{{RecoFilesPerJob#PROD-RECO: Group size or number of files per job#1}}'
recoTransFlag = '{{RecoTransformation#PROD-RECO: distribute output data True/False (False if merging)#False}}'
recoStartRun = '{{RecoRunStart#PROD-RECO: run start, to set the start run#0}}'
recoEndRun = '{{RecoRunEnd#PROD-RECO: run end, to set the end of the range#0}}'
recoType = '{{RecoType#PROD-RECO: DataReconstruction or DataReprocessing#DataReconstruction}}'
recoRuns = '{{RecoRuns#PROD-RECO: discrete list of run numbers (do not mix with start/endrun)#}}'

###########################################
# Fixed and implied parameters 
###########################################

currentReqID = int( '{{ID}}' )
prodGroup = '{{pDsc}}'
#used in case of a test e.g. certification etc.
bkConfigName = '{{configName}}'
bkConfigVersion = '{{configVersion}}'

#Other parameters from the request page
recoDQFlag = '{{inDataQualityFlag}}' #UNCHECKED
recoTransFlag = eval( recoTransFlag )
certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
validationFlag = eval( validationFlag )

dataTakingCond = '{{simDesc}}'
processingPass = '{{inProPass}}'
bkFileType = '{{inFileType}}'
eventType = '{{eventType}}'
recoEvtsPerJob = '-1'

if certificationFlag or localTestFlag:
  testFlag = True
  replicationFlag = False
  if certificationFlag:
    publishFlag = True
  if localTestFlag:
    publishFlag = False
else:
  publishFlag = True
  testFlag = False

inputDataList = []

bkScriptFlag = False

recoIDPolicy = 'download'

if not publishFlag:
  #this is 1380Gev MagUp
  #recoTestData = 'LFN:/lhcb/data/2011/RAW/FULL/LHCb/COLLISION11/88162/088162_0000000020.raw'
  #this is collision11
  recoTestData = 'LFN:/lhcb/data/2011/RAW/FULL/LHCb/COLLISION11/89333/089333_0000000003.raw'

  inputDataList.append( recoTestData )
  recoIDPolicy = 'protocol'
  bkScriptFlag = True

if testFlag:
  outBkConfigName = 'certification'
  outBkConfigVersion = 'test'
  recoEvtsPerJob = '25'
  recoStartRun = '93718'
  recoEndRun = '93720'
  recoCPU = '100000'
  dataTakingCond = 'Beam3500GeV-VeloClosed-MagUp'
  processingPass = 'Real Data'
  bkFileType = 'RAW'
  eventType = '90000000'
  recoDQFlag = 'ALL'
else:
  outBkConfigName = bkConfigName
  outBkConfigVersion = bkConfigVersion

if certificationFlag:
  recoEvtsPerJob = '-1'

if validationFlag:
  outBkConfigName = 'validation'

recoDQFlag = recoDQFlag.replace( ',', ';;;' ).replace( ' ', '' )

recoInputBKQuery = {
                    'DataTakingConditions'     : dataTakingCond,
                    'ProcessingPass'           : processingPass,
                    'FileType'                 : bkFileType,
                    'EventType'                : eventType,
                    'ConfigName'               : bkConfigName,
                    'ConfigVersion'            : bkConfigVersion,
                    'ProductionID'             : 0,
                    'DataQualityFlag'          : recoDQFlag
                    }

if int( recoEndRun ) and int( recoStartRun ):
  if int( recoEndRun ) < int( recoStartRun ):
    gLogger.error( 'Your end run "%s" should be less than your start run "%s"!' % ( recoEndRun, recoStartRun ) )
    DIRAC.exit( 2 )

if int( recoStartRun ):
  recoInputBKQuery['StartRun'] = int( recoStartRun )
if int( recoEndRun ):
  recoInputBKQuery['EndRun'] = int( recoEndRun )

if recoRuns:
  recoInputBKQuery['RunNumbers'] = recoRuns.replace( ',', ';;;' ).replace( ' ', '' )

#Have to confirm this isn't a FULL request for example
threeSteps = '{{p3App}}'
if threeSteps:
  gLogger.error( 'Three steps specified, not sure what to do! Exiting...' )
  DIRAC.exit( 2 )


#################################################################################
# Create the reconstruction production
#################################################################################

production = Production( BKKClientIn = bkClient )

if sysConfig:
  production.setJobParameters( { 'SystemConfig': sysConfig } )

production.setJobParameters( {'CPUTime': recoCPU } )
production.setProdType( recoType )
wkfName = 'Request%s_%s_{{eventType}}' % ( currentReqID, prodGroup.replace( ' ', '' ) )
#Rest can be taken from the details in the monitoring
production.LHCbJob.workflow.setName( '%s_%s_%s' % ( recoType, wkfName, appendName ) )
production.LHCbJob.workflow.setDescription( "%sRealdataFULLreconstructionproduction." % ( prodGroup.replace( ' ', '' ) ) )
production.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
production.setInputBKSelection( recoInputBKQuery )
production.setDBTags( '{{p1CDb}}', '{{p1DDDb}}' )

brunelOptions = "{{p1Opt}}"

brunelOutput = bkClient.getStepOutputFiles( int( '{{p1Step}}' ) )
if not brunelOutput['OK']:
  gLogger.error( 'Error getting res from BKK: %s', brunelOutput['Message'] )
  DIRAC.exit( 2 )

brunelOF = bkClient.getAvailableSteps( {'StepId':int( '{{p1Step}}' )} )['Value']['Records'][0][12]

histograms = False
brunelOutput = [x[0].lower() for x in brunelOutput['Value']['Records']]
if 'brunelhist' in brunelOutput:
  histograms = True


if len( brunelOutput ) > 2:
  gLogger.error( 'Too many output file types in Brunel step' )
  DIRAC.exit( 2 )
if len( brunelOutput ) == 2:
  if histograms:
    brunelOutput.remove( 'brunelhist' )
    brunelOutput = brunelOutput[0]
else:
  brunelOutput = brunelOutput[0]


production.addBrunelStep( "{{p1Ver}}", brunelOutput.lower(), brunelOptions, extraPackages = '{{p1EP}}',
                         eventType = eventType, inputData = inputDataList, inputDataType = 'mdf',
                         outputSE = recoDataSE,
                         dataType = 'Data', numberOfEvents = recoEvtsPerJob, histograms = histograms,
                         stepID = '{{p1Step}}', stepName = '{{p1Name}}', stepVisible = '{{p1Vis}}',
                         stepPass = '{{p1Pass}}', optionsFormat = brunelOF )

#Since this template is also used for "special" processings only add DaVinci step if defined
if "{{p2Ver}}":

  daVinciOptions = "{{p2Opt}}"

  daVinciInput = bkClient.getStepInputFiles( int( '{{p2Step}}' ) )
  if not daVinciInput['OK']:
    gLogger.error( 'Error getting res from BKK: %s', daVinciInput['Message'] )
    DIRAC.exit( 2 )

  daVinciInput = daVinciInput['Value']['Records'][0][0].lower()

  if daVinciInput != brunelOutput:
    gLogger.error( 'Brunel output (%s) is different from DaVinci input (%s)' % ( brunelOutput, daVinciInput ) )
    DIRAC.exit( 2 )

  daVinciOutput = bkClient.getStepOutputFiles( int( '{{p2Step}}' ) )
  if not daVinciOutput['OK']:
    gLogger.error( 'Error getting res from BKK: %s', daVinciOutput['Message'] )
    DIRAC.exit( 2 )

  histograms = False
  daVinciOutput = [x[0].lower() for x in daVinciOutput['Value']['Records']]
  if 'davincihist' in daVinciOutput:
    histograms = True

  davinciOF = bkClient.getAvailableSteps( {'StepId':int( '{{p2Step}}' )} )['Value']['Records'][0][12]
  if not davinciOF:
    davinciOF = 'DQ'

  if len( daVinciOutput ) > 2:
    gLogger.error( 'Too many output file types in DaVinci step' )
    DIRAC.exit( 2 )
  if len( daVinciOutput ) == 2:
    if histograms:
      daVinciOutput.remove( 'davincihist' )
      daVinciOutput = daVinciOutput[0]
  else:
    daVinciOutput = daVinciOutput[0]

  production.addDaVinciStep( "{{p2Ver}}", daVinciOutput, daVinciOptions, extraPackages = '{{p2EP}}',
                            inputDataType = daVinciInput.lower(), histograms = histograms, outputSE = 'Tier1-DST',
                            stepID = '{{p2Step}}', stepName = '{{p2Name}}', stepVisible = '{{p2Vis}}',
                            stepPass = '{{p2Pass}}', optionsFormat = davinciOF )

production.addFinalizationStep( ['UploadOutputData',
                                 'UploadLogFile',
                                 'FailoverRequest'] )
production.prodGroup = prodGroup
production.priority = str( recoPriority )
production.plugin = recoPlugin
production.setJobParameters( { 'InputDataPolicy': recoIDPolicy } )
production.setFileMask( [brunelOutput, 'HIST'] )
production.setJobFileGroupSize( recoFilesPerJob )

#################################################################################
# End of production API script, now what to do with the production object
#################################################################################

if ( not publishFlag ) and ( testFlag ):

  gLogger.info( 'Production test will be launched with number of events set to %s.' % ( recoEvtsPerJob ) )
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
                           bkQuery = recoInputBKQuery,
                           groupSize = recoFilesPerJob, #useless
                           derivedProduction = int( recoAncestorProd ),
                           bkScript = bkScriptFlag,
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
  gLogger.info( 'Reconstruction production creation completed but not published (publishFlag was %s). \
  Setting ID = %s (useless, just for the test)' % ( publishFlag, recoProdID ) )

#################################################################################
# End of the template.
#################################################################################
