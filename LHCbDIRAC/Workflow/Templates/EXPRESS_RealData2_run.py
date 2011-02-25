########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Workflow/Templates/FULL_RealData_Merging_run.py $
########################################################################


"""  The EXPRESS Real Data Reco template creates a workflow for Brunel & DaVinci with
     configurable number of events, CPU time, jobs to extend and priority.
     
     As this template has evolved it started to be used for "special" reconstructions
     in addition to the standard express stream processing. For studies that do
     not require execution or distribution outside of CERN this template is very 
     suitable.
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

gLogger = gLogger.getSubLogger( 'EXPRESS_RealData_Merging_run.py' )
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

#reco params
recoPriority = '{{RecoPriority#PROD-RECO: priority#7}}'
recoCPU = '{{RecoMaxCPUTime#PROD-RECO: Max CPU time in secs#1000000}}'
recoPlugin = '{{RecoPluginType#PROD-RECO: production plugin name#AtomicRun}}'
recoAncestorProd = '{{RecoAncestorProd#PROD-RECO: ancestor production if any#0}}'
recoDataSE = '{{RecoDataSE#PROD-RECO: Output Data Storage Element#Tier1-RDST}}'
recoFilesPerJob = '{{RecoFilesPerJob#PROD-RECO: Group size or number of files per job#1}}'
recoTransFlag = '{{RecoTransformation#PROD-RECO: distribute output data True/False (False if merging)#False}}'
recoStartRun = '{{RecoRunStart#PROD-RECO: run start, to set the start run#0}}'
recoEndRun = '{{RecoRunEnd#PROD-RECO: run end, to set the end of the range#0}}'
unmergedStreamSE = '{{RecoStreamSE#PROD-RECO: unmerged stream SE#Tier1-DST}}'

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
certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )

dataTakingCond = '{{simDesc}}'
processingPass = '{{inProPass}}'
BKfileType = '{{inFileType}}'
eventType = '{{eventType}}'
recoEvtsPerJob = '-1'

if certificationFlag:
  publishFlag = True
  testFlag = True
  mergingFlag = True
  replicationFlag = False
if localTestFlag:
  publishFlag = False
  testFlag = True
  mergingFlag = False

inputDataList = []

BKscriptFlag = False

#The below are fixed for the FULL stream
recoType = "FULL"
recoAppType = "DST"
recoIDPolicy = 'download'

if not publishFlag:
  recoTestData = 'LFN:/lhcb/data/2010/RAW/EXPRESS/LHCb/COLLISION10/81676/081676_0000000417.raw'
  inputDataList.append( recoTestData )
  recoIDPolicy = 'protocol'
  BKscriptFlag = True

if testFlag:
  outBkConfigName = 'certification'
  outBkConfigVersion = 'test'
  recoEvtsPerJob = '5'
  recoStartRun = '75346'
  recoEndRun = '75349'
  recoCPU = '100000'
  dataTakingCond = 'Beam3500GeV-VeloClosed-MagDown'
  processingPass = 'Real Data'
  BKfileType = 'RAW'
  eventType = '91000000'
  mergeRemoveInputsFlag = True
else:
  outBkConfigName = bkConfigName
  outBkConfigVersion = bkConfigVersion

if certificationFlag:
  recoEvtsPerJob = '-1'

recoInputBKQuery = {
                    'DataTakingConditions'     : dataTakingCond,
                    'ProcessingPass'           : processingPass,
                    'FileType'                 : BKfileType,
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

if re.search( ',', recoRunNumbers ) and not int( recoStartRun ) and not int( recoEndRun ):
  gLogger.info( 'Found run numbers to add to BK Query...' )
  runNumbers = [int( i ) for i in recoRunNumbers.replace( ' ', '' ).split( ',' )]
  recoInputBKQuery['RunNumbers'] = runNumbers

#Have to confirm this isn't a FULL request for example
threeSteps = '{{p3App}}'
if threeSteps:
  gLogger.error( 'Three steps specified, not sure what to do! Exiting...' )
  DIRAC.exit( 2 )

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
production.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
production.setInputBKSelection( recoInputBKQuery )
production.setDBTags( '{{p1CDb}}', '{{p1DDDb}}' )

brunelOptions = "{{p1Opt}}"
production.addBrunelStep( "{{p1Ver}}", recoAppType.lower(), brunelOptions, extraPackages = '{{p1EP}}',
                         eventType = eventType, inputData = inputDataList, inputDataType = 'mdf', outputSE = recoDataSE,
                         dataType = 'Data', numberOfEvents = recoEvtsPerJob, histograms = True,
                         stepID = '{{p1Step}}', stepName = '{{p1Name}}', stepVisible = '{{p1Vis}}' )

#Since this template is also used for "special" processings only add DaVinci step if defined
if "{{p2Ver}}":
  production.addDaVinciStep( "{{p2Ver}}", "davincihist", "{{p2Opt}}", extraPackages = '{{p2EP}}',
                            inputDataType = recoAppType.lower(), histograms = True,
                            stepID = '{{p2Step}}', stepName = '{{p2Name}}', stepVisible = '{{p2Vis}}' )

#QUESTO E' IN FULL
#production.addDaVinciStep( "{{p2Ver}}", "stripping", dvOptions, extraPackages = '{{p2EP}}', inputDataType = recoAppType.lower(),
#                          dataType = 'Data', outputSE = unmergedStreamSE, histograms = True,
#                          extraOutput = strippingOutputList,
#                          stepID = '{{p2Step}}', stepName = '{{p2Name}}', stepVisible = '{{p2Vis}}' )

production.addFinalizationStep()
production.setProdGroup( prodGroup )
#production.setFileMask( recoFileMask )
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
# End of the template.
#################################################################################
