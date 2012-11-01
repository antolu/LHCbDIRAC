""" merging the MDF files
"""

__RCSID__ = "$Id"


from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gLogger

from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

gLogger = gLogger.getSubLogger( 'mergeMDF_run.py' )
BKClient = BookkeepingClient()

###########################################
# Configurable parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True to create validation productions#False}}'

# workflow params for all productions
sysConfig = 'ANY'

# workflow params for all productions
#destination = 'LCG.CERN.ch'
startRun = '{{RunStart#GENERAL: run start, to set the start run#0}}'
endRun = '{{RunEnd#GENERAL: run end, to set the end of the range#0}}'
runs = '{{Runs#GENERAL: discrete list of run numbers (do not mix with start/endrun)#}}'

priority = '{{MergePriority#PROD-Merging: priority#8}}'
plugin = '{{MergePlugin#PROD-Merging: plugin#ByRunFileTypeSizeWithFlush}}'
CPU = '{{MergeMaxCPUTime#PROD-Merging: Max CPU time in secs#100000}}'

policy = '{{MergeIDPolicy#PROD-Merging: policy for input data access (download or protocol)#download}}'
outputSE = '{{MergeStreamSE#PROD-Merging: output data SE (merged streams)#CERN-FREEZER}}'

#Other parameters from the request page
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
fileType = '{{inFileType}}'

certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
validationFlag = eval( validationFlag )

oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'

if twoSteps:
  gLogger.error( 'Two steps specified, not sure what to do! Exiting...' )
  error = True
elif oneStep:
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
  gLogger.error( 'No steps??' )
  error = True

inputDataStep = []

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
  inputDataStep = ['/lhcb/data/2011/RAW/EXPRESS/LHCb/COLLISION11/102360/102360_0000000031.raw',
                   '/lhcb/data/2011/RAW/EXPRESS/LHCb/COLLISION11/97887/097887_0000000013.raw']
  BKscriptFlag = True
else:
  BKscriptFlag = False

if testFlag:
  outBkConfigName = 'certification'
  outBkConfigVersion = 'test'
  startRun = '90645'
  endRun = '90645'
  recoCPU = '100000'
  dataTakingCond = 'Beam3500GeV-VeloClosed-MagDown'
  processingPass = 'Real Data'
  fileType = 'RAW'
else:
  outBkConfigName = bkConfigName
  outBkConfigVersion = bkConfigVersion

if validationFlag:
  outBkConfigName = 'validation'


################################################################################
# BK Query
################################################################################

input = BKClient.getStepInputFiles( mergeStep )
if not input:
  gLogger.error( 'Error getting res from BKK: %s', input['Message'] )
  DIRAC.exit( 2 )

inputList = [x[0].lower() for x in input['Value']['Records']]
if len( inputList ) == 1:
  input = inputList[0]
else:
  gLogger.error( 'Multiple inputs...?', input['Message'] )
  DIRAC.exit( 2 )

output = BKClient.getStepOutputFiles( mergeStep )
if not output:
  gLogger.error( 'Error getting res from BKK: %s', output['Message'] )
  DIRAC.exit( 2 )

outputList = [x[0].lower() for x in output['Value']['Records']]

histFlag = False
for sOL in outputList:
  if 'HIST' in sOL:
    histFlag = True

DQFlag = DQFlag.replace( ',', ';;;' ).replace( ' ', '' )

inputBKQuery = {
                'DataTakingConditions'     : dataTakingCond,
                'ProcessingPass'           : processingPass,
                'FileType'                 : fileType,
                'EventType'                : eventType,
                'ConfigName'               : bkConfigName,
                'ConfigVersion'            : bkConfigVersion,
                'ProductionID'             : 0,
                'DataQualityFlag'          : inDQFlag,
                'Visible'                  : 'Yes'
                }


if int( endRun ) and int( startRun ):
  if int( endRun ) < int( startRun ):
    gLogger.error( 'Your end run ("%s") should not be less than your start run ("%s")!' % ( endRun, startRun ) )
    DIRAC.exit( 2 )

if int( startRun ):
  inputBKQuery['StartRun'] = int( startRun )
if int( endRun ):
  inputBKQuery['EndRun'] = int( endRun )

if runs:
  inputBKQuery['RunNumbers'] = runs.replace( ',', ';;;' ).replace( ' ', '' )



#################################################################################
# Creation of the production
#################################################################################


prod = Production()

prod.setJobParameters( { 'CPUTime': CPU } )
prod.setProdType( 'Merge' )
wkfName = 'Request%s_{{pDsc}}_{{eventType}}' % ( currentReqID ) #Rest can be taken from the details in the monitoring
prod.setWorkflowName( 'MERGE_%s_%s' % ( wkfName, appendName ) )
prod.setWorkflowDescription( "%s real data merging production." % ( prodGroup ) )
prod.setBKParameters( outBkConfigName, outBkConfigVersion, prodGroup, dataTakingCond )
prod.setInputBKSelection( inputBKQuery )
prod.setDBTags( '', '' )
prod.setJobParameters( {'InputDataPolicy': 'download' } )
prod.setProdPlugin( plugin )

prod._addGaudiStep( '', '', '', '-1', '', '', eventType, '',
                    outputSE = outputSE, inputData = inputDataStep, inputDataType = 'RAW',
                    stepID = mergeStep, stepName = mergeName, stepVisible = mergeVisibility, stepPass = mergePass,
                    modules = [ 'MergeMDF', 'BookkeepingReport'] )

prod.addFinalizationStep( ['UploadOutputData',
                           'UploadLogFile',
                           'FailoverRequest'] )

prod.setProdGroup( prodGroup )
prod.setProdPriority( priority )
prod.setJobFileGroupSize( 5 )
#prod.setFileMask(  )

#################################################################################
# Publishing of the production
#################################################################################

if ( not publishFlag ) and ( testFlag ):

  gLogger.info( 'Production test will be launched' )
  try:
    result = prod.runLocal()
    gLogger.info( 'Template finished successfully' )
    if result['OK']:
      DIRAC.exit( 0 )
    else:
      gLogger.error( 'Something wrong with execution!' )
      DIRAC.exit( 2 )
  except Exception, x:
    gLogger.error( 'Production test failed with exception' + str( x ) )
    DIRAC.exit( 2 )

result = prod.create( 
                     publish = publishFlag,
                     bkQuery = inputBKQuery,
#                     groupSize = filesPerJob,
                     bkScript = BKscriptFlag,
#                     derivedProduction = int( ancestorProd ),
                     requestID = currentReqID,
#                     reqUsed = 1,
                     transformation = False
                     )
if not result['OK']:
  gLogger.error( 'Production creation failed with result:\n%s\ntemplate is exiting...' % ( result ) )
  DIRAC.exit( 2 )

if publishFlag:
  diracProd = DiracProduction()

  prodID = result['Value']

  msg = 'Production %s successfully created ' % ( prodID )

  if testFlag:
    diracProd.production( prodID, 'manual', printOutput = True )
    msg = msg + 'and started in manual mode.'
  else:
    diracProd.production( prodID, 'automatic', printOutput = True )
    msg = msg + 'and started in automatic mode.'
  gLogger.info( msg )

else:
  prodID = 1
  gLogger.info( 'Production creation completed but not published (publishFlag was %s). Setting ID = %s (useless, just for the test)' % ( publishFlag, prodID ) )

