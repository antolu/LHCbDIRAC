########################################################################
# $HeadURL $
########################################################################

"""  The Real Data template creates a workflow for Brunel (and eventually DaVinci) with
     configurable number of events, CPU time, jobs to extend and priority.

     In can be used for FULL and EXPRESS processings.

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

import itertools
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gLogger
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

gLogger = gLogger.getSubLogger( 'RealData_run.py' )
BKClient = BookkeepingClient()

#################################################################################
# Below here is the production API script with notes
#################################################################################
#from LHCbDIRAC.Interfaces.API.Production import Production
#from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
#from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation

###########################################
# Configurable parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
expressFlag = '{{expressFlag#GENERAL: Set True for EXPRESS#False}}'

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
recoEvtsPerJob = '{{RecoNumberEvents#PROD-RECO: number of events per job (set to something small for a test)#-1}}'
unmergedStreamSE = '{{RecoStreamSE#PROD-RECO: unmerged stream SE#Tier1-DST}}'
#merging params
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
recoDQFlag = '{{inDataQualityFlag}}' #UNCHECKED
recoTransFlag = eval( recoTransFlag )
#recoBKPublishing = eval( recoBKPublishing )
mergeRemoveInputsFlag = eval( mergeRemoveInputsFlag )
replicationFlag = eval( replicationFlag )
#testProduction = eval(testProduction)
certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
expressFlag = eval( expressFlag )

dataTakingCond = '{{simDesc}}'
processingPass = '{{inProPass}}'
BKfileType = '{{inFileType}}'
eventType = '{{eventType}}'

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

#step1_out = BKClient.getStepOutputFiles( int( '{{p1Step}}' ) )
#if not step1_out:
#  gLogger.error( 'Error getting res from BKK: %s', step1_out['Message'] )
#  DIRAC.exit( 2 )
#
#step1_outList = [x[0] for x in step1_out['Value']['Records']]
#if len( step1_outList ) == 1:
#  step1_outList = step1_outList[0]
#  step1Type = step1_outList

#step2_out = BKClient.getStepOutputFiles( int( '{{p2Step}}' ) )
#if not step2_out:
#  gLogger.error( 'Error getting res from BKK: %s', step2_out['Message'] )
#  DIRAC.exit( 2 )
#
#step2_outList = [x[0] for x in step2_out['Value']['Records']]
#if len( step2_outList ) == 1:
#  step2_outList = step2_outList[0]
#  step1Type = step1_outList
#else:

stepIDsList = [ '{{p1Step}}', '{{p2Step}}', '{{p3Step}}', '{{p4Step}}', '{{p5Step}}', '{{p6Step}}', '{{p7Step}}']
stepNamesList = [ '{{p1Name}}', '{{p2Name}}', '{{p3Name}}', '{{p4Name}}', '{{p5Name}}', '{{p6Name}}', '{{p7Name}}']
appsList = [ '{{p1App}}', '{{p2App}}', '{{p3App}}', '{{p4App}}', '{{p5App}}', '{{p6App}}', '{{p7App}}']
versionsList = [ '{{p1Ver}}', '{{p2Ver}}', '{{p3Ver}}', '{{p4Ver}}', '{{p5Ver}}', '{{p6Ver}}', '{{p7Ver}}']
optionsList = [ '{{p1Opt}}', '{{p2Opt}}', '{{p3Opt}}', '{{p4Opt}}', '{{p5Opt}}', '{{p6Opt}}', '{{p7Opt}}']
visibilityList = [ '{{p1Vis}}', '{{p2Vis}}', '{{p3Vis}}', '{{p4Vis}}', '{{p5Vis}}', '{{p6Vis}}', '{{p7Vis}}']

for stepN in range( len( stepIDsList ) ):
  if not stepIDsList[ stepN ]:
    del stepIDsList[ stepN: ]
    break

stepsDictList = []

for step, stepOrder in itertools.izip( stepIDsList, range( len( stepIDsList ) ) ):

  stepDict = {}
  stepDict['StepID'] = step
  stepDict['StepName'] = stepNamesList[stepOrder]
  stepDict['Visibility'] = visibilityList[stepOrder]
  stepDict['AppName'] = appsList[stepOrder]
  stepDict['AppVersion'] = versionsList[stepOrder]
  stepDict['Options'] = optionsList[stepOrder]
  stepInput = BKClient.getstepInputFiles( int( step ) )
  if not stepInput:
    gLogger.error( 'Error getting res from BKK: %s', stepInput['Message'] )
    DIRAC.exit( 2 )
  stepDict['InputData'] = [x[0] for x in stepInput['Value']['Records']]
  stepOutput = BKClient.getStepOutputFiles( int( step ) )
  if not stepOutput:
    gLogger.error( 'Error getting res from BKK: %s', stepOutput['Message'] )
    DIRAC.exit( 2 )
  stepDict['InputData'] = [x[0] for x in stepOutput['Value']['Records']]

  stepsDictList.append( stepDict )

print stepsDictList

DIRAC.exit( 0 )
