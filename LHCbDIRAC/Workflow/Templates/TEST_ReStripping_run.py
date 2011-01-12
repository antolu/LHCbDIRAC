########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Workflow/Templates/TEST_ReStripping_run.py $
########################################################################

"""  A workflow for DaVinci for the restripping - test!
"""

__RCSID__ = "$Id: $"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
import sys,os,string,re
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gConfig, gLogger
#gLogger = gLogger.getSubLogger('FULL_RealData_Merging_run.py')

#################################################################################
# Below here is the production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation

###########################################
# Configurable parameters
###########################################

# workflow params for all productions
appendName = '{{WorkflowAppendName#Workflow string to append to production name#1}}'
sysConfig = '{{WorkflowSystemConfig#Workflow system config e.g. slc4_ia32_gcc34#x86_64-slc5-gcc43-opt}}'
destination = '{{WorkflowDestination#Workflow destination site e.g. LCG.CERN.ch#ALL}}'
testFlag = '{{WorkflowTestFlag#Workflow testing flag for local test True/False#False}}'
testProduction = '{{WorkflowTestFlag#Workflow testing flag e.g. for certification True/False#False}}'

#stripp params
strippPriority = '{{StrippPriority#Stripping production priority#7}}'
strippCPU = '{{StrippMaxCPUTime#Stripping Max CPU time in secs#1000000}}'
strippPlugin = '{{StrippPluginType#Stripping production plugin name#AtomicRun}}'
strippAncestorProd = '{{StrippAncestorProd#Stripping ancestor production if any#0}}'
strippDataSE = '{{StrippDataSE#Stripping Output Data Storage Element#Tier1-RDST}}'
strippFilesPerJob = '{{StrippFilesPerJob#Stripping Group size or number of files per job#1}}'
strippFileMask = '{{StrippOutputDataFileMask#Stripping file extns to save (comma separated)#DST,ROOT}}'
strippTransFlag = '{{StrippTransformation#Stripping distribute output data True/False (False if merging)#False}}'
strippBKPublishing = '{{BKPublishFlag#Stripping publish this to the BK#True}}'
strippStartRun = '{{StrippRunStart#Stripping run start, to set the start run#0}}'
strippEndRun = '{{StrippRunEnd#Stripping run end, to set the end of the range#0}}'
strippEvtsPerJob = '{{StrippNumberEvents#Stripping number of events per job (set to something small for a test)#-1}}'
unmergedStreamSE = '{{StrippStreamSE#Stripping unmerged stream SE#Tier1-DST}}'
#merging params
mergeDQFlag = '{{MergeDQFlag#Merging DQ Flag e.g. OK#OK}}'
mergePriority = '{{MergePriority#Merging production priority#8}}'
mergePlugin = '{{MergePlugin#Merging production plugin#MergeByRun}}'
mergeRemoveInputsFlag = '{{MergeRemoveFlag#Merging remove input data flag True/False#False}}'
mergeProdGroup = '{{MergeProdGroup#Merging what is appended to the BK proc pass#-Merged}}'
mergeCPU = '{{MergeMaxCPUTime#Merging Max CPU time in secs#300000}}'

#transformation params
transformationFlag = '{{TransformationEnable#Transformation flag to enable True/False#True}}'
transformationPlugin = '{{TransformationPlugin#Transformation plugin name#LHCbDSTBroadcast}}'

###########################################
# Fixed and implied parameters 
###########################################

currentReqID = int('{{ID}}')
prodGroup = '{{pDsc}}'
#used in case of a test e.g. certification etc.
bkConfigName = '{{configName}}'
bkConfigVersion = '{{configVersion}}'
strippRunNumbers='{{inProductionID}}'

#Other parameters from the request page
strippDQFlag = '{{inDataQualityFlag}}' #UNCHECKED
strippTransFlag = eval(strippTransFlag)
strippBKPublishing = eval(strippBKPublishing)
mergeRemoveInputsFlag = eval(mergeRemoveInputsFlag)
transformationFlag = eval(transformationFlag)
testFlag = eval(testFlag)
testProduction = eval(testFlag)

inputDataDaVinci = []

#In case we want just to test
if testFlag or testProduction:
  bkConfigName = 'certification'
  bkConfigVersion = 'test'
if testFlag:  
  events = '5'
  inputDataDaVinci = ['/lhcb/data/2010/SDST/00008178/0000/00008178_00009199_1.sdst']

#The below are fixed for the FULL stream
strippType="ReSTRIPPING"
strippAppType = "DST"
strippIDPolicy='download'

#Sort out the stripping file mask
if strippFileMask:
  maskList = [m.lower() for m in strippFileMask.replace(' ','').split(',')]
  if not strippAppType.lower() in maskList:
    maskList.append(strippAppType.lower())
  strippFileMask = string.join(maskList,';')

BkFileType = '{{inFileType}}'
#For the stripping only, starting from the SDTS, there's the need to have the RAW also 
if BkFileType == 'SDST':
  BkFileType = ['SDST', 'RAW']

strippInputBKQuery = {  'SimulationConditions'     : 'All',
                        'DataTakingConditions'     : '{{simDesc}}',
                        'ProcessingPass'           : '{{inProPass}}',
                        'FileType'                 : BkFileType,
                        'EventType'                : '{{eventType}}',
                        'ConfigName'               : bkConfigName,
                        'ConfigVersion'            : bkConfigVersion,
                        'ProductionID'             : 0,
                        'DataQualityFlag'          : strippDQFlag
                      }


if int(strippEndRun) and int(strippStartRun):
  if int(strippEndRun)<int(strippStartRun):
    gLogger.error('Your end run "%s" should be less than your start run "%s"!' %(strippEndRun,strippStartRun))
    DIRAC.exit(2)

if int(strippStartRun):
  strippInputBKQuery['StartRun']=int(strippStartRun)
if int(strippEndRun):
  strippInputBKQuery['EndRun']=int(strippEndRun)

if re.search(',',strippRunNumbers) and not int(strippStartRun) and not int(strippEndRun):
  gLogger.info('Found run numbers to add to BK Query...')
  runNumbers = [int(i) for i in strippRunNumbers.replace(' ','').split(',')]
  strippInputBKQuery['RunNumbers']=runNumbers

#Have to see whether it's a one or two step request and react accordingly
mergingFlag = False

oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'
threeSteps = '{{p3App}}'

if threeSteps:
  gLogger.error('Three steps specified, not sure what to do! Exiting...')
  DIRAC.exit(2)    

if twoSteps:
  gLogger.info('Stripping production + merging is requested...')
  mergingFlag=True
else:
  gLogger.info('Stripping production without merging is requested...')

strippScriptFlag = False
if not strippBKPublishing:
  strippScriptFlag = True

diracProd = DiracProduction() #used to set automatic status

#################################################################################
# Create the stripping production
#################################################################################

production = Production()

if not destination.lower() in ('all','any'):
  gLogger.info('Forcing destination site %s for production' %(destination))
  production.setDestination(destination)

if sysConfig:
  production.setSystemConfig(sysConfig)

production.setCPUTime(strippCPU)
production.setProdType('DataStripping')
wkfName='Request%s_{{pDsc}}_{{eventType}}' %(currentReqID) #Rest can be taken from the details in the monitoring
production.setWorkflowName('%s_%s_%s' %(strippType,wkfName,appendName))
production.setWorkflowDescription("%s Real data FULL stripping production." %(prodGroup))
production.setBKParameters(bkConfigName,bkConfigVersion,prodGroup,'{{simDesc}}')
production.setInputBKSelection(strippInputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

dvOptions="{{p1Opt}}"
production.addDaVinciStep("{{p1Ver}}",
                          "dst",
                          dvOptions,
                          extraPackages='{{p1EP}}',
                          inputDataType=strippAppType.lower(),
                          inputData=inputDataDaVinci,
                          numberOfEvents=events, 
                          dataType='Data',
                          outputSE=unmergedStreamSE,
                          histograms=True
                          )

production.addFinalizationStep()
production.setInputBKSelection(strippInputBKQuery)
production.setProdGroup(prodGroup)
production.setFileMask(strippFileMask)
production.setProdPriority(strippPriority)
production.setProdPlugin(strippPlugin)
production.setInputDataPolicy(strippIDPolicy)

if testFlag:
  gLogger.info('Production test will be launched with number of events set to %s.' %(events))
  try:
    result = production.runLocal()
    if result['OK']:
      DIRAC.exit(0)
    else:
      DIRAC.exit(2)
  except Exception,x:
    gLogger.error('Production test failed with exception:\n%s' %(x))
    DIRAC.exit(2)

result = production.create(
                           bkQuery=strippInputBKQuery,
                           groupSize=strippFilesPerJob,
                           derivedProduction=int(strippAncestorProd),
                           bkScript=strippScriptFlag,
                           requestID=currentReqID,
                           reqUsed=1,
                           transformation=strippTransFlag
                           )
if not result['OK']:
  gLogger.error('Production creation failed with result:\n%s\ntemplate is exiting...' %(result))
  DIRAC.exit(2)

strippProdID = result['Value']
diracProd.production(strippProdID,'automatic',printOutput=True)
gLogger.info('Reconstruction production successfully created with ID %s and started in automatic mode' %(strippProdID))

#################################################################################
# Create the merging productions if there are enough workflow steps
#################################################################################

if not mergingFlag:
  DIRAC.exit(0)

##################################################################################
### TEMPORARY HACK SINCE THERE IS NO REASONABLE WAY TO GET THE LIST OF STREAMS ###
##################################################################################

streamsList = ['CHARMCONTROL.DST', 'CHARMFULL.DST']

replicateList =  ['CHARMCONTROL.DST']

onlyOneOtherSite = ['CHARMFULL.DST'] # do outside of this template
onlyCERN = ['CALIBRATION.DST'] # Nothing special to be done for distribution

dstList = streamsList # call it dstList just to accommodate future hacks

##################################################################################


###########################################
# Now remove the banned streams
###########################################

if not dstList: # or not setcList:
  gLogger.error('Could not find any file types to merge! Exiting...')
  DIRAC.exit(2)

gLogger.info('List of DST file types is: %s' %(string.join(dstList,', ')))

###########################################
# Some parameters
###########################################

#below should be integrated in the ProductionOptions utility
dvExtraOptions = "from Configurables import RecordStream;"
dvExtraOptions+= "FileRecords = RecordStream(\"FileRecords\");"
dvExtraOptions+= "FileRecords.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'REC\'\""

mergeGroupSize = 5
mergeTransFlag = False

mergeOpts='{{p3Opt}}'
taggingOpts='{{p4Opt}}'

#very important! must add 'Merging' or whatever user decides to the processing pass from the initial step
if not re.search('^-',mergeProdGroup):
  mergeProdGroup = '-%s' %(mergeProdGroup)

prodGroup += mergeProdGroup

###########################################
# Start the productions for each file type
###########################################

productionList = []

for mergeStream in dstList:
  mergeSE = 'Tier1_M-DST'
  if mergeStream.lower() in onlyCERN:
    mergeSE = 'CERN_M-DST'
  
  mergeBKQuery = { 'ProductionID'             : strippProdID,
                   'DataQualityFlag'          : mergeDQFlag,
                   'FileType'                 : mergeStream}

  mergeProd = Production()
  mergeProd.setCPUTime(mergeCPU)
  mergeProd.setProdType('Merge')
  wkfName='Merging_Request%s_{{pDsc}}_{{eventType}}' %(currentReqID)
  mergeProd.setWorkflowName('%s_%s_%s' %(mergeStream.split('.')[0],wkfName,appendName))

  if sysConfig:
    mergeProd.setSystemConfig(sysConfig)
  
  mergeProd.setWorkflowDescription('Steam merging workflow for %s files from input production %s' %(mergeStream,strippProdID))
  mergeProd.setBKParameters(bkConfigName,bkConfigVersion,prodGroup,'{{simDesc}}')
  mergeProd.setDBTags('{{p1CDb}}','{{p1DDDb}}')
  
  mergeProd.addDaVinciStep('{{p3Ver}}','merge',mergeOpts,extraPackages='{{p3EP}}',eventType='{{eventType}}',
                           inputDataType=mergeStream.lower(),extraOpts=dvExtraOptions,
                           inputProduction=strippProdID,inputData=[],outputSE=mergeSE)

# N.B. Now we remove the tagging step.
#  mergeProd.addDaVinciStep('{{p4Ver}}','setc',taggingOpts,extraPackages='{{p4EP}}',inputDataType=mergeStream.lower())
  
  mergeProd.addFinalizationStep(removeInputData=mergeRemoveInputsFlag)
  mergeProd.setInputBKSelection(mergeBKQuery)
  mergeProd.setInputDataPolicy(strippIDPolicy)
  mergeProd.setProdGroup(prodGroup)
  mergeProd.setProdPriority(mergePriority)
  mergeProd.setJobFileGroupSize(mergeGroupSize)
#  mergeProd.setFileMask('setc;%s' %(mergeStream.lower()))
  # NO NEED, credo
  #mergeProd.setFileMask(mergeStream.lower())
  mergeProd.setProdPlugin(mergePlugin)

  result = mergeProd.create(bkScript=False,requestID=currentReqID,reqUsed=1,transformation=mergeTransFlag,bkProcPassPrepend='{{inProPass}}')
  if not result['OK']:
    gLogger.error('Production creation failed with result:\n%s\ntemplate is exiting...' %(result))
    DIRAC.exit(2)
    
  mergeID = result['Value']
  diracProd.production(mergeID,'automatic',printOutput=True)
  gLogger.info('Merging production %s for %s successfully created and started in automatic mode.' %(mergeID,mergeStream))
  productionList.append(int(mergeID)) 

#################################################################################
# Create the transformations explicitly since we need to propagate the types
#################################################################################

if not transformationFlag:
  gLogger.info('Transformation flag is False, exiting prior to creating transformations.')
  gLogger.info('Template finished successfully.')
  DIRAC.exit(0)

transDict = {'DST':replicateList} # We used to also distribute the SETC files as well

for streamType,streamList in transDict.items():
  transBKQuery = {  'ProductionID'           : productionList,
                    'FileType'               : streamList}
  
  transformation=Transformation()
  transName='STREAM_Replication_%s_Request%s_{{pDsc}}_{{eventType}}_%s' %(streamType,currentReqID,appendName)
  transformation.setTransformationName(transName)
  transformation.setTransformationGroup(prodGroup)
  transformation.setDescription('Replication of streamed %s from {{pDsc}}' %(streamType))
  transformation.setLongDescription('Replication of streamed %s from {{pDsc}} to all Tier1s' %(streamType))
  transformation.setType('Replication')
  transformation.setPlugin(transformationPlugin)  
  transformation.setBkQuery(transBKQuery)
  transformation.addTransformation()
  transformation.setStatus('Active')
  transformation.setAgentType('Automatic')
  transformation.setTransformationFamily(currentReqID)
  result = transformation.getTransformationID()
  if not result['OK']:
    gLogger.error('Problem during transformation creation with result:\n%s\nExiting...' %(result))
    DIRAC.exit(2)
    
  gLogger.info('Transformation creation result: %s' %(result))

gLogger.info('Template finished successfully.')
DIRAC.exit(0)

#################################################################################
# End of the template.
#################################################################################

