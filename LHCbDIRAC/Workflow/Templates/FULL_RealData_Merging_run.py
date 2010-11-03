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

__RCSID__ = "$Id: FULL_RealData_Merging_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
import sys,os,string,re
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gConfig, gLogger
gLogger = gLogger.getSubLogger('FULL_RealData_Merging_run.py')

#################################################################################
# Below here is the production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.ProductionManagementSystem.Client.Transformation import Transformation

###########################################
# Configurable parameters
###########################################

# workflow params for all productions
appendName = '{{WorkflowAppendName#Workflow string to append to production name#1}}'
sysConfig = '{{WorkflowSystemConfig#Workflow system config e.g. slc4_ia32_gcc34#x86_64-slc5-gcc43-opt}}'
destination = '{{WorkflowDestination#Workflow destination site e.g. LCG.CERN.ch#ALL}}'
testProduction = '{{WorkflowTestFlag#Workflow testing flag e.g. for certification True/False#False}}'

#reco params
recoPriority = '{{RecoPriority#Reconstruction production priority#7}}'
recoCPU = '{{RecoMaxCPUTime#Reconstruction Max CPU time in secs#1000000}}'
recoPlugin = '{{RecoPluginType#Reconstruction production plugin name#AtomicRun}}'
recoAncestorProd = '{{RecoAncestorProd#Reconstruction ancestor production if any#0}}'
recoDataSE = '{{RecoDataSE#Reconstruction Output Data Storage Element#Tier1-RDST}}'
recoFilesPerJob = '{{RecoFilesPerJob#Reconstruction Group size or number of files per job#1}}'
recoFileMask = '{{RecoOutputDataFileMask#Reconstruction file extns to save (comma separated)#DST,ROOT}}'
recoTransFlag = '{{RecoTransformation#Reconstruction distribute output data True/False (False if merging)#False}}'
recoBKPublishing = '{{BKPublishFlag#Reconstruction publish this to the BK#True}}'
recoStartRun = '{{RecoRunStart#Reconstruction run start, to set the start run#0}}'
recoEndRun = '{{RecoRunEnd#Reconstruction run end, to set the end of the range#0}}'
recoEvtsPerJob = '{{RecoNumberEvents#Reconstruction number of events per job (set to something small for a test)#-1}}'
unmergedStreamSE = '{{RecoStreamSE#Reconstruction unmerged stream SE#Tier1-DST}}'
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
recoRunNumbers='{{inProductionID}}'

#Other parameters from the request page
recoDQFlag = '{{inDataQualityFlag}}' #UNCHECKED
recoTransFlag = eval(recoTransFlag)
recoBKPublishing = eval(recoBKPublishing)
mergeRemoveInputsFlag = eval(mergeRemoveInputsFlag)
transformationFlag = eval(transformationFlag)
testProduction = eval(testProduction)

if testProduction:
  bkConfigName = 'certification'
  bkConfigVersion = 'test'

#The below are fixed for the FULL stream
recoType="FULL"
recoAppType = "SDST"
recoIDPolicy='download'

#Sort out the reco file mask
if recoFileMask:
  maskList = [m.lower() for m in recoFileMask.replace(' ','').split(',')]
  if not recoAppType.lower() in maskList:
    maskList.append(recoAppType.lower())
  recoFileMask = string.join(maskList,';')

recoInputBKQuery = { 'SimulationConditions'     : 'All',
                 'DataTakingConditions'     : '{{simDesc}}',
                 'ProcessingPass'           : '{{inProPass}}',
                 'FileType'                 : '{{inFileType}}',
                 'EventType'                : '{{eventType}}',
                 'ConfigName'               : '{{configName}}',
                 'ConfigVersion'            : '{{configVersion}}',
                 'ProductionID'             : 0,
                 'DataQualityFlag'          : recoDQFlag}


if int(recoEndRun) and int(recoStartRun):
  if int(recoEndRun)<int(recoStartRun):
    gLogger.error('Your end run "%s" should be less than your start run "%s"!' %(recoEndRun,recoStartRun))
    DIRAC.exit(2)

if int(recoStartRun):
  recoInputBKQuery['StartRun']=int(recoStartRun)
if int(recoEndRun):
  recoInputBKQuery['EndRun']=int(recoEndRun)

if re.search(',',recoRunNumbers) and not int(recoStartRun) and not int(recoEndRun):
  gLogger.info('Found run numbers to add to BK Query...')
  runNumbers = [int(i) for i in recoRunNumbers.replace(' ','').split(',')]
  recoInputBKQuery['RunNumbers']=runNumbers

#Have to see whether it's a two or four step request and react accordingly
mergingFlag = False

oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'
threeSteps = '{{p3App}}'
fourSteps = '{{p4App}}'
fiveSteps = '{{p5App}}'

if fiveSteps:
  gLogger.error('Five steps specified, not sure what to do! Exiting...')
  DIRAC.exit(2)    

if threeSteps and not fourSteps:
  gLogger.error('Three steps specified (only), not sure what to do! Exiting...')
  DIRAC.exit(2)

if fourSteps:
  gLogger.info('Reconstruction production + merging is requested...')
  mergingFlag=True
else:
  gLogger.info('Reconstruction production without merging is requested...')

recoScriptFlag = False
if not recoBKPublishing:
  recoScriptFlag = True

diracProd = DiracProduction() #used to set automatic status

#################################################################################
# Create the reconstruction production
#################################################################################

production = Production()

if not destination.lower() in ('all','any'):
  gLogger.info('Forcing destination site %s for production' %(destination))
  production.setDestination(destination)

if sysConfig:
  production.setSystemConfig(sysConfig)

production.setCPUTime(recoCPU)
production.setProdType('DataReconstruction')
wkfName='Request%s_{{pDsc}}_{{eventType}}' %(currentReqID) #Rest can be taken from the details in the monitoring
production.setWorkflowName('%s_%s_%s' %(recoType,wkfName,appendName))
production.setWorkflowDescription("%s Real data FULL reconstruction production." %(prodGroup))
production.setBKParameters(bkConfigName,bkConfigVersion,prodGroup,'{{simDesc}}')
production.setInputBKSelection(recoInputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

brunelOptions="{{p1Opt}}"
production.addBrunelStep("{{p1Ver}}",recoAppType.lower(),brunelOptions,extraPackages='{{p1EP}}',
                         eventType='{{eventType}}',inputData=[],inputDataType='mdf',outputSE=recoDataSE,
                         dataType='Data',numberOfEvents=recoEvtsPerJob,histograms=True)
dvOptions="{{p2Opt}}"
production.addDaVinciStep("{{p2Ver}}","dst",dvOptions,extraPackages='{{p2EP}}',inputDataType=recoAppType.lower(),
                          dataType='Data',outputSE=unmergedStreamSE,histograms=True)

production.addFinalizationStep()
production.setInputBKSelection(recoInputBKQuery)
production.setProdGroup(prodGroup)
production.setFileMask(recoFileMask)
production.setProdPriority(recoPriority)
production.setProdPlugin(recoPlugin)
production.setInputDataPolicy(recoIDPolicy)

result = production.create(bkQuery=recoInputBKQuery,groupSize=recoFilesPerJob,derivedProduction=int(recoAncestorProd),
                           bkScript=recoScriptFlag,requestID=currentReqID,reqUsed=1,transformation=recoTransFlag)
if not result['OK']:
  gLogger.error('Production creation failed with result:\n%s\ntemplate is exiting...' %(result))
  DIRAC.exit(2)

recoProdID = result['Value']
diracProd.production(recoProdID,'automatic',printOutput=True)
gLogger.info('Reconstruction production successfully created with ID %s and started in automatic mode' %(recoProdID))

#################################################################################
# Create the merging productions if there are enough workflow steps
#################################################################################

if not mergingFlag:
  DIRAC.exit(0)

bkFileTypes = gConfig.getValue('/Operations/Bookkeeping/FileTypes',[])
bannedStreams = gConfig.getValue('/Operations/Bookkeeping/BannedStreams',[])

##################################################################################
### TEMPORARY HACK SINCE THERE IS NO REASONABLE WAY TO GET THE LIST OF STREAMS ###
##################################################################################

bannedStreams.append('HADRON.DST')
bannedStreams.append('HADRONIC.DST')
bannedStreams.append('DIMUONDIPHOTON.DST')
bannedStreams.append('VO.DST')
bannedStreams.append('V0.DST') #Just in case
#bannedStreams.append('CHARMCONTROL.DST')
bannedStreams.append('CHARM.DST')  
bannedStreams.append('DIPHOTONDIMUON.DST')

for okStream in ['BHADRON.DST','CHARM.DST']:
  if okStream in bannedStreams:
    bannedStreams.remove(okStream)

dstList = [] 
#dstList.append('CHARMMICRODST.MDST')
#dstList.append('LEPTONICMICRODST.MDST')
dstList.append('CHARM.MDST')
dstList.append('LEPTONIC.MDST')

##################################################################################


if not bkFileTypes:
  gLogger.error('Could not contact CS to get BK File Types list! Exiting...')
  DIRAC.exit(2)
  
if not bannedStreams:
  gLogger.error('List of banned streams is null unexpectedly. Exiting...')
  DIRAC.exit(2)

###########################################
# Now remove the banned streams
###########################################

setcList = []
#restrict to '.DST' file types:
for fType in bkFileTypes:
  if re.search('\.DST$',fType) and not fType in bannedStreams:
    dstList.append(fType)
  if re.search('\.SETC$',fType) and not fType in bannedStreams:
    setcList.append(fType)

if not dstList or not setcList:
  gLogger.error('Could not find any file types to merge! Exiting...')
  DIRAC.exit(2)

#Until issue of naming streamed SETC files is resolved
setcList.append('SETC')

gLogger.info('List of DST file types from BK is: %s' %(string.join(dstList,', ')))
gLogger.info('List of SETC file types from BK is: %s' %(string.join(setcList,', ')))

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
  mergeBKQuery = { 'ProductionID'             : recoProdID,
                   'DataQualityFlag'          : mergeDQFlag,
                   'FileType'                 : mergeStream}

  mergeProd = Production()
  mergeProd.setCPUTime(mergeCPU)
  mergeProd.setProdType('Merge')
  wkfName='Merging_Request%s_{{pDsc}}_{{eventType}}' %(currentReqID)
  mergeProd.setWorkflowName('%s_%s_%s' %(mergeStream.split('.')[0],wkfName,appendName))

  if sysConfig:
    mergeProd.setSystemConfig(sysConfig)
  
  mergeProd.setWorkflowDescription('Steam merging workflow for %s files from input production %s' %(mergeStream,recoProdID))
  mergeProd.setBKParameters(bkConfigName,bkConfigVersion,prodGroup,'{{simDesc}}')
  mergeProd.setDBTags('{{p1CDb}}','{{p1DDDb}}')
  
  mergeProd.addDaVinciStep('{{p3Ver}}','merge',mergeOpts,extraPackages='{{p3EP}}',eventType='{{eventType}}',
                           inputDataType=mergeStream.lower(),extraOpts=dvExtraOptions,
                           inputProduction=recoProdID,inputData=[])
  mergeProd.addDaVinciStep('{{p4Ver}}','setc',taggingOpts,extraPackages='{{p4EP}}',inputDataType=mergeStream.lower())
  
  mergeProd.addFinalizationStep(removeInputData=mergeRemoveInputsFlag)
  mergeProd.setInputBKSelection(mergeBKQuery)
  mergeProd.setInputDataPolicy(recoIDPolicy)
  mergeProd.setProdGroup(prodGroup)
  mergeProd.setProdPriority(mergePriority)
  mergeProd.setJobFileGroupSize(mergeGroupSize)
  mergeProd.setFileMask('setc;%s' %(mergeStream.lower()))
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

transDict = {'DST':dstList,'SETC':setcList}

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