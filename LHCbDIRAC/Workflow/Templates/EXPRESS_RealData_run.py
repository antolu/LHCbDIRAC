########################################################################
# $Header: /local/reps/dirac/DIRAC3/LHCbSystem/WorkflowTemplates/FESTRecoTemplate_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $
########################################################################

"""  The EXPRESS Real Data Reco template creates a workflow for Brunel & DaVinci with
     configurable number of events, CPU time, jobs to extend and priority.
     
     As this template has evolved it started to be used for "special" reconstructions
     in addition to the standard express stream processing. For studies that do
     not require execution or distribution outside of CERN this template is very 
     suitable.
"""

__RCSID__ = "$Id: FESTRecoTemplate_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
import sys,os,string,re
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gConfig, gLogger
gLogger = gLogger.getSubLogger('EXPRESS_RealData_run.py')

#################################################################################
# Below here is the production API script with notes
#################################################################################

from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

###########################################
# Configurable parameters
###########################################

# workflow params for all productions
appendName = '{{WorkflowAppendName#Workflow string to append to production name#1}}'
sysConfig = '{{WorkflowSystemConfig#Workflow system config e.g. slc4_ia32_gcc34#x86_64-slc5-gcc43-opt}}'
destination = '{{WorkflowDestination#Workflow destination site e.g. LCG.CERN.ch for the EXPRESS#LCG.CERN.ch}}'
testProduction = '{{WorkflowTestFlag#Workflow testing flag e.g. for certification True/False#False}}'
# particular for the EXPRESS template
prependName = '{{WorkflowPrependName#String to prepend to production name#EXPRESS}}'

#reco params
recoPriority = '{{RecoPriority#Reconstruction production priority#9}}'
recoCPU = '{{RecoMaxCPUTime#Reconstruction Max CPU time in secs#100000}}'
recoPlugin = '{{RecoPluginType#Reconstruction production plugin name#ByRun}}'
recoAncestorProd = '{{RecoAncestorProd#Reconstruction ancestor production if any#0}}'
recoDataSE = '{{RecoDataSE#Reconstruction Output Data Storage Element#CERN_M-DST}}'
recoFilesPerJob = '{{RecoFilesPerJob#Reconstruction Group size or number of files per job#1}}'
recoFileMask = '{{RecoOutputDataFileMask#Reconstruction file extns to save (comma separated)#DST,ROOT}}'
recoTransFlag = '{{RecoTransformation#Reconstruction distribute output data True/False (False for EXPRESS)#False}}'
recoBKPublishing = '{{BKPublishFlag#Reconstruction publish this to the BK#True}}'
recoStartRun = '{{RecoRunStart#Reconstruction run start, to set the start run#0}}'
recoEndRun = '{{RecoRunEnd#Reconstruction run end, to set the end of the range#0}}'

###########################################
# Fixed and implied parameters 
###########################################

currentReqID = int('{{ID}}')
prodGroup = '{{pDsc}}'
bkConfigName = '{{configName}}'
bkConfigVersion = '{{configVersion}}'

recoDQFlag = '{{inDataQualityFlag}}' #UNCHECKED
#dqFlag = '{{inDataQualityFlag}}' #UNCHECKED
recoRunNumbers='{{inProductionID}}'

recoTransFlag = eval(recoTransFlag)
testProduction = eval(testProduction)

if testProduction:
  bkConfigName = 'certification'
  bkConfigVersion = 'test'

#The below are fixed for the EXPRESS stream
recoType="FULL"
recoAppType = "DST"
#appType = "DST"
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

#Have to confirm this isn't a FULL request for example
threeSteps = '{{p3App}}'
if threeSteps:
  gLogger.error('Three steps specified, not sure what to do! Exiting...')
  DIRAC.exit(2)

#For the FULL case there's an option to avoid BK publishing, not here
recoScriptFlag = False

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
production.setWorkflowName('%s_%s_%s' %(prependName,wkfName,appendName))
production.setWorkflowDescription("%s Real data EXPRESS reconstruction production." %(prodGroup))
production.setBKParameters(bkConfigName,bkConfigVersion,prodGroup,'{{simDesc}}')
production.setInputBKSelection(recoInputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

brunelOptions="{{p1Opt}}"
production.addBrunelStep("{{p1Ver}}",recoAppType.lower(),brunelOptions,extraPackages='{{p1EP}}',
                         eventType='{{eventType}}',inputData=[],inputDataType='mdf',outputSE=recoDataSE,
                         dataType='Data',histograms=True)

#Since this template is also used for "special" processings only add DaVinci step if defined
if "{{p2Ver}}":
  production.addDaVinciStep("{{p2Ver}}","davincihist","{{p2Opt}}",extraPackages='{{p2EP}}',
                            inputDataType=appType.lower(),histograms=True,abandonOutput=True)

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
DIRAC.exit(0)

#################################################################################
# End of the template.
#################################################################################