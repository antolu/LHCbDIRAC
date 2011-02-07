########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Workflow/Templates/MicroDST_StreamedData_run.py $
########################################################################

"""  The MicroDST template for streamed data.
"""

__RCSID__ = "$Id$"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
import sys,os,string,re
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gConfig, gLogger
gLogger = gLogger.getSubLogger('MicroDST_StreamedData_run.py')

#################################################################################
# Below here is the production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

###########################################
# Configurable parameters
###########################################

testFlag = '{{TemplateTest#A flag to set whether a test script should be generated#False}}'

# workflow params for all productions
appendName = '{{WorkflowAppendName#Workflow string to append to production name#1}}'
sysConfig = '{{WorkflowSystemConfig#Workflow system config e.g. slc4_ia32_gcc34#x86_64-slc5-gcc43-opt}}'
destination = '{{WorkflowDestination#Workflow destination site e.g. LCG.CERN.ch#ALL}}'
testProduction = '{{WorkflowTestFlag#Workflow testing flag e.g. for certification True/False#False}}'

#mdst params
mdstPriority = '{{MicroDSTPriority#MicroDST production priority#7}}'
mdstCPU = '{{MicroDSTMaxCPUTime#MicroDST Max CPU time in secs#1000000}}'
mdstPlugin = '{{MicroDSTPluginType#MicroDST production plugin name#Standard}}'
mdstAncestorProd = '{{MicroDSTAncestorProd#MicroDST ancestor production if any#0}}'
mdstDataSE = '{{MicroDSTDataSE#MicroDST Output Data Storage Element#Tier1_M-DST}}'
mdstFilesPerJob = '{{MicroDSTFilesPerJob#MicroDST Group size or number of files per job#10}}'
mdstFileMask = '{{MicroDSTOutputDataFileMask#MicroDST file extns to save (comma separated)#DST,ROOT}}'
mdstTransFlag = '{{MicroDSTTransformation#MicroDST distribute output data True/False (False if merging)#False}}'
mdstBKPublishing = '{{BKPublishFlag#MicroDST publish this to the BK#True}}'
mdstStartRun = '{{MicroDSTRunStart#MicroDST run start, to set the start run#0}}'
mdstEndRun = '{{MicroDSTRunEnd#MicroDST run end, to set the end of the range#0}}'
mdstIDPolicy = '{{MicroDSTIDPolicy#MicroDST Input data policy e.g. download, protocol#protocol}}'

###########################################
# Fixed and implied parameters 
###########################################

currentReqID = int('{{ID}}')
prodGroup = '{{pDsc}}'
#reset in case of a test e.g. certification etc.
bkConfigName = '{{configName}}'
bkConfigVersion = '{{configVersion}}'

#Other parameters from the request page
mdstDQFlag = '{{inDataQualityFlag}}' #UNCHECKED
mdstTransFlag = eval(mdstTransFlag)
mdstBKPublishing = eval(mdstBKPublishing)
testProduction = eval(testProduction)
testFlag = eval(testFlag)

if testProduction:
  bkConfigName = 'certification'
  bkConfigVersion = 'test'

inputDataDaVinci = []

events = '-1'
if testFlag:
  events = '5'
  inputDataDaVinci = ['/lhcb/data/2010/DIMUON.DST/00007544/0000/00007544_00000179_1.dimuon.dst']

#The below are fixed
mdstType="MicroDST"
mdstAppType = "MDST"

#Sort out the mdst file mask
if mdstFileMask:
  maskList = [m.lower() for m in mdstFileMask.replace(' ','').split(',')]
  if not mdstAppType.lower() in maskList:
    maskList.append(mdstAppType.lower())
  mdstFileMask = string.join(maskList,';')

mdstInputBKQuery = { 'SimulationConditions'     : 'All',
                 'DataTakingConditions'     : '{{simDesc}}',
                 'ProcessingPass'           : '{{inProPass}}',
                 'FileType'                 : '{{inFileType}}',
                 'EventType'                : '{{eventType}}',
                 'ConfigName'               : '{{configName}}',
                 'ConfigVersion'            : '{{configVersion}}',
                 'ProductionID'             : 0,
                 'DataQualityFlag'          : mdstDQFlag}


if int(mdstEndRun) and int(mdstStartRun):
  if int(mdstEndRun)<int(mdstStartRun):
    gLogger.error('Your end run "%s" should be less than your start run "%s"!' %(mdstEndRun,mdstStartRun))
    DIRAC.exit(2)

if int(mdstStartRun):
  mdstInputBKQuery['StartRun']=int(mdstStartRun)
if int(mdstEndRun):
  mdstInputBKQuery['EndRun']=int(mdstEndRun)

#Have to see whether it's a two or four step request and react accordingly
mergingFlag = False

oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'

if twoSteps:
  gLogger.error('More than one step specified, not sure what to do! Exiting...')
  DIRAC.exit(2)

mdstScriptFlag = False
if not mdstBKPublishing:
  mdstScriptFlag = True

diracProd = DiracProduction() #used to set automatic status

#################################################################################
# Create the MicroDST production
#################################################################################

production = Production()

if not destination.lower()=='all':
  gLogger.info('Forcing destination site %s for production' %(destination))
  production.setDestination(destination)

if sysConfig:
  production.setSystemConfig(sysConfig)

production.setCPUTime(mdstCPU)
production.setProdType('DataStripping')
wkfName='Request%s_{{pDsc}}_{{eventType}}' %(currentReqID) #Rest can be taken from the details in the monitoring
production.setWorkflowName('%s_%s_%s' %(mdstType,wkfName,appendName))
production.setWorkflowDescription("%s streamed data MicroDST production." %(prodGroup))
production.setBKParameters(bkConfigName,bkConfigVersion,prodGroup,'{{simDesc}}')
production.setInputBKSelection(mdstInputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

dvOptions="{{p1Opt}}"

production.addDaVinciStep("{{p1Ver}}",mdstAppType.lower(),dvOptions,extraPackages='{{p1EP}}',eventType='{{eventType}}',
                          inputData=inputDataDaVinci,inputDataType='dst',outputSE=mdstDataSE,numberOfEvents=events,
                          dataType='Data',histograms=False,stepID='{{p1Step}}',stepName='{{p1Name}}',stepVisible='{{p1Vis}}')

production.addFinalizationStep()
production.setInputBKSelection(mdstInputBKQuery)
production.setProdGroup(prodGroup)
production.setFileMask(mdstFileMask)
production.setProdPriority(mdstPriority)
production.setProdPlugin(mdstPlugin)
production.setInputDataPolicy(mdstIDPolicy)

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

result = production.create(bkQuery=mdstInputBKQuery,groupSize=mdstFilesPerJob,derivedProduction=int(mdstAncestorProd),
                           bkScript=mdstScriptFlag,requestID=currentReqID,reqUsed=1,transformation=mdstTransFlag)

if not result['OK']:
  gLogger.error('Production creation failed with result:\n%s\ntemplate is exiting...' %(result))
  DIRAC.exit(2)

mdstProdID = result['Value']
diracProd.production(mdstProdID,'automatic',printOutput=True)
gLogger.info('MicroDST production successfully created with ID %s and started in automatic mode' %(mdstProdID))

#################################################################################
# End of the template.
#################################################################################