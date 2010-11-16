########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Workflow/Templates/MC_Simulation_run.py $
########################################################################

"""  The MC Simulation Template creates workflows for the following simulation
     use-cases:
     
     - Gauss [ + Merging, + Transformation ]
     - Gauss -> Boole [ + Merging, + Transformation ]
     - Gauss -> Boole -> Brunel [ + Merging, + Transformation ]
     - Gauss -> Boole -> Moore -> Brunel [ + Merging, + Transformation ]
     
     with the following parameters being configurable via the interface:
     
     - number of events
     - CPU time in seconds
     - final output file type 
     - number of jobs to extend the initial MC production 
     - resulting WMS priority for each job
     - BK config name and version
     - system configuration
     - output file mask (e.g. to retain intermediate step output)
     - merging priority, plugin and group size
     - whether or not to ban Tier-1 sites as destination
     - whether or not a merging production should be created
     - whether or not a transformation should be created
     - whether outputs should be uploaded to CERN only for testing
     - string to append to the production name
     
     The template explicitly forces the tags of the CondDB / DDDB to be set
     at each step.  
     
"""

__RCSID__   = "$Id: MC_Simulation_run.py 23248 2010-03-18 07:57:40Z paterson $"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
import sys,os,string
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gLogger
gLogger = gLogger.getSubLogger('MC_Simulation_run.py')

#################################################################################
# Below here is the actual production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

###########################################
# Configurable and fixed parameters
###########################################
testFlag = '{{TemplateTest#A flag to set whether a test script should be generated#False}}'

events = '{{MCNumberOfEvents#MC Number of events per job#1000}}'
cpu = '{{MCMaxCPUTime#MC Max CPU time in secs#1000000}}'
priority = '{{MCPriority#MC Production priority#4}}'
extend = '{{MCExtend#MC extend production by this many jobs#100}}'
finalAppType = '{{MCFinalAppType#MC final file type to produce and merge e.g. DST,XDST,GEN,SIM...#DST}}'

configName = '{{BKConfigName#BK configuration name e.g. MC #MC}}'
configVersion = '{{BKConfigVersion#BK configuration version e.g. MC09, 2009, 2010#2010}}'

appendName = '{{WorkflowAppendName#Workflow string to append to production name#1}}'
outputFileMask = '{{WorkflowOutputDataFileMask#Workflow file extensions to save (comma separated) e.g. DST,DIGI#DST}}'
outputsCERN = '{{WorkflowCERNOutputs#Workflow upload workflow output to CERN#False}}'
sysConfig = '{{WorkflowSystemConfig#Workflow system config e.g. x86_64-slc5-gcc43-opt#ANY}}'
banTier1s = '{{WorkflowBanTier1s#Workflow ban Tier-1 sites for jobs Boolean True/False#True}}'

mergingFlag = '{{MergingEnable#Merging enable flag Boolean True/False#True}}' #True/False
mergingPlugin = '{{MergingPlugin#Merging plugin e.g. Standard, BySize#BySize}}'
mergingGroupSize = '{{MergingGroupSize#Merging Group Size e.g. BySize = GB file size#5}}'
mergingPriority = '{{MergingPriority#Merging Job Priority e.g. 8 by default#8}}'

transformationFlag = '{{TransformationEnable#Transformation enable flag Boolean True/False#True}}'

evtType = '{{eventType}}'
#Often MC requests are defined with many subrequests but we want to retain
#the parent ID for viewing on the production monitoring page. If a parent
#request is defined then this is used.
requestID = '{{ID}}'
parentReq = '{{_parent}}'
eventNumberTotal = '{{EventNumberTotal}}'

###########################################
# LHCb conventions implied by the above
###########################################

if finalAppType.lower()=='xdst':
  booleType = 'xdigi'
else:
  booleType = 'digi'

# This is SIM unless a one-step (+/- merging)
# Gauss production is requested, see below.
gaussAppType = 'sim'

testFlag = eval(testFlag)
transformationFlag = eval(transformationFlag)
mergingFlag = eval(mergingFlag)
banTier1s = eval(banTier1s)
outputsCERN=eval(outputsCERN)

#Use computing model default unless CERN is requested
defaultOutputSE='' # for all intermediate outputs
brunelDataSE=''
if outputsCERN:
  defaultOutputSE='CERN-RDST'
  brunelDataSE='CERN_MC_M-DST'

mcRequestTracking=1
mcTransformationFlag=transformationFlag
if mergingFlag:
  mcRequestTracking = 0
  mcTransformationFlag=False

if testFlag:
  events = '5'

#The below is in order to choose the right steps in the workflow automatically
#e.g. each number of steps maps to a unique number
mergingApp = 'LHCb'
mergingVersion = ''
mergingCondDB = ''
mergingDDDB = ''

oneStep = '{{p1App}}'
twoSteps = '{{p2App}}'
threeSteps = '{{p3App}}'
fourSteps = '{{p4App}}'
fiveSteps = '{{p5App}}'
sixSteps = '{{p6App}}'

if sixSteps:
  gLogger.error('Six steps specified, not sure what to do! Exiting...')
  DIRAC.exit(2)    

if outputFileMask:
  maskList = [m.lower() for m in outputFileMask.replace(' ','').split(',')]
  if not finalAppType.lower() in maskList:
    maskList.append(finalAppType.lower())
  outputFileMask = string.join(maskList,';')

###########################################
# Parameter passing, the bulk of the script
###########################################
production = Production()

if sysConfig:
  production.setSystemConfig(sysConfig)

production.setProdType('MCSimulation')
wkfName = 'Request{{ID}}_MC_{{simDesc}}_{{pDsc}}_EventType{{eventType}}_{{MCNumberOfEvents}}Events'

production.setWorkflowName('%s_%s' %(wkfName,appendName))
production.setBKParameters(configName,configVersion,'{{pDsc}}','{{simDesc}}')
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')
production.setSimulationEvents(events,eventNumberTotal)

decided=False

#To make editing the resulting script easier in all cases separate options from long function calls
gaussOpts = '{{p1Opt}}'
booleOpts = '{{p2Opt}}'
#Having Moore and Brunel at the third step means other Opts are defined later.

#Now try to guess what the request is actually asking for.
if fiveSteps:
  if not mergingFlag:
    gLogger.error('Five steps requested (without merging flag being set to True) not sure what to do! Exiting...')
    DIRAC.exit(2)
  if not fiveSteps.lower()==mergingApp.lower():
    gLogger.error('Five steps requested but last is not %s merging, not sure what to do! Exiting...' %(mergingApp))
    DIRAC.exit(2)
  
  prodDescription = 'A five step workflow Gauss->Boole->Moore->Brunel + Merging'
  production.addGaussStep('{{p1Ver}}','{{Generator}}',events,gaussOpts,eventType='{{eventType}}',
                          extraPackages='{{p1EP}}',condDBTag='{{p1CDb}}',ddDBTag='{{p1DDDb}}',
                          outputSE=defaultOutputSE,appType=gaussAppType)
  production.addBooleStep('{{p2Ver}}',booleType,booleOpts,extraPackages='{{p2EP}}',
                          condDBTag='{{p2CDb}}',ddDBTag='{{p2DDDb}}',outputSE=defaultOutputSE)
  mooreOpts = '{{p3Opt}}'
  production.addMooreStep('{{p3Ver}}',booleType,mooreOpts,extraPackages='{{p3EP}}',inputDataType=booleType,
                          condDBTag='{{p3CDb}}',ddDBTag='{{p3DDDb}}',outputSE=defaultOutputSE)
  brunelOpts = '{{p4Opt}}'
  production.addBrunelStep('{{p4Ver}}',finalAppType.lower(),brunelOpts,extraPackages='{{p4EP}}',inputDataType=booleType,
                           outputSE=brunelDataSE,condDBTag='{{p4CDb}}',ddDBTag='{{p4DDDb}}')  
  mergingVersion = '{{p5Ver}}'
  merginCondDB = '{{p5CDb}}'
  mergingDDDB = '{{p5DDDb}}'
  decided=True
  
if fourSteps and not decided:
  if not mergingFlag and threeSteps.lower()=='moore':
    prodDescription = 'A four step workflow Gauss->Boole->Moore-Brunel without merging' 
    production.addGaussStep('{{p1Ver}}','{{Generator}}',events,gaussOpts,eventType='{{eventType}}',
                            extraPackages='{{p1EP}}',condDBTag='{{p1CDb}}',ddDBTag='{{p1DDDb}}',
                            outputSE=defaultOutputSE,appType=gaussAppType)
    production.addBooleStep('{{p2Ver}}',booleType,booleOpts,extraPackages='{{p2EP}}',
                            condDBTag='{{p2CDb}}',ddDBTag='{{p2DDDb}}',outputSE=defaultOutputSE)
    mooreOpts = '{{p3Opt}}'
    production.addMooreStep('{{p3Ver}}',booleType,mooreOpts,extraPackages='{{p3EP}}',inputDataType=booleType,
                            condDBTag='{{p3CDb}}',ddDBTag='{{p3DDDb}}',outputSE=defaultOutputSE)
    brunelOpts = '{{p4Opt}}'
    production.addBrunelStep('{{p4Ver}}',finalAppType.lower(),brunelOpts,extraPackages='{{p4EP}}',inputDataType=booleType,
                             outputSE=brunelDataSE,condDBTag='{{p4CDb}}',ddDBTag='{{p4DDDb}}')  
    decided=True
  elif not mergingFlag and threeSteps.lower()=='brunel':
    prodDescription = 'A three step workflow Gauss->Boole->Brunel without merging'
    production.addGaussStep('{{p1Ver}}','{{Generator}}',events,gaussOpts,eventType='{{eventType}}',
                            extraPackages='{{p1EP}}',condDBTag='{{p1CDb}}',ddDBTag='{{p1DDDb}}',
                            outputSE=defaultOutputSE,appType=gaussAppType)
    production.addBooleStep('{{p2Ver}}',booleType,booleOpts,extraPackages='{{p2EP}}',
                            condDBTag='{{p2CDb}}',ddDBTag='{{p2DDDb}}',outputSE=defaultOutputSE)
    brunelOpts = '{{p3Opt}}'
    production.addBrunelStep('{{p3Ver}}',finalAppType.lower(),brunelOpts,extraPackages='{{p3EP}}',inputDataType=booleType,
                             outputSE=brunelDataSE,condDBTag='{{p3CDb}}',ddDBTag='{{p3DDDb}}')
    decided=True
  elif mergingFlag and fourSteps.lower()==mergingApp.lower():
    #Will likely disappear eventually as Moore will start to be used everywhere
    prodDescription = 'A four step workflow Gauss->Boole->Brunel + Merging'
    production.addGaussStep('{{p1Ver}}','{{Generator}}',events,gaussOpts,eventType='{{eventType}}',
                            extraPackages='{{p1EP}}',condDBTag='{{p1CDb}}',ddDBTag='{{p1DDDb}}',
                            outputSE=defaultOutputSE,appType=gaussAppType)
    production.addBooleStep('{{p2Ver}}',booleType,booleOpts,extraPackages='{{p2EP}}',
                            condDBTag='{{p2CDb}}',ddDBTag='{{p2DDDb}}',outputSE=defaultOutputSE)
    brunelOpts = '{{p3Opt}}'
    production.addBrunelStep('{{p3Ver}}',finalAppType.lower(),brunelOpts,extraPackages='{{p3EP}}',inputDataType=booleType,
                             outputSE=brunelDataSE,condDBTag='{{p3CDb}}',ddDBTag='{{p3DDDb}}')
    mergingVersion = '{{p4Ver}}'
    mergingCondDB = '{{p4CDb}}'
    mergingDDDB = '{{p4DDDb}}'
    decided=True 
  else:
    #So far ignoring possible Gauss->Boole->Moore + Merging combination (never requested).
    gLogger.error('Four steps requested but workflow is neither Gauss->Boole->Brunel +Merging, \
                   Gauss->Boole->Brunel without Merging, or Gauss->Boole->Moore->Brunel, not sure what to do! Exiting...')
    DIRAC.exit(2)

if threeSteps and not decided:
  if not mergingFlag and threeSteps.lower()=='brunel':
    prodDescription = 'A three step workflow of Gauss->Boole->Brunel without merging'
    production.addGaussStep('{{p1Ver}}','{{Generator}}',events,gaussOpts,eventType='{{eventType}}',
                            extraPackages='{{p1EP}}',condDBTag='{{p1CDb}}',ddDBTag='{{p1DDDb}}',
                            outputSE=defaultOutputSE,appType=gaussAppType)
    production.addBooleStep('{{p2Ver}}',booleType,booleOpts,extraPackages='{{p2EP}}',
                            condDBTag='{{p2CDb}}',ddDBTag='{{p2DDDb}}',outputSE=defaultOutputSE)
    brunelOpts = '{{p3Opt}}'
    production.addBrunelStep('{{p3Ver}}',finalAppType.lower(),brunelOpts,extraPackages='{{p3EP}}',inputDataType=booleType,
                             outputSE=brunelDataSE,condDBTag='{{p3CDb}}',ddDBTag='{{p3DDDb}}')    
    decided=True
  elif not mergingFlag and threeSteps.lower()==mergingApp.lower() and finalAppType.lower() in ['digi','xdigi']:
    prodDescription = 'A two step workflow of Gauss->Boole without merging' 
    production.addGaussStep('{{p1Ver}}','{{Generator}}',events,gaussOpts,eventType='{{eventType}}',
                            extraPackages='{{p1EP}}',condDBTag='{{p1CDb}}',ddDBTag='{{p1DDDb}}',
                            outputSE=defaultOutputSE,appType=gaussAppType)
    production.addBooleStep('{{p2Ver}}',finalAppType.lower(),booleOpts,extraPackages='{{p2EP}}',
                            condDBTag='{{p2CDb}}',ddDBTag='{{p2DDDb}}',outputSE=defaultOutputSE)
    decided=True    
  elif mergingFlag and threeSteps.lower()==mergingApp.lower() and finalAppType.lower() in ['digi','xdigi']:
    prodDescription = 'A three step workflow of Gauss->Boole + Merging' 
    production.addGaussStep('{{p1Ver}}','{{Generator}}',events,gaussOpts,eventType='{{eventType}}',
                            extraPackages='{{p1EP}}',condDBTag='{{p1CDb}}',ddDBTag='{{p1DDDb}}',
                            outputSE=defaultOutputSE,appType=gaussAppType)
    production.addBooleStep('{{p2Ver}}',finalAppType.lower(),booleOpts,extraPackages='{{p2EP}}',
                            condDBTag='{{p2CDb}}',ddDBTag='{{p2DDDb}}',outputSE=defaultOutputSE)
    mergingVersion = '{{p3Ver}}'
    mergingCondDB = '{{p3CDb}}'
    mergingDDDB = '{{p3DDDb}}'
    decided=True
  else:
    gLogger.error('Three steps requested but workflow is neither Gauss->Boole->Brunel with no merging nor \
                   Gauss->Boole + Merging for digi / xdigi. Exiting...')
    DIRAC.exit(2)
    
if twoSteps and not decided:
  if not mergingFlag and twoSteps.lower()=='boole' and finalAppType.lower() in ['digi','xdigi']:
    prodDescription = 'A two step workflow of Gauss->Boole without merging'
    production.addGaussStep('{{p1Ver}}','{{Generator}}',events,gaussOpts,eventType='{{eventType}}',
                            extraPackages='{{p1EP}}',condDBTag='{{p1CDb}}',ddDBTag='{{p1DDDb}}',
                            outputSE=defaultOutputSE,appType=gaussAppType)
    production.addBooleStep('{{p2Ver}}',finalAppType.lower(),booleOpts,extraPackages='{{p2EP}}',
                            condDBTag='{{p2CDb}}',ddDBTag='{{p2DDDb}}',outputSE=defaultOutputSE)
    decided=True
  elif mergingFlag and twoSteps.lower()==mergingApp.lower() and finalAppType.lower() in ['sim','gen']:
    prodDescription = 'A two step workflow of Gauss + Merging'
    gaussAppType=finalAppType
    production.addGaussStep('{{p1Ver}}','{{Generator}}',events,gaussOpts,eventType='{{eventType}}',
                            extraPackages='{{p1EP}}',condDBTag='{{p1CDb}}',ddDBTag='{{p1DDDb}}',
                            outputSE=defaultOutputSE,appType=gaussAppType)
    mergingVersion = '{{p2Ver}}'
    mergingCondDB = '{{p2CDb}}'
    mergingDDDB = '{{p2DDDb}}'
    decided=True
  else:
    gLogger.error('Two steps requested but workflow is neither Gauss->Boole without merging for digi / xdigi \
                  nor Gauss + Merging for sim or gen. Exiting...')  
    DIRAC.exit(2)

if oneStep and not decided:
  prodDescription = 'Assuming one step workflow of Gauss only without merging'
  gaussAppType=finalAppType
  production.addGaussStep('{{p1Ver}}','{{Generator}}',events,gaussOpts,eventType='{{eventType}}',
                          extraPackages='{{p1EP}}',condDBTag='{{p1CDb}}',ddDBTag='{{p1DDDb}}',
                          outputSE=defaultOutputSE,appType=gaussAppType)
  mergingFlag=False
  decided=True

# Finally, in case none of the above were eligible.
if not decided:
  gLogger.error('None of the understood application configurations were understood by this template. Exiting...')
  DIRAC.exit(2)
  
prodDescription = '%s for BK %s %s event type %s with %s events per job and final\
                   application file type %s.' %(prodDescription,configName,configVersion,evtType,events,finalAppType)
gLogger.info(prodDescription)
production.setWorkflowDescription(prodDescription)  
production.addFinalizationStep()
production.setCPUTime(cpu)
production.setProdGroup('{{pDsc}}')
production.setProdPriority(priority)
production.setOutputMode('Any')
production.setFileMask(outputFileMask)

if banTier1s:
  production.banTier1s()

#################################################################################
# End of production API script, now what to do with the production object
#################################################################################

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

result = production.create(requestID=int(requestID),reqUsed=mcRequestTracking,
                          transformation=mcTransformationFlag,bkScript=mergingFlag,
                          parentRequestID=int(parentReq))
if not result['OK']:
  gLogger.error('Error during production creation:\n%s\ncheck that the wkf name is unique.' %(result['Message']))
  DIRAC.exit(2)
  
mcProdID = result['Value']
msg = 'MC Production %s successfully created' %mcProdID
diracProd = DiracProduction()
if extend:
  diracProd.extendProduction(mcProdID,extend,printOutput=True)
  msg += ', extended by %s jobs' %extend

diracProd.production(mcProdID,'automatic',printOutput=True)
msg += ' and started in automatic submission mode.'
gLogger.info(msg)

if not mergingFlag:
  gLogger.info('No merging requested for production ID %s.' %(mcProdID))
  DIRAC.exit(0)

#################################################################################
# This is the start of the merging production definition (if requested)
#################################################################################

bkPassDict = {}

inputBKQuery = { 'SimulationConditions'     : 'All',
                 'DataTakingConditions'     : 'All',
                 'ProcessingPass'           : 'All',
                 'FileType'                 : finalAppType.upper(),
                 'EventType'                : evtType,
                 'ConfigName'               : 'All',
                 'ConfigVersion'            : 'All',
                 'ProductionID'             : int(mcProdID),
                 'DataQualityFlag'          : 'All'}

mergeProd = Production()
if sysConfig:
  mergeProd.setSystemConfig(sysConfig)
     
mergeProd.setProdType('Merge')
mergingName = 'Request{{ID}}_%sMerging_{{pDsc}}_EventType%s_Prod%s_Files%sGB' %(finalAppType,evtType,mcProdID,mergingGroupSize)
mergeProd.setWorkflowName(mergingName)
mergeProd.setWorkflowDescription('MC workflow for merging outputs from a previous production.')
mergeProd.setBKParameters(configName,configVersion,'{{pDsc}}','{{simDesc}}')
mergeProd.setDBTags(mergingCondDB,mergingDDDB)
mergeProd.addMergeStep(mergingVersion,eventType='{{eventType}}',inputDataType=finalAppType.lower(),
                       inputProduction=mcProdID,inputData=[],passDict=bkPassDict,
                       condDBTag=mergingCondDB,ddDBTag=mergingDDDB)
mergeProd.addFinalizationStep(removeInputData=True)
mergeProd.setInputBKSelection(inputBKQuery)
mergeProd.setInputDataPolicy('download')
mergeProd.setJobFileGroupSize(mergingGroupSize)
mergeProd.setProdGroup('{{pDsc}}')
mergeProd.setProdPriority(mergingPriority)

mergeProd.setFileMask(finalAppType.lower())
mergeProd.setProdPlugin(mergingPlugin)

result = mergeProd.create(bkScript=False,requestID=int(requestID),reqUsed=1,
                          transformation=transformationFlag,parentRequestID=int(parentReq))
if not result['OK']:
  gLogger.error('Error during merging production creation:\n%s\n' %(result['Message']))
  DIRAC.exit(2)

prodID = result['Value']
diracProd.production(prodID,'automatic',printOutput=True)
gLogger.info('Merging Production %s successfully created and started in automatic submission mode.' %prodID)

if not transformationFlag:
  gLogger.info('No transformation requested for production ID %s.' %(prodID))
  DIRAC.exit(0)  

#################################################################################
# End of the template.
#################################################################################