""" Moving toward a templates-less system
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import gLogger
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

gLogger = gLogger.getSubLogger( 'LaunchingRequest_run.py' )

pr = ProductionRequest()

pr.stepsList.append( '{{p1Step}}' )
pr.stepsList.append( '{{p2Step}}' )
pr.stepsList.append( '{{p3Step}}' )
pr.stepsList.append( '{{p4Step}}' )
pr.stepsList.append( '{{p5Step}}' )
pr.stepsList.append( '{{p6Step}}' )
pr.stepsList.append( '{{p7Step}}' )
pr.stepsList.append( '{{p8Step}}' )
pr.stepsList.append( '{{p9Step}}' )

pr.resolveSteps()

###########################################
# Configurable and fixed parameters
###########################################

pr.appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

w = '{{w#----->WORKFLOW: choose what to do#}}'
pr.prodsTypeList = '{{r#---List here which prods you want to create#DataSwimming,DataStripping,Merge}}'.split( ',' )
p = '{{p#--now map steps to productions#}}'
p1 = '{{p1#-Production 1 steps (e.g. 1,2,3 = first, second, third)#1,2,3}}'.split( ',' )
if p1:
  pr.stepsInProd.append( p1 )
p2 = '{{p2#-Production 2 steps (e.g. 4,5)#4,5}}'.split( ',' )
if p2:
  pr.stepsInProd.append( p2 )
p3 = '{{p3#-Production 3 steps (e.g. 6)#}}'.split( ',' )
if p3:
  pr.stepsInProd.append( p3 )

certificationFlag = eval( '{{certificationFLAG#GENERAL: Set True for certification test#False}}' )
localTestFlag = eval( '{{localTestFlag#GENERAL: Set True for local test#False}}' )
validationFlag = eval( '{{validationFlag#GENERAL: Set True for validation prod#False}}' )

# workflow params for all productions
pr.sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc46-opt#ANY}}'
pr.startRun = int( '{{startRun#GENERAL: run start, to set the start run#0}}' )
pr.endRun = int( '{{endRun#GENERAL: run end, to set the end of the range#0}}' )
pr.runsList = '{{runsList#GENERAL: discrete list of run numbers (do not mix with start/endrun)#}}'.split( ',' )
pr.targets = ['{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch#}}'] * len( pr.prodsTypeList )
pr.extraOptions = eval( '{{extraOptions#GENERAL: extra options as python dict stepNumber:options#}}' )

#p1 params
p1Plugin = '{{p1PluginType#PROD-P1: production plugin name#LHCbStandard}}'
p1Priority = '{{p1Priority#PROD-p1: priority#2}}'
p1CPU = '{{p1MaxCPUTime#PROD-p1: Max CPU time in secs#1000000}}'
p1GroupSize = '{{p1GroupSize#PROD-P1: Group Size#1}}'
p1AncestorProd = '{{p1AncestorProd#PROD-p1: ancestor production if any#0}}'
p1DataSE = '{{p1DataSE#PROD-p1: Output Data Storage Element#Tier1-DST}}'
p1FilesPerJob = '{{p1FilesPerJob#PROD-p1: Group size or number of files per job#1}}'
p1Policy = '{{p1Policy#PROD-P1: data policy (download or protocol)#download}}'
p1RemoveInputs = eval( '{{p1RemoveInputs#PROD-P1: removeInputs flag#False}}' )

#p2 params
p2Plugin = '{{p2PluginType#PROD-P1: production plugin name#LHCbStandard}}'
p2Priority = '{{p2Priority#PROD-p2: priority#2}}'
p2CPU = '{{p2MaxCPUTime#PROD-p2: Max CPU time in secs#1000000}}'
p2GroupSize = '{{p2GroupSize#PROD-P2: Group Size#1}}'
p2AncestorProd = '{{p2AncestorProd#PROD-p2: ancestor production if any#0}}'
p2DataSE = '{{p2DataSE#PROD-p2: Output Data Storage Element#Tier1-DST}}'
p2FilesPerJob = '{{p2FilesPerJob#PROD-p2: Group size or number of files per job#1}}'
p2Policy = '{{p2Policy#PROD-P1: data policy (download or protocol)#download}}'
p2RemoveInputs = eval( '{{p2RemoveInputs#PROD-P2: removeInputs flag#False}}' )

#p3 params
p3Plugin = '{{p3PluginType#PROD-P1: production plugin name#LHCbStandard}}'
p3Priority = '{{p3Priority#PROD-p3: priority#2}}'
p3CPU = '{{p3MaxCPUTime#PROD-p3: Max CPU time in secs#1000000}}'
p3GroupSize = '{{p3GroupSize#PROD-P3: Group Size#1}}'
p3AncestorProd = '{{p3AncestorProd#PROD-p3: ancestor production if any#0}}'
p3DataSE = '{{p3DataSE#PROD-p3: Output Data Storage Element#Tier1-DST}}'
p3FilesPerJob = '{{p3FilesPerJob#PROD-p3: Group size or number of files per job#1}}'
p3Policy = '{{p3Policy#PROD-P1: data policy (download or protocol)#download}}'
p3RemoveInputs = eval( '{{p3RemoveInputs#PROD-P3: removeInputs flag#False}}' )

parentReq = '{{_parent}}'
if not parentReq:
  pr.requestID = '{{ID}}'
else:
  pr.requestID = parentReq

pr.prodGroup = '{{pDsc}}'
#used in case of a test e.g. certification etc.
pr.configName = '{{configName}}'
pr.configVersion = '{{configVersion}}'
#Other parameters from the request page
pr.DQFlag = '{{inDataQualityFlag}}' #UNCHECKED
pr.dataTakingConditions = '{{simDesc}}'
pr.processingPass = '{{inProPass}}'
pr.bkFileType = '{{inFileType}}'
pr.eventType = '{{eventType}}'
pr.events = int( '{{NbOfEvents}}' )

if certificationFlag or localTestFlag:
  pr.testFlag = True
  if certificationFlag:
    pr.publishFlag = True
  if localTestFlag:
    pr.publishFlag = False
    pr.prodsToLaunch = [1]
else:
  pr.publishFlag = True
  pr.testFlag = False

inputDataList = []
if not pr.publishFlag:
  testData = 'LFN:/lhcb/LHCb/Collision11/CHARMCOMPLETEEVENT.DST/00012586/0000/00012586_00000706_1.charmcompleteevent.dst'
  inputDataList.append( testData )
  p1Policy = 'protocol'
  evtsPerJob = '5'
  bkScriptFlag = True
else:
  bkScriptFlag = False

#In case we want just to test, we publish in the certification/test part of the BKK
if pr.testFlag:
  pr.configName = 'certification'
  pr.configVersion = 'test'
  pr.startRun = '75336'
  pr.endRun = '75340'
  p1CPU = '100000'
  pr.dataTakingConditions = 'Beam3500GeV-VeloClosed-MagDown'
  pr.processingPass = 'Real Data/Reco12/Stripping17'
  pr.bkFileType = 'CHARMCOMPLETEEVENT.DST'
  pr.eventType = '90000000'
  pr.DQFlag = 'ALL'

pr._buildFullBKKQuery()

pr.outputSEs = [x for x in [p1DataSE, p2DataSE, p3DataSE] if x != '']
pr.removeInputsFlags = [p1RemoveInputs, p2RemoveInputs, p3RemoveInputs][0:len( pr.prodsTypeList )]
pr.priorities = [p1Priority, p2Priority, p3Priority][0:len( pr.prodsTypeList )]
pr.CPUs = [p1CPU, p2CPU, p3CPU][0:len( pr.prodsTypeList )]
pr.groupSizes = [p1GroupSize, p2GroupSize, p3GroupSize][0:len( pr.prodsTypeList )]
pr.plugins = [p1Plugin, p2Plugin, p3Plugin][0:len( pr.prodsTypeList )]
pr.inputs = [inputDataList]
pr.inputDataPolicies = [p1Policy, p2Policy, p3Policy][0:len( pr.prodsTypeList )]
