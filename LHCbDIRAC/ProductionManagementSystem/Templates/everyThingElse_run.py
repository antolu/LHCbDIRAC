""" Moving toward a templates-less system
"""

import ast

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import gLogger, exit as DIRACexit
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

__RCSID__ = "$Id$"

gLogger = gLogger.getSubLogger( 'LaunchingRequest_run.py' )

pr = ProductionRequest()

stepsList = [ '{{p1Step}}' ]
stepsList.append( '{{p2Step}}' )
stepsList.append( '{{p3Step}}' )
stepsList.append( '{{p4Step}}' )
stepsList.append( '{{p5Step}}' )
stepsList.append( '{{p6Step}}' )
stepsList.append( '{{p7Step}}' )
stepsList.append( '{{p8Step}}' )
stepsList.append( '{{p9Step}}' )
stepsList.append( '{{p10Step}}' )
stepsList.append( '{{p11Step}}' )
stepsList.append( '{{p12Step}}' )
stepsList.append( '{{p13Step}}' )
stepsList.append( '{{p14Step}}' )
stepsList.append( '{{p15Step}}' )
stepsList.append( '{{p16Step}}' )
stepsList.append( '{{p17Step}}' )
stepsList.append( '{{p18Step}}' )
stepsList.append( '{{p19Step}}' )
stepsList.append( '{{p20Step}}' )
pr.stepsList = stepsList

###########################################
# Configurable and fixed parameters
###########################################

pr.appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

w = '{{w#----->WORKFLOW: choose what to do#}}'
pr.prodsTypeList = '{{r#---List here which prods you want to create#DataSwimming,DataStripping,Merge}}'.split( ',' )
p = '{{p#--now map steps to productions#}}'
p1 = '{{p1#-Production 1 steps (e.g. 1,2,3 = first, second, third)#1,2,3}}'
if p1:
  p1 = p1.split( ',' )
  p1 = [int( x ) for x in p1]
  pr.stepsInProds.append( p1 )
p2 = '{{p2#-Production 2 steps (e.g. 4,5)#4,5}}'
if p2:
  p2 = p2.split( ',' )
  p2 = [int( x ) for x in p2]
  pr.stepsInProds.append( p2 )
p3 = '{{p3#-Production 3 steps (e.g. 6)#}}'
if p3:
  p3 = p3.split( ',' )
  p3 = [int( x ) for x in p3]
  pr.stepsInProds.append( p3 )

certificationFlag = ast.literal_eval( '{{certificationFLAG#GENERAL: Set True for certification test#False}}' )
localTestFlag = ast.literal_eval( '{{localTestFlag#GENERAL: Set True for local test#False}}' )
validationFlag = ast.literal_eval( '{{validationFlag#GENERAL: Set True for validation prod#False}}' )

# workflow params for all productions
pr.startRun = int( '{{startRun#GENERAL: run start, to set the start run#0}}' )
pr.endRun = int( '{{endRun#GENERAL: run end, to set the end of the range#0}}' )
pr.runsList = '{{runsList#GENERAL: discrete list of run numbers (do not mix with start/endrun)#}}'
pr.targets = ['{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch#}}'] * len( pr.prodsTypeList )
extraOptions = '{{extraOptions#GENERAL: extra options as python dict stepNumber:options#}}'
if extraOptions:
  pr.extraOptions = ast.literal_eval( extraOptions )
pr.derivedProduction = int( '{{AncestorProd#GENERAL: ancestor prod to be derived#0}}' )
pr.previousProdID = int( '{{previousProdID#GENERAL: previous prod ID (for BK query)#0}}' )
modulesList = '{{modulesList#GENERAL: custom modules list#}}'
enablePopularityReport = ast.literal_eval( '{{popularityReport#GENERAL: enable popularity report#False}}' )

# p1 params
p1Plugin = '{{p1PluginType#PROD-P1: production plugin name#LHCbStandard}}'
p1Priority = int( '{{p1Priority#PROD-P1: priority#2}}' )
p1CPU = '{{p1MaxCPUTime#PROD-P1: Max CPU time in secs#1000000}}'
p1GroupSize = '{{p1GroupSize#PROD-P1: Group size or number of files per job#1}}'
p1DataSE = '{{p1DataSE#PROD-P1: Output Data Storage Element#Tier1-DST}}'
try:
  p1DataSESpecial = ast.literal_eval( '{{p1DataSESpecial#PROD-P1: Special SEs per file type, e.g. {"T1":"SE1"}#}}' )
except SyntaxError:
  p1DataSESpecial = {}
p1Policy = '{{p1Policy#PROD-P1: data policy (download or protocol)#download}}'
p1RemoveInputs = ast.literal_eval( '{{p1RemoveInputs#PROD-P1: removeInputs flag#False}}' )
p1StepMask = '{{P1StepMask#PROD-P1: step output to save, semicolon separated (default is last)#}}'
p1multicoreFlag = '{{P1MulticoreFLag#PROD-P1: multicore flag#True}}'
p1outputMode = '{{P1OutputMode#PROD-P1: output mode#Local}}'
p1eventsRequested = '{{P1EventsRequested#PROD-P1: events requested (-1 = ALL)#-1}}'
p1ancestorDepth = int( '{{P1AncestorDepth#PROD-P1: ancestor depth#0}}' )

# p2 params
p2Plugin = '{{p2PluginType#PROD-P2: production plugin name#LHCbStandard}}'
p2Priority = int( '{{p2Priority#PROD-P2: priority#2}}' )
p2CPU = '{{p2MaxCPUTime#PROD-P2: Max CPU time in secs#1000000}}'
p2GroupSize = '{{p2GroupSize#PROD-P2: Group Size#1}}'
p2DataSE = '{{p2DataSE#PROD-P2: Output Data Storage Element#Tier1-DST}}'
try:
  p2DataSESpecial = ast.literal_eval( '{{p2DataSESpecial#PROD-P2: Special SEs per file type, e.g. {"T1":"SE1"}#}}' )
except SyntaxError:
  p2DataSESpecial = {}
p2Policy = '{{p2Policy#PROD-P2: data policy (download or protocol)#download}}'
p2RemoveInputs = ast.literal_eval( '{{p2RemoveInputs#PROD-P2: removeInputs flag#False}}' )
p2StepMask = '{{P2StepMask#PROD-P2: step output to save, semicolon separated (default is last#}}'
p2multicoreFlag = '{{P2MulticoreFLag#PROD-P2: multicore flag#True}}'
p2outputMode = '{{P2OutputMode#PROD-P2: output mode#Local}}'
p2eventsRequested = '{{P2EventsRequested#PROD-P2: events requested (-1 = ALL)#-1}}'
p2ancestorDepth = int( '{{P2AncestorDepth#PROD-P2: ancestor depth#0}}' )

# p3 params
p3Plugin = '{{p3PluginType#PROD-P3: production plugin name#LHCbStandard}}'
p3Priority = int( '{{p3Priority#PROD-p3: priority#2}}' )
p3CPU = '{{p3MaxCPUTime#PROD-P3: Max CPU time in secs#1000000}}'
p3GroupSize = '{{p3GroupSize#PROD-P3: Group Size#1}}'
p3DataSE = '{{p3DataSE#PROD-P3: Output Data Storage Element#Tier1-DST}}'
try:
  p3DataSESpecial = ast.literal_eval( '{{p3DataSESpecial#PROD-P3: Special SEs per file type, e.g. {"T1":"SE1"}#}}' )
except SyntaxError:
  p3DataSESpecial = {}
p3Policy = '{{p3Policy#PROD-P3: data policy (download or protocol)#download}}'
p3RemoveInputs = ast.literal_eval( '{{p3RemoveInputs#PROD-P3: removeInputs flag#False}}' )
p3StepMask = '{{P3StepMask#PROD-P3: step output to save, semicolon separated (default is last#}}'
p3multicoreFlag = '{{P3MulticoreFLag#PROD-P3: multicore flag#True}}'
p3outputMode = '{{P3OutputMode#PROD-P3: output mode#Any}}'
p3eventsRequested = '{{P3EventsRequested#PROD-P3: events requested (-1 = ALL)#-1}}'
p3ancestorDepth = int( '{{P3AncestorDepth#PROD-P3: ancestor depth#0}}' )

parentReq = '{{_parent}}'
if not parentReq:
  pr.requestID = '{{ID}}'
else:
  pr.requestID = parentReq

pr.resolveSteps()

pr.prodGroup = '{{inProPass}}' + '/' + '{{pDsc}}'
# used in case of a test e.g. certification etc.
pr.configName = '{{configName}}'
pr.configVersion = '{{configVersion}}'
pr.outConfigName = pr.configName
# Other parameters from the request page
pr.dqFlag = '{{inDataQualityFlag}}'  # UNCHECKED
pr.dataTakingConditions = '{{simDesc}}'
pr.processingPass = '{{inProPass}}'
if p1[0] == 1 and pr.prodsTypeList[0].lower() != 'mcsimulation':
  pr.bkFileType = '{{inFileType}}'
  pr.bkQueries = ['Full']
elif p1[0] == 1 and pr.prodsTypeList[0].lower() == 'mcsimulation':
  pr.bkQueries = ['']
  pr.prodGroup = '{{pDsc}}'
elif p1[0] != 1 and pr.prodsTypeList[0].lower() != 'mcsimulation':
  pr.bkQueries = ['fromPreviousProd']
  if not pr.previousProdID:
    gLogger.error( "Please specify an input production" )
    DIRACexit( 2 )
pr.eventType = '{{eventType}}'
pr.visibility = 'Yes'

if modulesList:
  pr.modulesList = modulesList.replace( ' ', '' ).split( ',' )

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

if validationFlag:
  pr.outConfigName = 'validation'

if enablePopularityReport:
  pr.modulesList.append( 'FileUsage' )

inputDataList = []
if not pr.publishFlag:
  testData = 'LFN:/lhcb/LHCb/Collision11/CHARMCOMPLETEEVENT.DST/00012586/0000/00012586_00000706_1.charmcompleteevent.dst'
  inputDataList.append( testData )
  p1Policy = 'protocol'
  evtsPerJob = '5'
  bkScriptFlag = True
else:
  bkScriptFlag = False

# In case we want just to test, we publish in the certification/test part of the BKK
if pr.testFlag:
  pr.outConfigName = 'certification'
  pr.configVersion = 'test'
  pr.startRun = '75336'
  pr.endRun = '75340'
  p1CPU = '100000'
  pr.events = ['2000']
  pr.dataTakingConditions = 'Beam3500GeV-VeloClosed-MagDown'
  pr.processingPass = 'Real Data/Reco12/Stripping17'
  pr.bkFileType = 'CHARMCOMPLETEEVENT.DST'
  pr.eventType = '90000000'
  pr.dqFlag = 'ALL'

if not p1StepMask:
  p1StepMask = len( pr.stepsInProds[0] )
if not p2StepMask:
  try:
    p2StepMask = len( pr.stepsInProds[1] )
  except IndexError:
    p2StepMask = ''
if not p3StepMask:
  try:
    p3StepMask = len( pr.stepsInProds[2] )
  except IndexError:
    p3StepMask = ''

pr.outputSEs = [x for x in [p1DataSE, p2DataSE, p3DataSE] if x != '']
pr.specialOutputSEs = [p1DataSESpecial, p2DataSESpecial, p3DataSESpecial]
pr.removeInputsFlags = [p1RemoveInputs, p2RemoveInputs, p3RemoveInputs][0:len( pr.prodsTypeList )]
pr.priorities = [p1Priority, p2Priority, p3Priority][0:len( pr.prodsTypeList )]
pr.cpus = [p1CPU, p2CPU, p3CPU][0:len( pr.prodsTypeList )]
pr.groupSizes = [p1GroupSize, p2GroupSize, p3GroupSize][0:len( pr.prodsTypeList )]
pr.plugins = [p1Plugin, p2Plugin, p3Plugin][0:len( pr.prodsTypeList )]
pr.inputs = [inputDataList, [], []][0:len( pr.prodsTypeList )]
pr.inputDataPolicies = [p1Policy, p2Policy, p3Policy][0:len( pr.prodsTypeList )]
pr.outputFileSteps = [p1StepMask, p2StepMask, p3StepMask]
pr.multicore = [p1multicoreFlag, p2multicoreFlag, p3multicoreFlag]
pr.outputModes = [p1outputMode, p2outputMode, p3outputMode]
pr.events = [p1eventsRequested, p2eventsRequested, p3eventsRequested]
pr.ancestorDepths = [p1ancestorDepth, p2ancestorDepth, p3ancestorDepth]

pr.buildAndLaunchRequest()
