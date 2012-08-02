""" Moving toward a templates-less system
"""

__RCSID__ = "$Id$"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
from DIRAC.Core.Base import Script
Script.parseCommandLine()

import LHCbDIRAC.Workflow.Templates.TemplatesUtilities

from DIRAC import gLogger
gLogger = gLogger.getSubLogger( 'EveryThingElse_run.py' )

#################################################################################
# Below here is the actual production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

###########################################
# Configurable and fixed parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

w = '{{w#----->WORKFLOW: choose what to do#}}'
r = '{{r#---List here which productions you want to create#DataSwimming,DataStripping,Merge}}'
p = '{{p#--now map steps to productions#}}'
p1 = '{{p1#-Production 1 steps (e.g. 1,2,3)#1,2,3}}'
p2 = '{{p2#-Production 2 steps (e.g. 4,5)#4,5}}'
p3 = '{{p3#-Production 3 steps (e.g. 6)#}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True for validation prod#False}}'

# workflow params for all productions
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc46-opt#ANY}}'
startRun = '{{startRun#GENERAL: run start, to set the start run#0}}'
endRun = '{{endRun#GENERAL: run end, to set the end of the range#0}}'
runsList = '{{runsList#GENERAL: discrete list of run numbers (do not mix with start/endrun)#}}'
targetSite = '{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch#ALL}}'
extraOptions = '{{extraOptions#GENERAL: extra options as python dict stepNumber:options#}}'
outputMode = '{{outputMode#GENERAL: Workflow upload workflow output#Any}}'

#p1 params
p1Plugin = '{{p1PluginType#PROD-P1: production plugin name#LHCbStandard}}'
p1Priority = '{{p1Priority#PROD-p1: priority#2}}'
p1CPU = '{{p1MaxCPUTime#PROD-p1: Max CPU time in secs#1000000}}'
p1AncestorProd = '{{p1AncestorProd#PROD-p1: ancestor production if any#0}}'
p1DataSE = '{{p1DataSE#PROD-p1: Output Data Storage Element#Tier1-DST}}'
p1FilesPerJob = '{{p1FilesPerJob#PROD-p1: Group size or number of files per job#1}}'
p1Policy = '{{p1Policy#PROD-P1: data policy (download or protocol)#download}}'

#p2 params
p2Plugin = '{{p2PluginType#PROD-P1: production plugin name#LHCbStandard}}'
p2Priority = '{{p2Priority#PROD-p2: priority#2}}'
p2CPU = '{{p2MaxCPUTime#PROD-p2: Max CPU time in secs#1000000}}'
p2AncestorProd = '{{p2AncestorProd#PROD-p2: ancestor production if any#0}}'
p2DataSE = '{{p2DataSE#PROD-p2: Output Data Storage Element#Tier1-DST}}'
p2FilesPerJob = '{{p2FilesPerJob#PROD-p2: Group size or number of files per job#1}}'
p2Policy = '{{p2Policy#PROD-P1: data policy (download or protocol)#download}}'

#p3 params
p3Plugin = '{{p3PluginType#PROD-P1: production plugin name#LHCbStandard}}'
p3Priority = '{{p3Priority#PROD-p3: priority#2}}'
p3CPU = '{{p3MaxCPUTime#PROD-p3: Max CPU time in secs#1000000}}'
p3AncestorProd = '{{p3AncestorProd#PROD-p3: ancestor production if any#0}}'
p3DataSE = '{{p3DataSE#PROD-p3: Output Data Storage Element#Tier1-DST}}'
p3FilesPerJob = '{{p3FilesPerJob#PROD-p3: Group size or number of files per job#1}}'
p3Policy = '{{p3Policy#PROD-P1: data policy (download or protocol)#download}}'


requestID = '{{ID}}'
parentReq = '{{_parent}}'
prodGroup = '{{pDsc}}'
#used in case of a test e.g. certification etc.
bkConfigName = '{{configName}}'
bkConfigVersion = '{{configVersion}}'
#Other parameters from the request page
DQFlag = '{{inDataQualityFlag}}' #UNCHECKED
dataTakingCond = '{{simDesc}}'
processingPass = '{{inProPass}}'
bkFileType = '{{inFileType}}'
eventType = '{{eventType}}'
NbOfEvents = int( '{{NbOfEvents}}' )
#Productions-steps
p1 = p1.split( ',' )
p2 = p2.split( ',' )
p3 = p3.split( ',' )

certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )
if extraOptions:
  extraOptions = eval( extraOptions )

if certificationFlag or localTestFlag:
  testFlag = True
  if certificationFlag:
    publishFlag = True
  if localTestFlag:
    publishFlag = False
else:
  publishFlag = True
  testFlag = False

diracProd = DiracProduction()

inputDataList = []
if not publishFlag:
  testData = 'LFN:/lhcb/LHCb/Collision11/CHARMCOMPLETEEVENT.DST/00012586/0000/00012586_00000706_1.charmcompleteevent.dst'
  inputDataList.append( testData )
  p1Policy = 'protocol'
  evtsPerJob = '5'
  bkScriptFlag = True
else:
  bkScriptFlag = False

#In case we want just to test, we publish in the certification/test part of the BKK
if testFlag:
  bkConfigName = 'certification'
  bkConfigVersion = 'test'
  startRun = '75336'
  endRun = '75340'
  recoCPU = '100000'
  dataTakingCond = 'Beam3500GeV-VeloClosed-MagDown'
  processingPass = 'Real Data/Reco12/Stripping17'
  bkFileType = 'CHARMCOMPLETEEVENT.DST'
  eventType = '90000000'
  DQFlag = 'ALL'

DQFlag = DQFlag.replace( ',', ';;;' ).replace( ' ', '' )

inputBKQuery = {
                    'DataTakingConditions'     : dataTakingCond,
                    'ProcessingPass'           : processingPass,
                    'FileType'                 : bkFileType,
                    'EventType'                : eventType,
                    'ConfigName'               : bkConfigName,
                    'ConfigVersion'            : bkConfigVersion,
                    'ProductionID'             : 0,
                    'DataQualityFlag'          : DQFlag
                    }


stepsList = []

stepsList.append( '{{p1Step}}' )
stepsList.append( '{{p2Step}}' )
stepsList.append( '{{p3Step}}' )
stepsList.append( '{{p4Step}}' )
stepsList.append( '{{p5Step}}' )
stepsList.append( '{{p6Step}}' )
stepsList.append( '{{p7Step}}' )
stepsList.append( '{{p8Step}}' )
stepsList.append( '{{p9Step}}' )

#get a list of steps dictionaries
stepsDictList = LHCbDIRAC.Workflow.Templates.TemplatesUtilities.resolveSteps( stepsList )

p1Steps = []
p2Steps = []
p3Steps = []

prodsList = []







prodID = 0
for prodType, stepsList, bkQuery, removeInput, tracking, outputSE, priority, cpu, inputDataList in prodsList:
  prod = LHCbDIRAC.Workflow.Templates.TemplatesUtilities.buildProduction( prodType = prodType,
                                                                          stepsList = stepsList,
                                                                          requestID = requestID,
                                                                          prodDesc = prodGroup,
                                                                          configName = bkConfigName,
                                                                          configVersion = bkConfigVersion,
                                                                          dataTakingConditions = dataTakingCond,
                                                                          appendName = appendName,
                                                                          extraOptions = extraOptions,
                                                                          defaultOutputSE = outputSE,
                                                                          inputDataList = inputDataList,
                                                                          eventType = '{{eventType}}',
                                                                          events = eventType,
                                                                          priority = priority,
                                                                          cpu = cpu,
                                                                          sysConfig = sysConfig,
                                                                          generatorName = '{{Generator}}',
                                                                          outputMode = outputMode,
                                                                          targetSite = targetSite,
                                                                          removeInputData = removeInput,
                                                                          bkQuery = bkQuery,
                                                                          previousProdID = prodID )

  prodID = LHCbDIRAC.Workflow.Templates.TemplatesUtilities.launchProduction( prod = prod,
                                                                             publishFlag = publishFlag,
                                                                             testFlag = testFlag,
                                                                             requestID = requestID,
                                                                             tracking = tracking,
                                                                             diracProd = diracProd,
                                                                             logger = gLogger )
