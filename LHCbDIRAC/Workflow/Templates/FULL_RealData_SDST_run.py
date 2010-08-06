########################################################################
# $Header: /local/reps/dirac/DIRAC3/LHCbSystem/WorkflowTemplates/FESTRecoTemplate_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $
########################################################################

"""  The FULL Real Data Reco template creates a workflow for Brunel & DaVinci with
     configurable number of events, CPU time, jobs to extend and priority.
"""

__RCSID__ = "$Id: FESTRecoTemplate_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $"

import sys,os,string
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'FULL_RealData_SDST'

from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

#Configurable parameters
priority = '{{Priority#Production priority#7}}'
cpu = '{{MaxCPUTime#MC Max CPU time in secs#600000}}'
plugin = '{{PluginType#Production plugin name#AtomicRun}}'
ancestorProd = '{{AncestorProd#Ancestor production if any#0}}'
dataSE = '{{DataSE#Output Data Storage Element#Tier1_M-DST}}'
filesPerJob = '{{FilesPerJob#Group size or number of files per job#1}}'
appendName = '{{AppendName#String to append to production name#1}}'
transFlag = '{{Transformation#Distribute production output data True/False (set to False if merging later)#True}}'
bkPublishing = '{{BKPublishFlag#Publish this production to the BK(set to False if merging later)#True}}'
startRun = '{{RunStart#Run start for a run range set the start run#0}}'
endRun = '{{RunEnd#Run end to set the end of the range#0}}'
sysConfig = '{{SystemConfig#Production system config e.g. slc4_ia32_gcc34#x86_64-slc5-gcc43-opt}}'
destination = '{{Destination#Set a destination site e.g. LCG.CERN.ch#ALL}}'

#Other parameters
dqFlag = '{{inDataQualityFlag}}' #UNCHECKED
inProductionID='{{inProductionID}}'

transFlag = eval(transFlag)
bkPublishing = eval(bkPublishing)

#The below are fixed for the FULL stream
recoType="FULL"
appType = "SDST"

if args:
  inProductionID = int(inProductionID)
  inProductionID = 0 #this is not used

inputBKQuery = { 'SimulationConditions'     : 'All',
                 'DataTakingConditions'     : '{{simDesc}}',
                 'ProcessingPass'           : '{{inProPass}}',
                 'FileType'                 : '{{inFileType}}',
                 'EventType'                : '{{eventType}}',
                 'ConfigName'               : '{{configName}}',
                 'ConfigVersion'            : '{{configVersion}}',
                 'ProductionID'             : inProductionID,
                 'DataQualityFlag'          : dqFlag}


if int(endRun) and int(startRun):
  if int(endRun)<int(startRun):
    print 'ERROR: your end run should be less than your start run!'
    sys.exit(0)

if int(startRun):
  inputBKQuery['StartRun']=int(startRun)
if int(endRun):
  inputBKQuery['EndRun']=int(endRun)


production = Production()

if not destination.lower()=='all':
  print 'Forcing destination site %s for production' %(destination)
  production.setDestination(destination)

if sysConfig:
  production.setSystemConfig(sysConfig)

production.setCPUTime(cpu)
production.setProdType('DataReconstruction')
wkfName='Request{{ID}}_{{pDsc}}_{{eventType}}_{{p1App}}{{p1Ver}}_{{p2App}}{{p2Ver}}_DDDB{{p1DDDb}}_CondDB{{p1CDb}}'
production.setWorkflowName('%s_%s_%s' %(recoType,wkfName,appendName))
production.setWorkflowDescription("Real data FULL reconstruction production.")
production.setBKParameters('{{configName}}','{{configVersion}}','{{pDsc}}','{{simDesc}}')
production.setInputBKSelection(inputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

production.addBrunelStep("{{p1Ver}}",appType.lower(),"{{p1Opt}}",extraPackages='{{p1EP}}',eventType='{{eventType}}',inputData=[],inputDataType='mdf',outputSE=dataSE,dataType='Data',histograms=True)
production.addDaVinciStep("{{p2Ver}}","dst","{{p2Opt}}",extraPackages='{{p2EP}}',inputDataType='sdst',dataType='Data',histograms=True)

production.addFinalizationStep()

production.setInputBKSelection(inputBKQuery)
production.setProdGroup('{{pDsc}}')

production.setFileMask("dst;root;sdst;fetc")
production.setProdPriority(priority)
production.setProdPlugin(plugin)
production.setInputDataPolicy('download')

scriptFlag = False
if not bkPublishing:
  scriptFlag = True

result = production.create(bkQuery=inputBKQuery,groupSize=filesPerJob,derivedProduction=int(ancestorProd),bkScript=scriptFlag,requestID=int('{{ID}}'),reqUsed=1,transformation=transFlag)
if not result['OK']:
  print 'Error:',result['Message']

print result

prodID = result['Value']
msg = 'Production %s successfully created' %prodID
diracProd = DiracProduction()
diracProd.production(prodID,'automatic',printOutput=True)
msg += ' and started in automatic submission mode.'
print msg
