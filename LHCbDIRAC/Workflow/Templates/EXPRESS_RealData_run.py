########################################################################
# $Header: /local/reps/dirac/DIRAC3/LHCbSystem/WorkflowTemplates/FESTRecoTemplate_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $
########################################################################

"""  The EXPRESS Real Data Reco template creates a workflow for Brunel & DaVinci with
     configurable number of events, CPU time, jobs to extend and priority.
"""

__RCSID__ = "$Id: FESTRecoTemplate_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $"

import sys,os,string
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'EXPRESS_RealData'

####################### BLACK MAGIC TO SET UP PROXY CONTAINED WITHIN #########################
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
localCfg = LocalConfiguration()
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate","yes" )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  print "There were errors when loading configuration",resultDict[ 'Message' ]
  sys.exit(1)
###############################################################################

from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

#Configurable parameters
priority = '{{Priority#Production priority#7}}'
cpu = '{{MaxCPUTime#MC Max CPU time in secs#100000}}'
plugin = '{{PluginType#Production plugin name#ByRun}}'
ancestorProd = '{{AncestorProd#Ancestor production if any#0}}'
dataSE = '{{DataSE#Output Data Storage Element#CERN_M-DST}}'
filesPerJob = '{{FilesPerJob#Group size or number of files per job#1}}'
appendName = '{{AppendName#String to append to production name#1}}'

#Other parameters
dqFlag = '{{inDataQualityFlag}}' #UNCHECKED
inProductionID='{{inProductionID}}'

#The below are fixed for the EXPRESS stream
recoType="EXPRESS"
appType = "DST"

if args:
  inProductionID = int(inProductionID)


inputBKQuery = { 'SimulationConditions'     : 'All',
                 'DataTakingConditions'     : '{{simDesc}}',
                 'ProcessingPass'           : '{{inProPass}}',
                 'FileType'                 : '{{inFileType}}',
                 'EventType'                : '{{eventType}}',
                 'ConfigName'               : '{{configName}}',
                 'ConfigVersion'            : '{{configVersion}}',
                 'ProductionID'             : inProductionID,
                 'DataQualityFlag'          : dqFlag}

production = Production()
production.setCPUTime(cpu)
production.setProdType('DataReconstruction')
wkfName='Request{{ID}}_{{pDsc}}_{{eventType}}_{{p1App}}{{p1Ver}}_{{p2App}}{{p2Ver}}_DDDB{{p1DDDb}}_CondDB{{p1CDb}}'
production.setWorkflowName('%s_%s_%s' %(recoType,wkfName,appendName))
production.setWorkflowDescription("Real data EXPRESS reconstruction production.")
production.setBKParameters('{{configName}}','{{configVersion}}','{{pDsc}}','{{simDesc}}')
production.setInputBKSelection(inputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

production.addBrunelStep("{{p1Ver}}",appType.lower(),"{{p1Opt}}",extraPackages='{{p1EP}}',eventType='{{eventType}}',inputData=[],inputDataType='mdf',outputSE=dataSE,histograms=True)
production.addDaVinciStep("{{p2Ver}}","davincihist","{{p2Opt}}",extraPackages='{{p2EP}}',inputDataType=appType.lower(),histograms=True)
production.addFinalizationStep()

production.setInputBKSelection(inputBKQuery)
production.setProdGroup('{{pDsc}}')
production.setFileMask("%s;root" %(appType.lower()))
production.setProdPriority(priority)
production.setProdPlugin(plugin)
production.setInputDataPolicy('download')
if not args:
  print 'No arguments specified, will create workflow only.'
  os.chdir(start)
  production.setWorkflowName(templateName)
  wf =  production.workflow.toXMLFile(templateName)
  print 'Created local workflow template: %s' %templateName
  sys.exit(0)

result = production.create(bkQuery=inputBKQuery,groupSize=filesPerJob,derivedProduction=int(ancestorProd),bkScript=False,requestID=int('{{ID}}'),reqUsed=1,transformation=False)
if not result['OK']:
  print 'Error:',result['Message']

prodID = result['Value']
msg = 'Production %s successfully created' %prodID
diracProd = DiracProduction()
diracProd.production(prodID,'automatic',printOutput=True)
msg += ' and started in automatic submission mode.'
print msg
