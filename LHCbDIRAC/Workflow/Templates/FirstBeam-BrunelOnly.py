########################################################################
# $Header: /local/reps/dirac/DIRAC3/LHCbSystem/WorkflowTemplates/FESTRecoTemplate_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $
########################################################################

"""  The Real Data Reco template creates a workflow for Brunel with
     configurable number of events, CPU time, jobs to extend and priority.
"""

__RCSID__ = "$Id: FESTRecoTemplate_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $"

import sys,os,string
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'FirstBeam_BrunelOnly'

from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

#Configurable parameters
priority = '{{Priority#Production priority#7}}'
plugin = '{{PluginType#Production plugin name#ByRun}}'
ancestorProd = '{{AncestorProd#Ancestor production if any#0}}'
dataSE = '{{DataSE#Output Data Storage Element#Tier1-DST}}'

#Other parameters
dqFlag = '{{inDataQualityFlag}}' #UNCHECKED
inProductionID='{{inProductionID}}'
recoType="FULL"
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
production.setProdType('DataReconstruction')
wkfName='{{pDsc}}_{{eventType}}_{{p1App}}{{p1Ver}}_DDDB{{p1DDDb}}_CondDB{{p1CDb}}_Request{{ID}}_1'
production.setWorkflowName('%s_%s' %(recoType,wkfName))
production.setWorkflowDescription("Real data reconstruction production.")
production.setBKParameters('{{configName}}','{{configVersion}}','{{pDsc}}','{{simDesc}}')
production.setInputBKSelection(inputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

production.addBrunelStep("{{p1Ver}}",appType.lower(),"{{p1Opt}}",extraPackages='{{p1EP}}',eventType='{{eventType}}',inputData=[],inputDataType='mdf',outputSE=dataSE,histograms=True)
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

result = production.create(bkQuery=inputBKQuery,groupSize=1,derivedProduction=int(ancestorProd),bkScript=False,requestID=int('{{ID}}'),reqUsed=1)
if not result['OK']:
  print 'Error:',result['Message']

prodID = result['Value']
msg = 'Production %s successfully created' %prodID
diracProd = DiracProduction()
diracProd.production(prodID,'start',printOutput=True)
msg += ' and started in manual submission mode.'
print msg
