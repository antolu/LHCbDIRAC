########################################################################
# $Header: /local/reps/dirac/DIRAC3/LHCbSystem/WorkflowTemplates/FESTRecoTemplate_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $
########################################################################

"""  The Real Data Reco template creates a workflow for Brunel with
     configurable number of events, CPU time, jobs to extend and priority.
"""

__RCSID__ = "$Id$"

import sys,os,string
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'Online_BrunelOnly'

from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

#Configurable parameters
priority = '{{Priority#Production priority#7}}'
plugin = '{{PluginType#Production plugin name#Standard}}'
ancestorProd = '{{AncestorProd#Ancestor production if any#0}}'
dataSE = '{{DataSE#Output Data Storage Element#Tier1-RDST}}'
appendName = '{{AppendName#String to append to production name#1}}'
appType = '{{ApplicationType#Output File Type e.g. DST/RDST#RDST}}'

#Other parameters
dqFlag = '{{inDataQualityFlag}}' #UNCHECKED
inProductionID='{{inProductionID}}'

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

production.setPlatform('DIRAC') #IMPORTANT for Online tests not to interfere with production
production.setDestination('DIRAC.ONLINE-FARM.ch')

production.setProdType('DataReconstruction')
wkfName='Request{{ID}}_{{pDsc}}_{{eventType}}_{{p1App}}{{p1Ver}}_DDDB{{p1DDDb}}_CondDB{{p1CDb}}_1'
production.setWorkflowName('%s_%s' %(wkfName,appendName))
production.setWorkflowDescription("Real data reconstruction production.")
production.setBKParameters('{{configName}}','{{configVersion}}','{{pDsc}}','{{simDesc}}')
production.setInputBKSelection(inputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

production.addBrunelStep("{{p1Ver}}",appType.lower(),"{{p1Opt}}",extraPackages='{{p1EP}}',eventType='{{eventType}}',inputData=[],inputDataType='mdf',outputSE=dataSE,histograms=True)
#Not sure if you want a finalization step...
production.addFinalizationStep()

production.setInputBKSelection(inputBKQuery)
production.setProdGroup('{{pDsc}}')
production.setFileMask("%s;root" %(appType.lower()))
production.setProdPriority(priority)
production.setProdPlugin(plugin)
production.setInputDataPolicy('download')

result = production.create(bkQuery=inputBKQuery,groupSize=1,derivedProduction=int(ancestorProd),bkScript=False,requestID=int('{{ID}}'),reqUsed=1)
if not result['OK']:
  print 'Error:',result['Message']

prodID = result['Value']
msg = 'Production %s successfully created' %prodID
diracProd = DiracProduction()
diracProd.production(prodID,'start',printOutput=True)
msg += ' and started in manual submission mode.'
print msg
