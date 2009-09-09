########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/WorkflowTemplates/FESTRecoTemplate_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $
########################################################################

"""  The FEST Reco template creates a workflow for Brunel & DaVinci with
     configurable number of events, CPU time, jobs to extend and priority.
"""

__RCSID__ = "$Id: FESTRecoTemplate_run.py,v 1.3 2009/09/09 08:57:06 paterson Exp $"

import sys,os,string
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'FESTRecoTemplate'

####################### BLACK MAGIC TO SET UP PROXY CONTAINED WITHIN #########################
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
localCfg = LocalConfiguration()
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate","yes" )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  print "There were errors when loading configuration",resultDict[ 'Message' ]
  sys.exit(1)
###############################################################################


from DIRAC.Interfaces.API.DiracProduction import DiracProduction
try:
  from LHCbSystem.Client.Production import Production
except:
  from DIRAC.LHCbSystem.Client.Production import Production

#Configurable parameters
priority = '{{Priority#Production priority (default 7)}}'
plugin = '{{PluginType#Production plugin name (default CCRC_RAW)}}'
ancestorProd = '{{AncestorProd#Ancestor production if any (default 0)}}'
dataSE = '{{DataSE#Output Data Storage Element (default Tier1-RDST)}}'

#Other parameters
dqDict = {}
dqDict['ok']='FULL'
dqDict['maybe']='REPROCESSING'
dqDict['unchecked']='EXPRESS'
dqDict['bad']='BAD'
dqDict['all']='ALL'

dqFlag = '{{inDataQualityFlag}}'
recoType='RECO'
inProductionID='{{inProductionID}}'
if args:
  inProductionID = int(inProductionID)
  recoType=dqDict[dqFlag.lower()]

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
wkfName='{{pDsc}}_{{eventType}}_{{p1App}}{{p1Ver}}_{{p2App}}{{p2Ver}}_DDDB{{p1DDDb}}_CondDB{{p1CDb}}_Request{{ID}}'
production.setWorkflowName('%s_%s' %(recoType,wkfName))
production.setWorkflowDescription("FEST data reconstruction production.")
production.setBKParameters('{{configName}}','{{configVersion}}','{{pDsc}}','{{simDesc}}')
production.setInputBKSelection(inputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

production.addBrunelStep("{{p1Ver}}","rdst","{{p1Opt}}",extraPackages='{{p1EP}}',eventType='{{eventType}}',inputData=[],inputDataType='mdf',outputSE=dataSE,histograms=True)
production.addDaVinciStep("{{p2Ver}}","dst","{{p2Opt}}",extraPackages='{{p2EP}}',histograms=True)
production.addFinalizationStep()

production.setProdPlugin(plugin)
production.setInputBKSelection(inputBKQuery)
production.setProdGroup('{{pDsc}}')
production.setFileMask("rdst;root")

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
