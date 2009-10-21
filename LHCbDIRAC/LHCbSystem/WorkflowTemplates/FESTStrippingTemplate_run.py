########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/WorkflowTemplates/FESTStrippingTemplate_run.py,v 1.1 2009/10/21 12:47:29 paterson Exp $
########################################################################

"""  The FEST Stripping template creates a workflow for Brunel & DaVinci with
     configurable number of events, CPU time, jobs to extend and priority.

     This does
"""

__RCSID__ = "$Id: FESTStrippingTemplate_run.py,v 1.1 2009/10/21 12:47:29 paterson Exp $"

import sys,os,string
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'FESTStrippingTemplate'

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
cpu = '{{MaxCPUTime#Max CPU time in secs#100000}}'
priority = '{{Priority#Production priority#7}}'
plugin = '{{PluginType#Production plugin name#Standard}}'
filesPerJob = '{{FilesPerJob#Group size or number of files per job#1}}'
appendName = '{{AppendName#String to append to production name#1}}'

dqFlag = '{{inDataQualityFlag}}'
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
production.setProdType('DataStripping')
wkfName='{{pDsc}}_{{eventType}}_{{p1App}}{{p1Ver}}_{{p2App}}{{p2Ver}}_DDDB{{p1DDDb}}_CondDB{{p1CDb}}_Request{{ID}}'
production.setWorkflowName(wkfName)
production.setWorkflowName('%s_%s' %(wkfName,appendName))

production.setWorkflowDescription("FEST data stripping production.")
production.setBKParameters('{{configName}}','{{configVersion}}','{{pDsc}}','{{simDesc}}')
production.setInputBKSelection(inputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

lfn = 'LFN:/lhcb/data/2009/RDST/00005447/0000/00005447_00000733_1.rdst'

production.addDaVinciStep("{{p1Ver}}","fetc","{{p1Opt}}",extraPackages='{{p1EP}}',eventType='{{eventType}}',histograms=False,numberOfEvents="-1",inputData=lfn,inputDataType='rdst',dataType='Data')
production.addBrunelStep("{{p2Ver}}","dst","{{p2Opt}}",extraPackages='{{p2EP}}',inputDataType='fetc',dataType='Data',histograms=False)
production.addDaVinciStep("{{p3Ver}}","dst","{{p3Opt}}",extraPackages='{{p3EP}}',inputDataType='dst',dataType='Data',histograms=True)
production.addFinalizationStep()

production.setProdPlugin(plugin)
production.setInputBKSelection(inputBKQuery)
production.setProdGroup('{{pDsc}}')
production.setProdPriority(priority)
production.setFileMask("fetc;dst;root")
#production.setAncestorDepth(2)
#temporary
production.setWorkflowLib('v9r27')


if not args:
  print 'No arguments specified, will create workflow only.'
  os.chdir(start)
  production.setWorkflowName(templateName)
  wf =  production.workflow.toXMLFile(templateName)
  print 'Created local workflow template: %s' %templateName
  sys.exit(0)

result = production.create(inputBKQuery,groupSize=filesPerJob,bkScript=True,requestID=int('{{ID}}'),reqUsed=0)
if not result['OK']:
  print 'Error:',result['Message']

prodID = result['Value']
msg = 'Production %s successfully created' %prodID
diracProd = DiracProduction()
production.setProdParameter(prodID,'AncestorDepth',2)
msg+=' AncestorDepth set to 2, '
diracProd.production(prodID,'start',printOutput=True)
msg += ' and started in manual submission mode.'
print msg