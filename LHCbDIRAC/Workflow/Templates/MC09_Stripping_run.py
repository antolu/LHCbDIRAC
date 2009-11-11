########################################################################
# $HeadURL$
########################################################################

"""  The MC09 stripping template creates a workflow for Brunel & DaVinci with
     configurable number of events, CPU time, jobs to extend and priority.

     Initial production / no BK to be sent

"""

__RCSID__ = "$Id$"

import sys,os,string
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'MC09_Stripping'

####################### BLACK MAGIC TO SET UP PROXY CONTAINED WITHIN #########################
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
localCfg = LocalConfiguration()
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate","yes" )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  print "There were errors when loading configuration",resultDict[ 'Message' ]
  sys.exit(1)
###############################################################################


from LHCbDIRAC.LHCbSystem.Client.DiracProduction import DiracProduction
from LHCbDIRAC.LHCbSystem.Client.Production import Production

#Configurable parameters
cpu = '{{MaxCPUTime#MC Max CPU time in secs#100000}}'
priority = '{{Priority#Production priority#7}}'
plugin = '{{PluginType#Production plugin name#Standard}}'
filesPerJob = '{{FilesPerJob#Group size or number of files per job#10}}'
appendName = '{{AppendName#String to append to production name#1}}'

inProductionID='{{inProductionID}}'

inputBKQuery = { 'SimulationConditions'     : '{{simDesc}}',
                 'DataTakingConditions'     : 'All',
                 'ProcessingPass'           : '{{inProPass}}',
                 'FileType'                 : '{{inFileType}}',
                 'EventType'                : '{{eventType}}',
                 'ConfigName'               : '{{configName}}',
                 'ConfigVersion'            : '{{configVersion}}',
                 'ProductionID'             : inProductionID,
                 'DataQualityFlag'          : 'All'}

production = Production()
production.setProdType('DataStripping')
wkfName='{{pDsc}}_{{eventType}}_{{p1App}}{{p1Ver}}_{{p2App}}{{p2Ver}}_DDDB{{p1DDDb}}_CondDB{{p1CDb}}_Request{{ID}}'
production.setWorkflowName(wkfName)
production.setWorkflowName('%s_%s' %(wkfName,appendName))
production.setWorkflowDescription("MC09 data stripping production.")
production.setBKParameters('{{configName}}','{{configVersion}}','{{pDsc}}','{{simDesc}}')
production.setInputBKSelection(inputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

lfn = 'LFN:/lhcb/MC/MC09/DST/00004838/0000/00004838_00000001_1.dst'

production.addDaVinciStep("{{p1Ver}}","fetc","{{p1Opt}}",extraPackages='{{p1EP}}',eventType='{{eventType}}',histograms=True,numberOfEvents="-1",inputData=lfn,dataType='MC')
production.addBrunelStep("{{p2Ver}}","dst","{{p2Opt}}",extraPackages='{{p2EP}}',inputDataType='fetc',dataType='MC',histograms=True)
production.addDaVinciStep("{{p3Ver}}","dst","{{p3Opt}}",extraPackages='{{p3EP}}',inputDataType='dst',dataType='MC',histograms=True)
production.addFinalizationStep()

production.setProdPlugin(plugin)
production.setInputBKSelection(inputBKQuery)
production.setProdGroup('{{pDsc}}')
production.setProdPriority(priority)
production.setFileMask("fetc;dst;root")

#temporary
production.setWorkflowLib('v9r26')


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
diracProd.production(prodID,'start',printOutput=True)
msg += ' and started in manual submission mode.'
print msg
