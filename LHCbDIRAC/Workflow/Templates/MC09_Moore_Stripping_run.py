########################################################################
# $Id: /local/reps/dirac/DIRAC3/LHCbSystem/WorkflowTemplates/MC09_Stripping_run.py,v 1.3 2009/10/14 07:36:20 paterson Exp $
########################################################################

"""  The MC09 stripping template creates a workflow for Moore with
     configurable number of events, CPU time, jobs to extend and priority.

"""

__RCSID__ = "$Id: MC09_Stripping_run.py,v 1.3 2009/10/14 07:36:20 paterson Exp $"

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
filesPerJob = '{{FilesPerJob#Group size or number of files per job#50}}'
appendName = '{{AppendName#String to append to production name#1}}'
outputAppend = '{{OutputAppendName#Output file append name#L0Hlt1}}'
inProductionID='{{inProductionID}}'
evtType = '{{eventType}}'
inputBKQuery = { 'SimulationConditions'     : '{{simDesc}}',
                 'DataTakingConditions'     : 'All',
                 'ProcessingPass'           : '{{inProPass}}',
                 'FileType'                 : '{{inFileType}}',
                 'EventType'                : evtType,
                 'ConfigName'               : '{{configName}}',
                 'ConfigVersion'            : '{{configVersion}}',
                 'ProductionID'             : inProductionID,
                 'DataQualityFlag'          : 'All'}

production = Production()
production.setProdType('DataStripping')
wkfName='{{pDsc}}_{{eventType}}_{{p1App}}{{p1Ver}}_{{p2App}}{{p2Ver}}_DDDB{{p1DDDb}}_CondDB{{p1CDb}}_Request{{ID}}'
production.setWorkflowName(wkfName)
production.setWorkflowName('%s_%s' %(wkfName,appendName))
production.setWorkflowDescription("MC09 Moore data stripping production.")
production.setBKParameters('{{configName}}','{{configVersion}}','{{pDsc}}','{{simDesc}}')
production.setInputBKSelection(inputBKQuery)
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

lfn = 'LFN:/lhcb/MC/MC09/DST/00004838/0000/00004838_00000001_1.dst'

production.addMooreStep("{{p1Ver}}","dst","{{p1Opt}}",extraPackages='{{p1EP}}',eventType=evtType,numberOfEvents="-1",inputData=lfn,dataType='MC',outputAppendName=outputAppend)
production.addFinalizationStep()

production.setProdPlugin(plugin)
production.setInputBKSelection(inputBKQuery)
production.setProdGroup('{{pDsc}}')
production.setProdPriority(priority)
production.setFileMask("dst")

if not args:
  print 'No arguments specified, will create workflow only.'
  os.chdir(start)
  production.setWorkflowName(templateName)
  wf =  production.workflow.toXMLFile(templateName)
  print 'Created local workflow template: %s' %templateName
  sys.exit(0)

#result = production.create(inputBKQuery,groupSize=filesPerJob,bkScript=True,requestID=int('{{ID}}'),reqUsed=0)
result = production.create(inputBKQuery,groupSize=filesPerJob,bkScript=False,requestID=int('{{ID}}'),reqUsed=1)
if not result['OK']:
  print 'Error:',result['Message']
  sys.exit(2)

prodID = result['Value']
msg = 'Production %s successfully created' %prodID
diracProd = DiracProduction()
diracProd.production(prodID,'start',printOutput=True)
msg += ' and started in manual submission mode.'
print msg


#groupSize = '{{MergeSize#File size for merging jobs (in GB)#5}}'
#priority = '9'
#fileType = '{{FileType#File type to merge#L0HLT1.DST}}'
#
#
#inputBKQuery = { 'SimulationConditions'     : 'All',
#                 'DataTakingConditions'     : 'All',
#                 'ProcessingPass'           : 'All',
#                 'FileType'                 : fileType,
#                 'EventType'                : evtType,
#                 'ConfigName'               : 'All',
#                 'ConfigVersion'            : 'All',
#                 'ProductionID'             : prodID,
#                 'DataQualityFlag'          : 'All'}
#
#mergeProd = Production()
#mergeProd.setProdType('Merge')
#mergeProd.setWorkflowName('%s_Merging_{{pDsc}}_EventType%s_Prod%s_Files%sGB_Request{{ID}}_%s' %(fileType,evtType,prodID,groupSize,appendName))
#mergeProd.setWorkflowDescription('MC09 workflow for merging outputs from a previous production.')
#mergeProd.setBKParameters('MC','MC09','{{pDsc}}','{{simDesc}}')
#mergeProd.setDBTags('{{p1CDb}}','{{p1DDDb}}')
#mergeProd.addMergeStep('{{p4Ver}}',optionsFile='$STDOPTS/PoolCopy.opts',eventType='{{eventType}}',inputDataType=fileType,inputProduction=prodID,inputData=[],passDict={})
#mergeProd.addFinalizationStep(removeInputData=False)
#mergeProd.setInputBKSelection(inputBKQuery)
#mergeProd.setProdGroup('{{pDsc}}')
#mergeProd.setProdPriority(priority)
#mergeProd.setJobFileGroupSize(groupSize)
#mergeProd.setFileMask(fileType.lower())
#mergeProd.setProdPlugin('BySize')
#
#result = mergeProd.create(bkScript=False,requestID=int('{{ID}}'),reqUsed=1)
#if not result['OK']:
#  print 'Error:',result['Message']
#
#diracProd = DiracProduction()
#mergeID = result['Value']
#diracProd.production(mergeID,'start',printOutput=True)
#print 'Production %s successfully created and started in manual submission mode.' %mergeID
