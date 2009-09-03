########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/WorkflowTemplates/MC09_MC_Merging_run.py,v 1.1 2009/09/03 08:07:16 paterson Exp $
########################################################################

"""  The MC09 template creates a workflow for Gauss->Boole->Brunel with
     configurable number of events, CPU time, jobs to extend and priority.
     In addition this creates the necessary merging production.
"""

__RCSID__ = "$Id: MC09_MC_Merging_run.py,v 1.1 2009/09/03 08:07:16 paterson Exp $"

import sys,os
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'MC09_MC_Merging_Template'

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

#configurable parameters
events = '{{numberOfEvents#MC Number of events per job (default 500)}}'
cpu = '{{MaxCPUTime#MC Max CPU time in secs (default 100000)}}'
priority = '{{Priority#MC Production priority (default 4)}}'
extend = '{{Extend#Extend MC production by this many jobs}}'

production = Production()
production.setProdType('MCSimulation')
production.setWorkflowName('{{simDesc}}_{{pDsc}}_EventType{{eventType}}_{{numberOfEvents}}Events_Request{{ID}}')
production.setWorkflowDescription('MC09 workflow for Gauss, Boole and Brunel.')
production.setBKParameters('MC','MC09','{{pDsc}}','{{simDesc}}')
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

production.addGaussStep('{{p1Ver}}','{{Generator}}',events,'{{p1Opt}}',eventType='{{eventType}}',extraPackages='{{p1EP}}')
production.addBooleStep('{{p2Ver}}','digi','{{p2Opt}}',extraPackages='{{p2EP}}')
production.addBrunelStep('{{p3Ver}}','dst','{{p3Opt}}',extraPackages='{{p3EP}}',inputDataType='digi')
production.addFinalizationStep()

production.setCPUTime(cpu)
production.setProdGroup('{{pDsc}}')
production.setProdPriority(priority)
production.setOutputMode('Any')
production.setFileMask('dst')

if not args:
  print 'No arguments specified, will create workflow only.'
  os.chdir(start)
  production.setWorkflowName(templateName)
  wf =  production.workflow.toXMLFile(templateName)
  print 'Created local workflow template: %s' %templateName
  sys.exit(0)

result = production.create(requestID=int('{{ID}}'),reqUsed=0)

if not result['OK']:
  print 'Error:',result['Message']
  sys.exit(2)

prodID = result['Value']
msg = 'Production %s successfully created' %prodID
diracProd = DiracProduction()
if extend:
  diracProd.extendProduction(prodID,extend,printOutput=True)
  msg += ', extended by %s jobs' %extend

diracProd.production(prodID,'start',printOutput=True)
msg += ' and started in manual submission mode.'
print msg


#Configurable parameters
numberOfFiles = '{{NumberInputFiles#Merge Number of input files to merge per job}}'
priority = '9'
fileType = '{{FileType#Merge File type to merge (default DST)}}'

#Other parameters
evtType = '{{eventType}}'
inputProd  = int(prodID)

bkPassDict = {}
#if args:
#  evtType = int(evtType)
#  inputProd = int(inputProd)
#else:
#  bkPassDict = {'Step3': {'ApplicationName': '{{p4App}}', 'ApplicationVersion': '{{p4Ver}}', 'ExtraPackages': '{{p4EP}}', 'DDDb': '@{DDDBTag}', 'OptionFiles': '{{p4Opt}}', 'CondDb': '@{CondDBTag}'}, 'Step2': {'ApplicationName': '{{p3App}}', 'ApplicationVersion': '{{p3Ver}}', 'ExtraPackages': '{{p3EP}}', 'DDDb': '@{DDDBTag}', 'OptionFiles': '{{p3Opt}}', 'CondDb': '@{CondDBTag}'}, 'Step1': {'ApplicationName': '{{p2App}}', 'ApplicationVersion': '{{p2Ver}}', 'ExtraPackages': '{{p2EP}}', 'DDDb': '@{DDDBTag}', 'OptionFiles': '{{p2Opt}}', 'CondDb': '@{CondDBTag}'}, 'Step0': {'ApplicationName': '{{p1App}}', 'ApplicationVersion': '{{p1Ver}}', 'ExtraPackages': '{{p1EP}}', 'DDDb': '@{DDDBTag}', 'OptionFiles': '{{p1Opt}}', 'CondDb': '@{CondDBTag}'}}

inputBKQuery = { 'SimulationConditions'     : 'All',
                 'DataTakingConditions'     : 'All',
                 'ProcessingPass'           : 'All',
                 'FileType'                 : fileType,
                 'EventType'                : evtType,
                 'ConfigName'               : 'All',
                 'ConfigVersion'            : 'All',
                 'ProductionID'             : inputProd,
                 'DataQualityFlag'          : 'All'}

mergeProd = Production()
mergeProd.setProdType('Merge')
mergeProd.setWorkflowName('{{pDsc}}_EventType%s_Prod%s_Files%s_Merging%s_Request{{ID}}' %(evtType,inputProd,numberOfFiles,fileType))
mergeProd.setWorkflowDescription('MC09 workflow for merging outputs from a previous production.')
mergeProd.setBKParameters('MC','MC09','{{pDsc}}','{{simDesc}}')
mergeProd.setDBTags('{{p1CDb}}','{{p1DDDb}}')
mergeProd.addMergeStep('{{p4Ver}}',optionsFile='$STDOPTS/PoolCopy.opts',eventType='{{eventType}}',inputDataType=fileType,inputProduction=inputProd,inputData=[],passDict=bkPassDict)
mergeProd.addFinalizationStep(removeInputData=True)
mergeProd.setInputBKSelection(inputBKQuery)
mergeProd.setJobFileGroupSize(numberOfFiles)
mergeProd.setProdGroup('{{pDsc}}')
mergeProd.setProdPriority(priority)
mergeProd.setFileMask(fileType)

#if not args:
#  print 'No arguments specified, will create workflow only.'
#  os.chdir(start)
#  mergeProd.setWorkflowName(templateName)
#  wf =  mergeProd.workflow.toXMLFile(templateName)
#  print 'Created local workflow template: %s' %templateName
#  sys.exit(0)

result = mergeProd.create(bkScript=False,requestID=int('{{ID}}'),reqUsed=1)
if not result['OK']:
  print 'Error:',result['Message']

prodID = result['Value']
diracProd.production(prodID,'start',printOutput=True)
print 'Production %s successfully created and started in manual submission mode.' %prodID
