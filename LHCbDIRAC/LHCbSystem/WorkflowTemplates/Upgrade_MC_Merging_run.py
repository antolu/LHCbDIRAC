########################################################################
# $HeadURL$
########################################################################

"""  The Upgrade template creates a workflow for Gauss->Boole->Brunel with
     configurable number of events, CPU time, jobs to extend and priority.

     Unlike MC09, the upgrade template explicitly forces the tags of the
     CondDB / DDDB to be set at each step.
"""

__RCSID__ = "$Id$"

import sys,os
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'Upgrade_MC_Merging_Template'

####################### BLACK MAGIC TO SET UP PROXY CONTAINED WITHIN #########################
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
localCfg = LocalConfiguration()
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate","yes" )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  print "There were errors when loading configuration",resultDict[ 'Message' ]
  sys.exit(1)
###############################################################################

os.system('hostname')

from DIRAC.Interfaces.API.DiracProduction import DiracProduction
try:
  from LHCbSystem.Client.Production import Production
except:
  from DIRAC.LHCbSystem.Client.Production import Production

#configurable parameters
events = '{{numberOfEvents#MC Number of events per job#500}}'
cpu = '{{MaxCPUTime#MC Max CPU time in secs#100000}}'
priority = '{{Priority#MC Production priority#4}}'
extend = '{{Extend#Extend MC production by this many jobs#100}}'
appendName = '{{AppendName#String to append to production name#1}}'
finalAppType = '{{FinalAppType#Brunel file type to produce and merge e.g. DST/XDST#DST}}'

production = Production()
production.setProdType('MCSimulation')
wkfName = 'MC_{{simDesc}}_{{pDsc}}_EventType{{eventType}}_{{numberOfEvents}}Events_Request{{ID}}'
production.setWorkflowName('%s_%s' %(wkfName,appendName))
production.setWorkflowDescription('Upgrade workflow for Gauss, Boole and Brunel.')
production.setBKParameters('MC','Upgrade','{{pDsc}}','{{simDesc}}')
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

production.addGaussStep('{{p1Ver}}','{{Generator}}',events,'{{p1Opt}}',eventType='{{eventType}}',extraPackages='{{p1EP}}',condDBTag='{{p1CDb}}',ddDBTag='{{p1DDDb}}')
production.addBooleStep('{{p2Ver}}','digi','{{p2Opt}}',extraPackages='{{p2EP}}',condDBTag='{{p2CDb}}',ddDBTag='{{p2DDDb}}')
#production.addBrunelStep('{{p3Ver}}','dst','{{p3Opt}}',extraPackages='{{p3EP}}',inputDataType='digi',condDBTag='{{p3CDb}}',ddDBTag='{{p3DDDb}}')
production.addBrunelStep('{{p3Ver}}',finalAppType.lower(),'{{p3Opt}}',extraPackages='{{p3EP}}',inputDataType='digi',outputSE='CERN_MC_M-DST',condDBTag='{{p3CDb}}',ddDBTag='{{p3DDDb}}')

production.addFinalizationStep()

production.setCPUTime(cpu)
production.setProdGroup('{{pDsc}}')
production.setProdPriority(priority)
production.setOutputMode('Any')
production.setFileMask(finalAppType.lower())

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
groupSize = '{{MergeSize#File size for merging jobs (in GB)#5}}'
priority = '9'

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
                 'FileType'                 : finalAppType,
                 'EventType'                : evtType,
                 'ConfigName'               : 'All',
                 'ConfigVersion'            : 'All',
                 'ProductionID'             : inputProd,
                 'DataQualityFlag'          : 'All'}

mergeProd = Production()
mergeProd.setProdType('Merge')
mergeProd.setWorkflowName('%sMerging_{{pDsc}}_EventType%s_Prod%s_Files%sGB_Request{{ID}}' %(finalAppType,evtType,inputProd,groupSize))
mergeProd.setWorkflowDescription('Upgrade workflow for merging outputs from a previous production.')
mergeProd.setBKParameters('MC','Upgrade','{{pDsc}}','{{simDesc}}')
mergeProd.setDBTags('{{p1CDb}}','{{p1DDDb}}')
mergeProd.addMergeStep('{{p4Ver}}',optionsFile='$STDOPTS/PoolCopy.opts',eventType='{{eventType}}',inputDataType=finalAppType.lower(),inputProduction=inputProd,inputData=[],passDict=bkPassDict,condDBTag='{{p4CDb}}',ddDBTag='{{p4DDDb}}')
mergeProd.addFinalizationStep(removeInputData=True)
mergeProd.setInputBKSelection(inputBKQuery)
mergeProd.setJobFileGroupSize(groupSize)
mergeProd.setProdGroup('{{pDsc}}')
mergeProd.setProdPriority(priority)
mergeProd.setFileMask(finalAppType.lower())
mergeProd.setProdPlugin('BySize')

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
