########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/LHCbSystem/WorkflowTemplates/MC09MergingTemplate_run.py $
########################################################################

"""  The MC09 merging template creates a workflow for merging with
     configurable number of files per job, priority and merging file
     type.  The input production is specified at creation time.
"""

__RCSID__ = "$Id: MC09MergingTemplate_run.py 18064 2009-11-05 19:40:01Z acasajus $"

import sys,os
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'MC09MergingTemplate'

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
numberOfFiles = '{{NumberInputFiles#Number of input files to merge per job}}'
priority = '{{Priority#Production priority (default 9)}}'
fileType = '{{FileType#File type to merge (default DST)}}'

#Other parameters
evtType = '{{eventType}}'
inputProd  = '{{InputProduction}}'

bkPassDict = {}
if args:
  evtType = int(evtType)
  inputProd = int(inputProd)
else:
  bkPassDict = {'Step3': {'ApplicationName': '{{p4App}}', 'ApplicationVersion': '{{p4Ver}}', 'ExtraPackages': '{{p4EP}}', 'DDDb': '@{DDDBTag}', 'OptionFiles': '{{p4Opt}}', 'CondDb': '@{CondDBTag}'}, 'Step2': {'ApplicationName': '{{p3App}}', 'ApplicationVersion': '{{p3Ver}}', 'ExtraPackages': '{{p3EP}}', 'DDDb': '@{DDDBTag}', 'OptionFiles': '{{p3Opt}}', 'CondDb': '@{CondDBTag}'}, 'Step1': {'ApplicationName': '{{p2App}}', 'ApplicationVersion': '{{p2Ver}}', 'ExtraPackages': '{{p2EP}}', 'DDDb': '@{DDDBTag}', 'OptionFiles': '{{p2Opt}}', 'CondDb': '@{CondDBTag}'}, 'Step0': {'ApplicationName': '{{p1App}}', 'ApplicationVersion': '{{p1Ver}}', 'ExtraPackages': '{{p1EP}}', 'DDDb': '@{DDDBTag}', 'OptionFiles': '{{p1Opt}}', 'CondDb': '@{CondDBTag}'}}

inputBKQuery = { 'SimulationConditions'     : 'All',
                 'DataTakingConditions'     : 'All',
                 'ProcessingPass'           : 'All',
                 'FileType'                 : fileType,
                 'EventType'                : evtType,
                 'ConfigName'               : 'All',
                 'ConfigVersion'            : 'All',
                 'ProductionID'             : inputProd,
                 'DataQualityFlag'          : 'All'}

production = Production()
production.setProdType('Merge')
production.setWorkflowName('{{pDsc}}_EventType%s_Prod%s_Files%s_Merging%s_Request{{ID}}' %(evtType,inputProd,numberOfFiles,fileType))
production.setWorkflowDescription('MC09 workflow for merging outputs from a previous production.')
production.setBKParameters('MC','MC09','{{pDsc}}','{{simDesc}}')
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')
production.addMergeStep('{{p4Ver}}',optionsFile='$STDOPTS/PoolCopy.opts',eventType='{{eventType}}',inputDataType=fileType,inputProduction='{{InputProduction}}',inputData=[],passDict=bkPassDict)
production.addFinalizationStep(removeInputData=True)
production.setInputBKSelection(inputBKQuery)
production.setJobFileGroupSize(numberOfFiles)
production.setProdGroup('{{pDsc}}')
production.setProdPriority(priority)
production.setFileMask(fileType)

if not args:
  print 'No arguments specified, will create workflow only.'
  os.chdir(start)
  production.setWorkflowName(templateName)
  wf =  production.workflow.toXMLFile(templateName)
  print 'Created local workflow template: %s' %templateName
  sys.exit(0)

result = production.create(bkScript=False,requestID=int('{{ID}}'),reqUsed=1)
if not result['OK']:
  print 'Error:',result['Message']

prodID = result['Value']
diracProd = DiracProduction()
diracProd.production(prodID,'start',printOutput=True)
print 'Production %s successfully created and started in manual submission mode.' %prodID