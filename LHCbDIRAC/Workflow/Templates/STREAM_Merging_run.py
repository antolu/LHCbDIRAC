########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Workflow/Templates/STREAM_Merging_run.py $
########################################################################

"""  The stream merging template allows merging using DaVinci for stream files. 
"""

__RCSID__ = "$Id: STREAM_Merging_run.py 18813 2009-12-01 14:46:33Z paterson $"

import sys,os,string,re
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'STREAM_Merging_Template'

####################### BLACK MAGIC TO SET UP PROXY CONTAINED WITHIN #########################
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
localCfg = LocalConfiguration()
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate","yes" )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  print "There were errors when loading configuration",resultDict[ 'Message' ]
  sys.exit(1)
###############################################################################

from DIRAC import gConfig
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.Interfaces.API.Production import Production

#Configurable parameters
cpu = '{{MaxCPUTime#Workflow Max CPU time in secs#300000}}'
priority = '{{Priority#Workflow production priority#9}}'
appendName = '{{WorkflowAppendName#Workflow string to append to production name#1}}'
mergingPlugin = '{{MergingPlugin#Merging plugin e.g. Standard, ByRunFileTypeSize#ByRunFileTypeSize}}'
mergingGroupSize = '{{MergingGroupSize#Merging Group Size (e.g. BySize = GB file size)#1}}'
removeInputDataFlag = '{{RemoveInputDataFlag#Workflow flag for removal of input data Boolean True/False#False}}'
inputProd = '{{inputProd#Workflow input production ID used to set full processing pass (MUST be set)#0}}'
transformationFlag = '{{TransformationEnable#Transformation enable flag Boolean True/False#True}}'

removeInputDataFlag = eval(removeInputDataFlag)
transformationFlag = eval(transformationFlag)
evtType = '{{eventType}}'
configName = '{{configName}}'
configVersion = '{{configVersion}}'

inputProd  = int(inputProd)
if not inputProd:
  print 'ERROR, input production must be set, currently this is "%s"' %(inputProd)
  sys.exit(2)

#Until the request page allows to select the file types these have to be "inferred" from all that are possible
fileTypes = gConfig.getValue('/Operations/Bookkeeping/FileTypes',[])
tmpTypes = []
#restrict to '.DST' file types:
for fType in fileTypes:
  if re.search('\.DST$',fType):
    tmpTypes.append(fType)
fileTypes = tmpTypes
print 'Until request interface updated the following BK types are manually added to the query:\n%s' %(string.join(fileTypes,', '))
#End

#For the initial test using 6394 and request 993
#fileTypes = ['RADIATIVE.DST']

diracProd = DiracProduction()

for fileType in fileTypes:

  inputBKQuery = { 'SimulationConditions'     : 'All',
                   'DataTakingConditions'     :'{{simDesc}}',
                   'ProcessingPass'           : '{{inProPass}}',
                   'FileType'                 : fileType,
                   'EventType'                : evtType,
                   'ConfigName'               : '{{configName}}',
                   'ConfigVersion'            : '{{configVersion}}',
                   'ProductionID'             : 0, #important that this isn't used as there can be several IDs (input prod is only for getting a compatible proc pass for the BK) 
                   'DataQualityFlag'          : 'All'}
  
  mergeProd = Production()
  mergeProd.setProdType('Merge')
  mergeProd.setWorkflowName('STREAM_Merging_Request{{ID}}_{{pDsc}}_EventType%s_Prod%s' %(evtType,inputProd))
  mergeProd.setWorkflowDescription('Steam merging workflow for outputs from a previous production.')
  mergeProd.setBKParameters('{{configName}}','{{configVersion}}','{{pDsc}}','{{simDesc}}')
  mergeProd.setDBTags('{{p1CDb}}','{{p1DDDb}}')
  
#  id=['/lhcb/data/2010/DST/00006394/0000/00006394_00000179_2.Radiative.dst','/lhcb/data/2010/DST/00006394/0000/00006394_00000988_2.Radiative.dst']
  id = []
  mergeProd.addDaVinciStep('{{p1Ver}}','merge','{{p1Opt}}',extraPackages='{{p1EP}}',eventType=evtType,inputDataType=fileType.lower(),inputProduction=inputProd,inputData=[])
  mergeProd.addDaVinciStep('{{p2Ver}}','setc','{{p2Opt}}',extraPackages='{{p2EP}}',inputDataType=fileType.lower())
  
  mergeProd.addFinalizationStep(removeInputData=removeInputDataFlag)
  mergeProd.setInputBKSelection(inputBKQuery)
  mergeProd.setProdGroup('{{pDsc}}')
  mergeProd.setProdPriority(priority)
  mergeProd.setJobFileGroupSize(mergingGroupSize)
  mergeProd.setFileMask('setc;%s' %(fileType.lower()))
  mergeProd.setProdPlugin(mergingPlugin)

#  mergeProd.runLocal()
#  print mergeProd.create(publish=False,transformation=False,bkScript=True)
#  sys.exit(0)

  result = mergeProd.create(bkScript=False,requestID=int('{{ID}}'),reqUsed=1,transformation=transformationFlag)
  if not result['OK']:
    print 'Error:',result['Message']
    sys.exit(2)

  prodID = result['Value']
  diracProd.production(prodID,'manual',printOutput=True)
  print 'Production %s for %s successfully created and started in manual submission mode.' %(prodID,fileType)