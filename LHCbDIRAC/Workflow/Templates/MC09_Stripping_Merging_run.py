########################################################################
# $HeadURL$
########################################################################

"""  The MC09 template creates a workflow for Gauss->Boole->Brunel with
     configurable number of events, CPU time, jobs to extend and priority.
     In addition this creates the necessary merging production.
"""

__RCSID__ = "$Id$"

import sys,os
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'MC09_Stripping_Merging_Template'

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
priority = '{{Priority#MC Production priority#9}}'
appendName = '{{AppendName#String to append to production name#1}}'

#Eventually this should be retrieved from the BK
fileTypes = {}
fileTypes['DSTAR.DST']=1
fileTypes['LAMBDA.DST']=5 #300MB files
fileTypes['BMUON.DST']=1
fileTypes['HADRON.DST']=1
fileTypes['JPSI.DST']=1


#Other parameters
evtType = '{{eventType}}'
inputProd  = '{{InputProduction}}'
inputProd  = int(inputProd)

for fileType,groupSize in fileTypes.items():
  bkPassDict = {}
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
  mergeProd.setWorkflowName('%s_Merging_{{pDsc}}_EventType%s_Prod%s_Files%sGB_Request{{ID}}_%s' %(fileType,evtType,inputProd,groupSize,appendName))
  mergeProd.setWorkflowDescription('MC09 workflow for merging outputs from a previous production.')
  mergeProd.setBKParameters('MC','MC09','{{pDsc}}','{{simDesc}}')
  mergeProd.setDBTags('{{p1CDb}}','{{p1DDDb}}')
  mergeProd.addMergeStep('{{p4Ver}}',optionsFile='$STDOPTS/PoolCopy.opts',eventType='{{eventType}}',inputDataType=fileType,inputProduction=inputProd,inputData=[],passDict=bkPassDict)
  mergeProd.addFinalizationStep(removeInputData=False)
  mergeProd.setInputBKSelection(inputBKQuery)
  mergeProd.setProdGroup('{{pDsc}}')
  #temporary setting
  mergeProd.setWorkflowLib('v9r26')
  #end temp setting
  mergeProd.setProdPriority(priority)
  mergeProd.setJobFileGroupSize(groupSize)
  mergeProd.setFileMask(fileType)
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

  diracProd = DiracProduction()
  prodID = result['Value']
  diracProd.production(prodID,'start',printOutput=True)
  print 'Production %s successfully created and started in manual submission mode.' %prodID