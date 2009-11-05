########################################################################
# $HeadURL$
########################################################################

"""  The MDF merging template allows to disable the watchdog CPU check
     in the case that many input files per job are requested.
"""

__RCSID__ = "$Id$"

import sys,os
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'MC09_MC_MDF_DST_Merging_Template'

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
inputProd  = '{{InputProduction#Input production ID}}'
cpu = '{{MaxCPUTime#MC Max CPU time in secs#100000}}'
priority = '{{Priority#MC Production priority#9}}'
appendName = '{{AppendName#String to append to production name#1}}'
groupSize = '{{MergeSize#File size for merging jobs (in GB)#1}}'

evtType = '{{eventType}}'

############
#MDF Merging
############

#Configurable parameters
fileType = 'MDF'
mdfSE = 'CERN-RDST'

#Other parameters

bkPassDict = {}
mid = ['/lhcb/MC/MC09/MDF/00004811/0001/00004811_00012000_2.mdf']
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
mergeProd.setWorkflowName('%sMerging_{{pDsc}}_EventType%s_Prod%s_Files%sGB_Request{{ID}}_%s' %(fileType,evtType,inputProd,groupSize,appendName))
mergeProd.setWorkflowDescription('MC09 workflow for merging outputs from a previous production.')
mergeProd.setBKParameters('MC','MC09','{{pDsc}}','{{simDesc}}')
mergeProd.setDBTags('{{p1CDb}}','{{p1DDDb}}')
mergeProd.addMergeStep('{{p4Ver}}',optionsFile='$STDOPTS/PoolCopy.opts',eventType='{{eventType}}',inputDataType=fileType,outputSE=mdfSE,inputProduction=inputProd,inputData=mid,passDict=bkPassDict)
mergeProd.addFinalizationStep(removeInputData=True)
mergeProd.setInputBKSelection(inputBKQuery)
mergeProd.setJobFileGroupSize(groupSize)
mergeProd.setProdPlugin('BySize')
mergeProd.setProdGroup('{{pDsc}}')
mergeProd.setProdPriority(priority)
mergeProd.setFileMask(fileType)
mergeProd.disableCPUCheck()

result = mergeProd.create(bkScript=False,requestID=int('{{ID}}'),reqUsed=1)
if not result['OK']:
  print 'Error:',result['Message']

prodID = result['Value']
diracProd = DiracProduction()
diracProd.production(prodID,'start',printOutput=True)
print 'Production %s successfully created and started in manual submission mode.' %prodID