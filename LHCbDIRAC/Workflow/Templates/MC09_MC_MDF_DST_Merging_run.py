########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/LHCbSystem/WorkflowTemplates/MC09_MC_MDF_DST_Merging_run.py $
########################################################################

"""  The MC09 template creates a workflow for Gauss->Boole->Brunel with
     configurable number of events, CPU time, jobs to extend and priority.
     In addition this creates the necessary merging productions for MDF+DST.
"""

__RCSID__ = "$Id: MC09_MC_MDF_DST_Merging_run.py 18064 2009-11-05 19:40:01Z acasajus $"

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

from LHCbDIRAC.LHCbSystem.Client.DiracProduction import DiracProduction
from LHCbDIRAC.LHCbSystem.Client.Production import Production

#configurable parameters
events = '{{numberOfEvents#MC Number of events per job#500}}'
cpu = '{{MaxCPUTime#MC Max CPU time in secs#100000}}'
priority = '{{Priority#MC Production priority#4}}'
extend = '{{Extend#Extend MC production by this many jobs#100}}'
appendName = '{{AppendName#String to append to production name#1}}'

production = Production()
production.setProdType('MCSimulation')
wkfName = 'MC_{{simDesc}}_{{pDsc}}_EventType{{eventType}}_{{numberOfEvents}}Events_Request{{ID}}'
production.setWorkflowName('%s_%s' %(wkfName,appendName))
production.setWorkflowDescription('MC09 workflow for Gauss, Boole and Brunel.')
production.setBKParameters('MC','MC09','{{pDsc}}','{{simDesc}}')
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

production.addGaussStep('{{p1Ver}}','{{Generator}}',events,'{{p1Opt}}',eventType='{{eventType}}',extraPackages='{{p1EP}}')

booleOpts = 'Boole().Outputs  = ["DIGI","MDF"];'
booleOpts += """OutputStream("DigiWriter").Output = "DATAFILE='PFN:@{outputData}' TYP='POOL_ROOTTREE' OPT='RECREATE'";"""
booleOpts += """OutputStream("RawWriter").Output = "DATAFILE='PFN:@{STEP_ID}.mdf' SVC='LHCb::RawDataCnvSvc' OPT='REC'";"""
booleOpts += "MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC';"
booleOpts += 'HistogramPersistencySvc().OutputFile = "@{applicationName}_@{STEP_ID}_Hist.root"'
outputSE = 'CERN-RDST'
extra = {"outputDataName":"@{STEP_ID}.mdf","outputDataType":"mdf","outputDataSE":outputSE}

production.addBooleStep('{{p2Ver}}','digi','{{p2Opt}}',extraPackages='{{p2EP}}',overrideOpts=booleOpts,extraOutputFile=extra)
production.addBrunelStep('{{p3Ver}}','dst','{{p3Opt}}',extraPackages='{{p3EP}}',inputDataType='digi')
production.addFinalizationStep()

production.setCPUTime(cpu)
production.setProdGroup('{{pDsc}}')
production.setProdPriority(priority)
production.setOutputMode('Any')
production.setFileMask('dst;mdf')

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

mcProdID = prodID

############
#DST Merging
############

#Configurable parameters
numberOfFiles = '{{NumberInputFiles#Merge Number of input files to merge per job}}'
priority = '9'
fileType = 'DST'

#Other parameters
evtType = '{{eventType}}'
inputProd  = int(prodID)

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
mergeProd.setWorkflowName('%sMerging_{{pDsc}}_EventType%s_Prod%s_Files%s_Request{{ID}}' %(fileType,evtType,inputProd,numberOfFiles))
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

result = mergeProd.create(bkScript=False,requestID=int('{{ID}}'),reqUsed=1)
if not result['OK']:
  print 'Error:',result['Message']

prodID = result['Value']
diracProd.production(prodID,'start',printOutput=True)
print 'Production %s successfully created and started in manual submission mode.' %prodID


############
#MDF Merging
############

#Configurable parameters
priority = '9'
fileType = 'MDF'
mdfSE = 'CERN-RDST'

#Other parameters
evtType = '{{eventType}}'
inputProd  = int(mcProdID)
mergeFiles = '200'
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
mergeProd.setWorkflowName('%sMerging_{{pDsc}}_EventType%s_Prod%s_Files%s_Request{{ID}}' %(fileType,evtType,inputProd,mergeFiles))
mergeProd.setWorkflowDescription('MC09 workflow for merging outputs from a previous production.')
mergeProd.setBKParameters('MC','MC09','{{pDsc}}','{{simDesc}}')
mergeProd.setDBTags('{{p1CDb}}','{{p1DDDb}}')
mergeProd.addMergeStep('{{p4Ver}}',optionsFile='$STDOPTS/PoolCopy.opts',eventType='{{eventType}}',inputDataType=fileType,outputSE=mdfSE,inputProduction=inputProd,inputData=mid,passDict=bkPassDict)
mergeProd.addFinalizationStep(removeInputData=True)
mergeProd.setInputBKSelection(inputBKQuery)
mergeProd.setJobFileGroupSize(mergeFiles)
mergeProd.setProdGroup('{{pDsc}}')
mergeProd.setProdPriority(priority)
mergeProd.setFileMask(fileType)

result = mergeProd.create(bkScript=False,requestID=int('{{ID}}'),reqUsed=1)
if not result['OK']:
  print 'Error:',result['Message']

prodID = result['Value']
diracProd.production(prodID,'start',printOutput=True)
print 'Production %s successfully created and started in manual submission mode.' %prodID
