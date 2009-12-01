########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Workflow/Templates/MC09_MC_MDF_DST_Merging_run.py $
########################################################################

"""  The FEST MC template creates MDF + DIGI output for Gauss and Boole steps.
"""

__RCSID__ = "$Id: MC09_MC_MDF_DST_Merging_run.py 18161 2009-11-11 12:07:09Z acasajus $"

import sys,os
start = os.getcwd()
os.chdir('/tmp')
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

templateName = 'FEST_MC_MDF_Template'

####################### BLACK MAGIC TO SET UP PROXY CONTAINED WITHIN #########################
from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration
localCfg = LocalConfiguration()
localCfg.addDefaultEntry( "/DIRAC/Security/UseServerCertificate","yes" )
resultDict = localCfg.loadUserData()
if not resultDict[ 'OK' ]:
  print "There were errors when loading configuration",resultDict[ 'Message' ]
  sys.exit(1)
###############################################################################

from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction
from LHCbDIRAC.Interfaces.API.Production import Production

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
production.setBKParameters('MC','Fest','{{pDsc}}','{{simDesc}}')
production.setDBTags('{{p1CDb}}','{{p1DDDb}}')

production.addGaussStep('{{p1Ver}}','{{Generator}}',events,'{{p1Opt}}',eventType='{{eventType}}',extraPackages='{{p1EP}}',outputSE='CERN-RDST')

booleOpts = 'Boole().Outputs  = ["DIGI","MDF"];'
booleOpts += """OutputStream("DigiWriter").Output = "DATAFILE='PFN:@{outputData}' TYP='POOL_ROOTTREE' OPT='RECREATE'";"""
booleOpts += """OutputStream("RawWriter").Output = "DATAFILE='PFN:@{STEP_ID}.mdf' SVC='LHCb::RawDataCnvSvc' OPT='REC'";"""
booleOpts += "MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC';"
booleOpts += 'HistogramPersistencySvc().OutputFile = "@{applicationName}_@{STEP_ID}_Hist.root"'
outputSE = 'CERN-RDST'
extra = {"outputDataName":"@{STEP_ID}.mdf","outputDataType":"mdf","outputDataSE":outputSE}

production.addBooleStep('{{p2Ver}}','digi','{{p2Opt}}',extraPackages='{{p2EP}}',overrideOpts=booleOpts,extraOutputFile=extra)
production.addFinalizationStep()

production.setCPUTime(cpu)
production.setProdGroup('{{pDsc}}')
production.setProdPriority(priority)
production.setOutputMode('Any')
production.setFileMask('digi;mdf')

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
