####################################################################
#FEST production workflow
####################################################################

__RCSID__ = "$Id: wkf_Fest.py,v 1.6 2009/02/18 19:58:53 paterson Exp $"

from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *


attempt=2
prodID = '00009999'
jobID = '00000111'
simDescription = 'Beam5TeV-VeloClosed-MagDown'
#simDescription = 'Beam7TeV-VeloClosed-MagDown'
wkf_name = "FEST_%s_%s" %(simDescription,attempt)
eventType = "30000000"
numberEventSignal = 2000 #was 4000 but trying to be quick
numberEvent = -1
generatorName = "Pythia"
Gauss_version = "v36r2" #
Gauss_optfile = "Gauss-2008.py;Beam5TeV-VeloClosed-MagDown.py;$DECFILESROOT/options/@{eventType}.opts;$LBPYTHIAROOT/options/Pythia.opts"
#Gauss_optfile = "Gauss-2008.py;Beam7TeV-VeloClosed-MagDown.py;$DECFILESROOT/options/@{eventType}.opts;$LBPYTHIAROOT/options/Pythia.opts"
Boole_version = "v17r2p1"
Boole_optfile = "Boole-FEST09.py"
system_os = "slc4_ia32_gcc34"
WorkflowLib_version = "wkf-v9r1"
priority = '8'
loglevel='verbose'
maxcputime=300000
configName='MC'
configVersion='2009'
emailList = 'lhcb-datacrash@cern.ch'
dataType = 'DATA'
outputDataFileMask = "mdf" #semicolon separated list if necessary
soft_package = "Boole."+Boole_version+";Gauss."+Gauss_version
extraPackages = '' #semicolon separated list if necessary
Gauss_SE = "CERN-disk"
Boole_SE = "CERN-disk"

#################################################
#Options
#################################################
dddbOpt = ";LHCbApp().DDDBtag = \"head-20090112\""
conddbOpt = ";LHCbApp().CondDBtag = \"sim-20090112\""
evtOptGauss = ";LHCbApp().EvtMax = %s" %numberEventSignal
evtOptBoole = ";LHCbApp().EvtMax = %s" %numberEvent

opt_gauss = ";MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'"
opt_gauss = opt_gauss + ";OutputStream(\"GaussTape\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\""
opt_gauss = opt_gauss+ dddbOpt
opt_gauss = opt_gauss+ conddbOpt
opt_gauss = opt_gauss+ evtOptGauss

opt_boole = ";OutputStream(\"RawWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' SVC=\'LHCb::RawDataCnvSvc\' OPT=\'RECREATE\'\""
opt_boole = opt_boole +";MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'"
opt_boole = opt_boole + ";OutputStream(\"RawWriter\").OutputLevel = INFO"
#opt_boole = opt_boole +";Boole().noWarnings = True"
opt_boole = opt_boole+ dddbOpt
opt_boole = opt_boole+ conddbOpt
opt_boole = opt_boole+ evtOptBoole

#################################################
#Module definitions
#################################################

gaudiApp = ModuleDefinition('GaudiApplication')
gaudiApp.setDescription('Gaudi Application module')
gaudiApp.setBody('from WorkflowLib.Module.GaudiApplication import GaudiApplication\n')

#define module 3
analyseLog = ModuleDefinition('LogChecker')
analyseLog.setDescription('Check LogFile module')
analyseLog.setBody('from WorkflowLib.Module.LogChecker import *\n')

#define module 4
genBKReport = ModuleDefinition('BookkeepingReport')
genBKReport.setDescription('Bookkeeping Report module')
genBKReport.setBody('from WorkflowLib.Module.BookkeepingReport import * \n')
genBKReport.addParameter(Parameter("STEP_ID","","string","self","STEP_ID",True,False," step id "))

#define module 5
finalizeStep = ModuleDefinition('StepFinalization')
finalizeStep.setDescription('Step Finalization module')
finalizeStep.setBody('from WorkflowLib.Module.StepFinalization import * \n')

#define module 6
sendBK = ModuleDefinition('SendBookkeeping')
sendBK.setDescription('Sends the BK reports')
sendBK.setBody('from WorkflowLib.Module.SendBookkeeping import SendBookkeeping \n')

dataUpload = ModuleDefinition('UploadOutputData')
dataUpload.setDescription('Uploads the output data')
dataUpload.setBody('from WorkflowLib.Module.UploadOutputData import UploadOutputData \n')

logUpload = ModuleDefinition('UploadLogFile')
logUpload.setDescription('Uploads the log files')
logUpload.setBody('from WorkflowLib.Module.UploadLogFile import UploadLogFile \n')

failoverRequest = ModuleDefinition('FailoverRequest')
failoverRequest.setDescription('Sends any failover requests')
failoverRequest.setBody('from WorkflowLib.Module.FailoverRequest import * \n')

#################################################
#Gaudi Application Step
#################################################
gaudiAppDefn = StepDefinition('Gaudi_App_Step')
gaudiAppDefn.addModule(gaudiApp)
gaudiAppDefn.createModuleInstance('GaudiApplication', 'gaudiApp')
gaudiAppDefn.addModule(analyseLog)
gaudiAppDefn.createModuleInstance('LogChecker', 'analyseLog')
gaudiAppDefn.addModule(genBKReport)
gaudiAppDefn.createModuleInstance('BookkeepingReport', 'genBKReport')
# now we can add parameters for the STEP but instead of typing them we can just use old one from modules
gaudiAppDefn.addParameterLinked(analyseLog.parameters)
gaudiAppDefn.addParameterLinked(gaudiApp.parameters)
# and we can add additional parameter which will be used as a global
gaudiAppDefn.addParameter(Parameter("eventType","","string","","",True, False, "Event Type"))
gaudiAppDefn.addParameter(Parameter("inputData","","string","","",True,False,"InputData"))
gaudiAppDefn.addParameter(Parameter("outputData","","string","","",True,False,"etc name"))
gaudiAppDefn.addParameter(Parameter("applicationName","","string","","",True, False, "Application Name"))
gaudiAppDefn.addParameter(Parameter("applicationVersion","","string","","",True, False, "Application Name"))
gaudiAppDefn.addParameter(Parameter("applicationType","","string","","",True, False, "Application Type"))
gaudiAppDefn.addParameter(Parameter("applicationLog","","string","","",True,False,"name of the output file of the application"))
gaudiAppDefn.addParameter(Parameter("optionsFile","","string","","",True,False,"Options File"))
gaudiAppDefn.addParameter(Parameter("optionsLine","","string","","",True,False,"option to be added last","option"))
gaudiAppDefn.addParameter(Parameter("optionsLinePrev","","string","","",True,False,"options to be added first","option"))
gaudiAppDefn.addParameter(Parameter("numberOfEvents","","string","","",True,False,"number of events"))
gaudiAppDefn.addParameter(Parameter("numberOfEventsInput","","string","","",True,False,"number of events as input"))
gaudiAppDefn.addParameter(Parameter("numberOfEventsOutput","","string","","",True,False,"number of events as input"))
gaudiAppDefn.addParameter(Parameter("inputDataType","","string","","",True, False, "Input Data Type"))
gaudiAppDefn.addParameter(Parameter("listoutput",[],"list","","",True,False,"list of output data"))

#################################################
#Finalization step
#################################################
finalization = StepDefinition('Job_Finalization')

finalization.addModule(sendBK)
finalization.createModuleInstance('SendBookkeeping','sendBK')
finalization.addParameterLinked(sendBK.parameters)

finalization.addModule(dataUpload)
finalization.createModuleInstance('UploadOutputData','dataUpload')
finalization.addParameterLinked(dataUpload.parameters)

finalization.addModule(logUpload)
finalization.createModuleInstance('UploadLogFile','logUpload')
finalization.addParameterLinked(logUpload.parameters)

finalization.addModule(failoverRequest)
finalization.createModuleInstance('FailoverRequest','failoverRequest')
finalization.addParameterLinked(failoverRequest.parameters)


#################################################
#Workflow
#################################################
workflow = Workflow(name=wkf_name)
workflow.setDescription('FEST Production')
workflow.addStep(gaudiAppDefn)

gaussStep = workflow.createStepInstance('Gaudi_App_Step', 'gauss')
gaussStep.addParameter(Parameter("firstEventNumber",1,"int","","",True,False,"first event number"))
gaussStep.setValue("eventType", eventType)
gaussStep.setValue("numberOfEvents", numberEventSignal)
gaussStep.setValue("numberOfEventsInput", 0)
gaussStep.setValue("numberOfEventsOutput", 0)
gaussStep.setValue("inputDataType","None")
gaussStep.setValue("generatorName", generatorName)
gaussStep.setValue("applicationName", "Gauss")
gaussStep.setValue("applicationVersion", Gauss_version)
gaussStep.setValue("applicationType", "sim")
gaussStep.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
gaussStep.setValue("outputData","@{STEP_ID}.@{applicationType}")
gaussStep.setValue("optionsFile", Gauss_optfile)
gaussStep.setValue("optionsLine",opt_gauss)
gaussStep.setValue("optionsLinePrev","None")
gaussStep.setValue("extraPackages",extraPackages)
list1_out=[{"outputDataName":"@{STEP_ID}.@{applicationType}","outputDataType":"SIM","outputDataSE":Gauss_SE}]
gaussStep.setValue("listoutput",list1_out)

booleStep = workflow.createStepInstance('Gaudi_App_Step', 'boole')
booleStep.addParameter(Parameter("inputData","","string","","",True,False,"InputData"))
booleStep.setLink("inputData",gaussStep.getName(),"outputData")
booleStep.setValue("eventType", eventType)
booleStep.setValue("numberOfEvents", numberEvent)
booleStep.setValue("inputDataType","SIM")
booleStep.setValue("applicationName", "Boole")
booleStep.setValue("applicationVersion", Boole_version)
booleStep.setValue("applicationType", "mdf")
booleStep.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
booleStep.setValue("outputData","@{STEP_ID}.@{applicationType}")
booleStep.setValue("optionsFile", Boole_optfile)
booleStep.setValue("optionsLine",opt_boole)
booleStep.setValue("optionsLinePrev","None")
booleStep.setValue("extraPackages",extraPackages)
list2_out=[{"outputDataName":"@{STEP_ID}.@{applicationType}","outputDataType":"MDF","outputDataSE":Boole_SE}]
booleStep.setValue("listoutput",list2_out)


workflow.addStep(finalization)
workflow.createStepInstance('Job_Finalization', 'finalization')

workflow.addParameter(Parameter("InputSandbox","LFN:/lhcb/applications/WorkflowLib-"+WorkflowLib_version+".tar.gz","JDL","","",True, False, "WorkflowLib to be used"))
workflow.addParameter(Parameter("BannedSites","LCG.CERN.ch;LCG.CNAF.it;LCG.RAL.uk;LCG.PIC.es;LCG.IN2P3.fr;LCG.NIKHEF.nl;LCG.GRIDKA.de","JDL","","",True, False, "user Name"))
workflow.addParameter(Parameter("JobType","MCSimulation","JDL","","",True, False, "Job TYpe"))
workflow.addParameter(Parameter("Owner","paterson","JDL","","",True, False, "user Name"))
workflow.addParameter(Parameter("StdError","std.err","JDL","","",True, False, "user Name"))
workflow.addParameter(Parameter("StdOutput","std.out","JDL","","",True, False, "user Name"))
workflow.addParameter(Parameter("OUTPUT_MAX","20","string","","",True,False,"nb max of output to keep"))
workflow.addParameter(Parameter("SystemConfig",system_os,"JDLReqt","","",True, False, "Application Name"))

# and finally we can unlink them because we inherit them linked
workflow.unlink(workflow.parameters)

workflow.addParameter(Parameter("PRODUCTION_ID",prodID,"string","","",True, False, "Temporary fix"))
workflow.addParameter(Parameter("JOB_ID",jobID,"string","","",True, False, "Temporary fix"))
workflow.addParameter(Parameter("poolXMLCatName","pool_xml_catalog.xml","string","","",True, False, "Application Name"))
workflow.addParameter(Parameter("configName",configName,"string","","",True, False, "Configuration Name"))
workflow.addParameter(Parameter("configVersion",configVersion,"string","","",True, False, "Configuration Version"))
workflow.addParameter(Parameter("Priority",priority,"JDL","","",True,False,"Priority"))
workflow.addParameter(Parameter("LogLevel",loglevel,'JDL',"","",True,False,'LogLevel'))
workflow.addParameter(Parameter("MaxCPUTime",maxcputime,"JDLReqt","","",True, False, "Max CPU time to be used"))
workflow.addParameter(Parameter("SoftwarePackages",soft_package,"JDL","","",True,False,"soft to be used"))
workflow.addParameter(Parameter("emailAddress",emailList,"string","","",True, False, "Email to send a report from the LogCheck module"))
workflow.addParameter(Parameter("dataType",dataType,"string","","",True, False, "type of Datatype"))
workflow.addParameter(Parameter("outputDataFileMask",outputDataFileMask,"string","","",True, False, "Only upload files with the extensions in this parameter"))

if os.path.exists(wkf_name+'.xml'):
  print 'Removed existing workflow'
  os.remove(wkf_name+'.xml')
workflow.toXMLFile(wkf_name+'.xml')
print workflow.createCode()