####################################################################
#Updated FEST reconstruction workflow with new finalization modules
####################################################################

__RCSID__ = "$Id: FEST_Reconstruction.py,v 1.2 2009/03/05 13:28:10 paterson Exp $"

from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *
#same as RecoTemplate
# Variable which need to be set
attempt=1
prodID = '00099998'
jobID =  '00000001'
wkf_name = "FEST_Reconstruction_Full_%s" %(attempt)
eventTypeSignal = "90000000"
#90000000 -  Full Stream
#91000000 - Express Stream
numberOfEvents = -1 #90k / FEST file
Brunel_version = "v34r2"
DaVinci_version = "v22r1"
Brunel_AppConfig="AppConfig.v2r1"
Brunel_optfile = "$APPCONFIGOPTS/Brunel/FEST-200903.py;$APPCONFIGOPTS/UseOracle.py"
DaVinci_optfile = "$APPCONFIGOPTS/DaVinci/DVMonitorDst.py" #;$APPCONFIGOPTS/UseOracle.py"
system_os = "slc4_ia32_gcc34"
WorkflowLib_version = "wkf-v9r3"
priority = '8'
loglevel='debug'
maxcputime=300000
configName='Fest'
configVersion='Fest'
emailList = 'lhcb-datacrash@cern.ch'
dataType = 'DATA'
outputDataFileMask = "rdst;root"
outputMode = 'Local'
soft_package = 'Brunel.'+Brunel_version
soft_package+= ';'+Brunel_AppConfig
soft_package+= ';'+'DaVinci.'+DaVinci_version
extraPackages = Brunel_AppConfig
Brunel_SE = 'Tier1-RDST'
Hist_SE = 'CERN-HIST'

histogramName = '@{applicationName}_@{STEP_ID}_Hist.root'

#General options
evtOpt = ";LHCbApp().EvtMax = %s" %numberOfEvents

#dddbOpt = ";LHCbApp().DDDBtag = \"head-20081002\""
#conddbOpt = ";LHCbApp().CondDBtag = \"head-20081002\""

opt_brunel = "#include \"$BRUNELOPTS/SuppressWarnings.opts\""
#opt_brunel = opt_brunel+";#include \"$SQLDDDBROOT/options/SQLDDDB-Oracle.opts\""
opt_brunel = opt_brunel+";MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'"
opt_brunel = opt_brunel+";OutputStream(\"DstWriter\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\""
opt_brunel = opt_brunel+";HistogramPersistencySvc().OutputFile = \"%s\"" %(histogramName)
opt_brunel = opt_brunel+ evtOpt
#opt_brunel = opt_brunel+ dddbOpt
#opt_brunel = opt_brunel+ conddbOpt

opt_davinci = "MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'"
opt_davinci += ';from DaVinci.Configuration import *'
opt_davinci += ";DaVinci().HistogramFile = \"%s\"" %(histogramName)
opt_davinci += ';DaVinci().EvtMax = %s' %(numberOfEvents)

#indata = "LFN:/lhcb/data/2009/RAW/FULL/FEST/FEST/43041/043041_0000000002.raw"
indata="LFN:/lhcb/data/2009/RAW/EXPRESS/FEST/FEST/44878/044878_0000000002.raw"

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
#Brunel step
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
gaudiAppDefn.addParameter(Parameter("extraPackages","","string","","",True,False,"Extra software packages to setup"))

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
workflow.setDescription('FEST Reconstruction')
workflow.addStep(gaudiAppDefn)

brunelStep = workflow.createStepInstance('Gaudi_App_Step', 'brunel')
brunelStep.setValue("numberOfEvents", numberOfEvents)
brunelStep.setValue("eventType", eventTypeSignal)
brunelStep.setValue("inputDataType","MDF")
brunelStep.setValue("applicationName", "Brunel")
brunelStep.setValue("applicationVersion", Brunel_version)
brunelStep.setValue("applicationType", "rdst")
brunelStep.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
brunelStep.setValue("outputData","@{STEP_ID}.@{applicationType}")
brunelStep.setValue("optionsFile", Brunel_optfile)
brunelStep.setValue("optionsLine",opt_brunel)
brunelStep.setValue("extraPackages",extraPackages)
brunelStep.setValue("optionsLinePrev","None")
brunelStep.setLink("inputData","self","InputData")
list1_out=[]
list1_out.append({"outputDataName":"@{STEP_ID}.@{applicationType}","outputDataType":"@{applicationType}","outputDataSE":Brunel_SE})
list1_out.append({"outputDataName":histogramName,"outputDataType":"HIST","outputDataSE":Hist_SE})
brunelStep.setValue("listoutput",list1_out)

davinciStep = workflow.createStepInstance('Gaudi_App_Step', 'davinci')
davinciStep.addParameter(Parameter("inputData","","string","","",True,False,"InputData"))
davinciStep.setLink("inputData",brunelStep.getName(),"outputData")
davinciStep.setValue("eventType", eventTypeSignal)
davinciStep.setValue("numberOfEvents", numberOfEvents)
davinciStep.setValue("inputDataType","RDST")
davinciStep.setValue("applicationName", "DaVinci")
davinciStep.setValue("applicationVersion", DaVinci_version)
davinciStep.setValue("applicationType", "dst")
davinciStep.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
davinciStep.setValue("outputData","@{STEP_ID}.@{applicationType}")
davinciStep.setValue("optionsFile", DaVinci_optfile)
davinciStep.setValue("optionsLine",opt_davinci)
davinciStep.setValue("optionsLinePrev","None")
davinciStep.setValue("extraPackages",extraPackages)
list2_out=[]
list2_out.append({"outputDataName":histogramName,"outputDataType":"HIST","outputDataSE":Hist_SE})
davinciStep.setValue("listoutput",list2_out)
workflow.addStep(finalization)
workflow.createStepInstance('Job_Finalization', 'finalization')

#################################################
#Parameters
#################################################

workflow.addParameter(Parameter("InputSandbox","LFN:/lhcb/applications/WorkflowLib-"+WorkflowLib_version+".tar.gz","JDL","","",True, False, "WorkflowLib to be used"))
workflow.addParameter(Parameter("InputData",indata,"JDL","","",True, False, "Application Name"))
#workflow.addParameter(Parameter("AncestorDepth","1","JDL","","",True,False, "Ancestor Depth"))
workflow.addParameter(Parameter("JobType","DataReconstruction","JDL","","",True, False, "Job TYpe"))
workflow.addParameter(Parameter("Owner","paterson","JDL","","",True, False, "user Name"))
workflow.addParameter(Parameter("StdError","std.err","JDL","","",True, False, "user Name"))
workflow.addParameter(Parameter("StdOutput","std.out","JDL","","",True, False, "user Name"))
workflow.addParameter(Parameter("OUTPUT_MAX","20","string","","",True,False,"nb max of output to keep"))
workflow.addParameter(Parameter("SystemConfig",system_os,"JDLReqt","","",True, False, "Application Name"))

# and finally we can unlink them because we inherit them linked
workflow.unlink(workflow.parameters)

workflow.addParameter(Parameter("PRODUCTION_ID",prodID,"string","","",True, False, "Temporary fix"))
workflow.addParameter(Parameter("JOB_ID",jobID,"string","","",True, False, "Temporary fix"))
workflow.addParameter(Parameter("sourceData",indata,"string","","",True, False, "Application Name"))
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
workflow.addParameter(Parameter("outputMode",outputMode,"string","","",True, False, "Only upload files with the extensions in this parameter"))

if os.path.exists('wkf_'+wkf_name+'.xml'):
  print 'Removed existing workflow'
  os.remove('wkf_'+wkf_name+'.xml')
workflow.toXMLFile('wkf_'+wkf_name+'.xml')
print workflow.createCode()
