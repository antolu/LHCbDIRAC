
from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *

# Variable which need to be set
wkf_name = "MC_test"
#eventTypeSignal = "13144001"
eventTypeSignal = "10000000"
numberEventSignal = 2
numberEventMB = 2
numberEvent = -1
Gauss_version = "v31r0"
Gauss_optfile = "lhcb-200802.opts"
Boole_version = "v15r1"
Boole_optfile = "v200601.opts"
Brunel_version = "v32r7"
Brunel_optfile = "v200601.opts"
system_os = "slc4_ia32_gcc34"

opt_gauss = "#include \"$DECFILESROOT/options/@{eventType}.opts\""
opt_gauss = opt_gauss + ";GaussTape.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\""

opt_boole = "DigiWriter.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\""
opt_boole = opt_boole+ ";SpilloverSelector.Input = {\"DATAFILE=\'@{spilloverData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\",\"DATAFILE=\'@{pileupData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"}"

#opt_brunel = "#include \"$BRUNELOPTS/SuppressWarnings.opts\""
opt_brunel = "MessageSvc.Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc.timeFormat = '%Y-%m-%d %H:%M:%S UTC'"
opt_brunel = opt_brunel+";EventLoopMgr.OutputLevel = 3"
opt_brunel = opt_brunel+";DstWriter.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\""
opt_brunel = opt_brunel+";HistogramPersistencySvc.OutputFile = \"\""
opt_brunel = opt_brunel+";IODataManager.AgeLimit = 2"

#define Module 2
module2 = ModuleDefinition('GaudiApplication')#during constraction class creates duplicating copies of the params
module2.setDescription('Gaudi Application module')
module2.setBody('from WorkflowLib.Module.GaudiApplication import GaudiApplication\n')

#define module 3
module3 = ModuleDefinition('LogChecker')#during constraction class creates duplicating copies of the params
module3.setDescription('Check LogFile module')
module3.setBody('from WorkflowLib.Module.LogChecker import *\n')

#define module 4
module4 = ModuleDefinition('BookkeepingReport')
module4.setDescription('Bookkeeping Report module')
module4.setBody('from WorkflowLib.Module.BookkeepingReport import * \n')
module4.addParameter(Parameter("STEP_ID","","string","self","STEP_ID",True,False," step id "))

#define module 5
module5 = ModuleDefinition('StepFinalization')
module5.setDescription('Step Finalization module')
module5.setBody('from WorkflowLib.Module.StepFinalization import * \n')

#define module 6
module6 = ModuleDefinition('JobFinalization')
module6.setDescription('Job Finalization module')
module6.setBody('from WorkflowLib.Module.NewJobFinalization import * \n')

#define module 7
module7 = ModuleDefinition('Dummy')
module7.setDescription('Dummy module')
module7.setBody('from WorkflowLib.Module.Dummy import * \n')


###############   STEPS ##################################
#### step 1 we are creating step definition
step1 = StepDefinition('Gaudi_App_Step')
step1.addModule(module2) # Creating instance of the module 'Gaudi_App_Step'
moduleInstance2 = step1.createModuleInstance('GaudiApplication', 'module2')
##moduleInstance2 = step1.createModuleInstance('Dummy', 'module7')

step1.addModule(module3) # Creating instance of the module 'LogChecker'
moduleInstance3 = step1.createModuleInstance('LogChecker', 'module3')

step1.addModule(module4) # Creating instance of the module 'LogChecker'
moduleInstance4 = step1.createModuleInstance('BookkeepingReport', 'module4')

# now we can add parameters for the STEP but instead of typing them we can just use old one from modules
step1.addParameterLinked(module3.parameters)
step1.addParameterLinked(module2.parameters)

# and we can add additional parameter which will be used as a global
step1.addParameter(Parameter("eventType","","string","","",True, False, "Event Type"))
step1.addParameter(Parameter("outputData","","string","","",True,True,"etc name"))
step1.addParameter(Parameter("applicationName","","string","","",True, False, "Application Name"))
step1.addParameter(Parameter("applicationVersion","","string","","",True, False, "Application Name"))
step1.addParameter(Parameter("applicationType","","string","","",True, False, "Application Type"))
step1.addParameter(Parameter("applicationLog","","string","","",True,False,"name of the output file of the application"))
step1.addParameter(Parameter("optionsFile","","string","","",True,False,"Options File"))
step1.addParameter(Parameter("optionsLine","","string","","",True,False,"option to be added last","option"))
step1.addParameter(Parameter("optionsLinePrev","","string","","",True,False,"options to be added first","option"))
step1.addParameter(Parameter("numberOfEvents","","string","","",True,False,"number of events"))
step1.addParameter(Parameter("numberOfEventsInput","","string","","",True,False,"number of events as input"))
step1.addParameter(Parameter("numberOfEventsOutput","","string","","",True,False,"number of events as input"))
step1.addParameter(Parameter("inputDataType","","string","","",True, False, "Input Data Type"))
step1.addParameter(Parameter("listoutput",[],"list","","",True,False,"list of output data"))

step3 = StepDefinition('Job_Finalization')
step3.addModule(module6)
moduleInstance6 = step3.createModuleInstance('JobFinalization','module6')
step3.addParameterLinked(module6.parameters)

##############  WORKFLOW #################################
workflow1 = Workflow(name=wkf_name)
workflow1.setDescription('Workflow of GaudiApplication')

workflow1.addStep(step1)
step1_prefix="step1_"
stepInstance1 = workflow1.createStepInstance('Gaudi_App_Step', 'gauss')
stepInstance1.addParameter(Parameter("firstEventNumber",1,"int","","",True,False,"first event number"))
stepInstance1.setValue("eventType", eventTypeSignal)
stepInstance1.setValue("numberOfEvents", numberEventSignal)
stepInstance1.setValue("numberOfEventsInput", 0)
stepInstance1.setValue("numberOfEventsOutput", 0)
stepInstance1.setValue("inputDataType","None")
stepInstance1.setValue("applicationName", "Gauss")
stepInstance1.setValue("applicationVersion", Gauss_version)
stepInstance1.setValue("applicationType", "sim")
stepInstance1.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
stepInstance1.setValue("outputData","@{STEP_ID}.@{applicationType}")
stepInstance1.setValue("optionsFile", Gauss_optfile)
stepInstance1.setValue("optionsLine",opt_gauss)
stepInstance1.setValue("optionsLinePrev","None")
list1_out=[{"outputDataName":"@{STEP_ID}.@{applicationType}","outputType":"sim","outputDataSE":"Tier1_M-DST"}]
stepInstance1.setValue("listoutput",list1_out)

step11_prefix="step11_"
stepInstance11 = workflow1.createStepInstance('Gaudi_App_Step', 'gausspileup')
stepInstance11.addParameter(Parameter("firstEventNumber",1,"int","","",True,False,"first event number"))
stepInstance11.setValue("eventType", "30000000")
stepInstance11.setValue("numberOfEvents", numberEventMB)
stepInstance11.setValue("numberOfEventsInput", 0)
stepInstance11.setValue("numberOfEventsOutput", 0)
stepInstance11.setValue("inputDataType","None")
stepInstance11.setValue("applicationName", "Gauss")
stepInstance11.setValue("applicationVersion", Gauss_version)
stepInstance11.setValue("applicationType", "sim")
stepInstance11.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
stepInstance11.setValue("outputData","@{STEP_ID}.@{applicationType}")
stepInstance11.setValue("optionsFile", Gauss_optfile)
stepInstance11.setValue("optionsLine",opt_gauss)
stepInstance11.setValue("optionsLinePrev","None")
list11_out=[{"outputDataName":"@{STEP_ID}.@{applicationType}","outputType":"sim","outputDataSE":"Tier1_M-DST"}]
stepInstance11.setValue("listoutput",list11_out)

step12_prefix="step12_"
stepInstance12 = workflow1.createStepInstance('Gaudi_App_Step', 'gaussspillover')
stepInstance12.addParameter(Parameter("firstEventNumber",1,"int","","",True,False,"first event number"))
stepInstance12.setValue("eventType", "30000000")
stepInstance12.setValue("numberOfEvents", numberEventMB)
stepInstance12.setValue("numberOfEventsInput", 0)
stepInstance12.setValue("numberOfEventsOutput", 0)
stepInstance12.setValue("inputDataType","None")
stepInstance12.setValue("applicationName", "Gauss")
stepInstance12.setValue("applicationVersion", Gauss_version)
stepInstance12.setValue("applicationType", "sim")
stepInstance12.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
stepInstance12.setValue("outputData","@{STEP_ID}.@{applicationType}")
stepInstance12.setValue("optionsFile", Gauss_optfile)
stepInstance12.setValue("optionsLine",opt_gauss)
stepInstance12.setValue("optionsLinePrev","None")
list12_out=[{"outputDataName":"@{STEP_ID}.@{applicationType}","outputType":"sim","outputDataSE":"Tier1_M-DST"}]
stepInstance12.setValue("listoutput",list12_out)

step2_prefix="step2_"
stepInstance2 = workflow1.createStepInstance('Gaudi_App_Step', 'boole')
stepInstance2.addParameter(Parameter("inputData","","string","","",True,False,"InputData"))
stepInstance2.addParameter(Parameter("pileupData","","string","","",True,False,"InputData"))
stepInstance2.addParameter(Parameter("spilloverData","","string","","",True,False,"InputData"))
stepInstance2.setLink("inputData",stepInstance1.getName(),"outputData")
stepInstance2.setLink("pileupData",stepInstance11.getName(),"outputData")
stepInstance2.setLink("spilloverData",stepInstance12.getName(),"outputData")
stepInstance2.setValue("eventType", eventTypeSignal)
stepInstance2.setValue("numberOfEvents", numberEvent)
stepInstance2.setValue("inputDataType","SIM")
stepInstance2.setValue("applicationName", "Boole")
stepInstance2.setValue("applicationVersion", Boole_version)
stepInstance2.setValue("applicationType", "digi")
stepInstance2.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
stepInstance2.setValue("outputData","@{STEP_ID}.@{applicationType}")
stepInstance2.setValue("optionsFile", Boole_optfile)
stepInstance2.setValue("optionsLine",opt_boole)
stepInstance2.setValue("optionsLinePrev","None")
list2_out=[{"outputDataName":"@{STEP_ID}.@{applicationType}","outputType":"digi","outputDataSE":"Tier1_M-DST"}]
stepInstance2.setValue("listoutput",list2_out)

step3_prefix="step3_"
stepInstance3 = workflow1.createStepInstance('Gaudi_App_Step', 'brunel')
stepInstance3.setValue("eventType", eventTypeSignal)
stepInstance3.setValue("numberOfEvents", numberEvent)
stepInstance3.setValue("inputDataType","DIGI")
stepInstance3.setValue("applicationName", "Brunel")
stepInstance3.setValue("applicationVersion", Brunel_version)
stepInstance3.setValue("applicationType", "dst")
stepInstance3.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
stepInstance3.setValue("outputData","@{STEP_ID}.@{applicationType}")
stepInstance3.setValue("optionsFile", Brunel_optfile)
stepInstance3.setValue("optionsLine",opt_brunel)
stepInstance3.setValue("optionsLinePrev","None")
stepInstance3.addParameter(Parameter("inputData","","string","","",True,False,"InputData"))
stepInstance3.setLink("inputData",stepInstance2.getName(),"outputData")
list3_out=[{"outputDataName":"@{STEP_ID}.@{applicationType}","outputType":"dst","outputDataSE":"Tier1_M-DST"}]
stepInstance3.setValue("listoutput",list3_out)

workflow1.addStep(step3)
step4_prefix="step4_"
stepInstance4 = workflow1.createStepInstance('Job_Finalization', 'Step4')
# Now lets define parameters on the top
# lets specify parameters on the level of workflow

workflow1.addParameter(Parameter("JobType","test","JDL","","",True, False, "Job TYpe"))
workflow1.addParameter(Parameter("Owner","joel","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("StdError","std.err","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("StdOutput","std.out","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("OUTPUT_MAX","20","string","","",True,False,"nb max of output to keep"))

workflow1.addParameter(Parameter("MaxCPUTime",300000,"JDLReqt","","",True, False, "Application Name"))
workflow1.addParameter(Parameter("Platform","gLite","JDLReqt","","",True, False, "platform"))
workflow1.addParameter(Parameter("SystemConfig",system_os,"JDLReqt","","",True, False, "Application Name"))

# and finally we can unlink them because we inherit them linked
workflow1.unlink(workflow1.parameters)

workflow1.addParameter(Parameter("PRODUCTION_ID","00004044","string","","",True, False, "Temporary fix"))
workflow1.addParameter(Parameter("JOB_ID","00000101","string","","",True, False, "Temporary fix"))
workflow1.addParameter(Parameter("emailAddress","joel.closier@cern.ch","string","","",True, False, "Email to send a report from the LogCheck module"))
workflow1.addParameter(Parameter("dataType","MC","string","","",True, False, "type of Datatype"))
workflow1.addParameter(Parameter("poolXMLCatName","pool_xml_catalog.xml","string","","",True, False, "Application Name"))
workflow1.addParameter(Parameter("configName","MC","string","","",True, False, "Configuration Name"))
workflow1.addParameter(Parameter("configVersion","2008","string","","",True, False, "Configuration Version"))
workflow1.addParameter(Parameter("systemConfig",system_os,"string","","",True, False, "Application Name"))

if os.path.exists('wkf_MC.xml'):
  print 'Removed existing workflow'
  os.remove('wkf_MC.xml')
workflow1.toXMLFile('wkf_MC.xml')
#w4 = fromXMLFile("/afs/cern.ch/user/g/gkuznets/test1.xml")
#print 'Creating code for the workflow'
#print workflow1.showCode()
print workflow1.createCode()
#eval(compile(workflow1.createCode(),'<string>','exec'))
#workflow1.execute()
