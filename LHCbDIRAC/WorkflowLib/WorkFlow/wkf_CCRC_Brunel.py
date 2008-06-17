from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *


##########  PART TO EDIT  ##############
wkf_name = "CCRC_Reconstruction_test"
nb_evt_step1 = 25000
Brunel_version = "v32r5"
opt_brunel = "#include \"$BRUNELOPTS/SuppressWarnings.opts\""
opt_brunel = opt_brunel+";MessageSvc.Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc.timeFormat = '%Y-%m-%d %H:%M:%S UTC'"
opt_brunel = opt_brunel+";EventLoopMgr.OutputLevel = 3"
opt_brunel = opt_brunel+";DstWriter.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\" "
opt_brunel = opt_brunel+";HistogramPersistencySvc.OutputFile = \"\""
##########   DO NOT EDIT AFTER THIS LINE #############
#indata = "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_1.sim;LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_2.sim;LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_3.sim"
indata = "LFN:/lhcb/production/CCRC08/v0/00002090/RAW/0000/00002090_00002534_1.raw"
#indata = "LFN:/lhcb/data/CCRC08/RAW/LHCb/CCRC/402154/402154_0000047096.raw;LFN:/lhcb/data/CCRC08/RAW/LHCb/CCRC/402154/402154_0000047097.raw"


#define Module 2
module2 = ModuleDefinition('GaudiApplication')#during constraction class creates duplicating copies of the params
module2.setDescription('Gaudi Application module')
module2.setBody('from WorkflowLib.Module.GaudiApplication import GaudiApplication\n')

# we add empty parameters but linked up as default
module2.addParameter(Parameter("DataType","","string","self","DataType",True, False, "data type"))
module2.addParameter(Parameter("CONFIG_NAME","","string","self","CONFIG_NAME",True, False, "Configuration Name"))
module2.addParameter(Parameter("CONFIG_VERSION","","string","self","CONFIG_VERSION",True, False, "Configuration Version"))
module2.addParameter(Parameter("EVENTTYPE","","string","self","EVENTTYPE",True, False, "Event Type"))
module2.addParameter(Parameter("appName","","string","self","appName",True, False, "Application Name"))
module2.addParameter(Parameter("appVersion","","string","self","appVersion",True, False, "Application Version"))
module2.addParameter(Parameter("appType","","string","self","appType",True,False,"Application Version"))
module2.addParameter(Parameter("appLog","","string","self","appLog",True,False,"list of logfile"))
module2.addParameter(Parameter("inputData","","jdl","self","inputData",True, False, "List of InputData"))
module2.addParameter(Parameter("inputDataType","","string","self","inputDataType",True, False, "Input Data Type"))
module2.addParameter(Parameter("nb_events_input","","string","self","nb_events_input",True,False,"number of events as input"))
module2.addParameter(Parameter("NUMBER_OF_EVENTS","","string","self","NUMBER_OF_EVENTS",True, False, "number of events requested"))
module2.addParameter(Parameter("optionsFile","","string","self","optionsFile",True,False,"Options File"))
module2.addParameter(Parameter("optionsLine","","string","self","optionsLine",True,False,"Number of Event","option"))
module2.addParameter(Parameter("systemConfig","","string","self","systemConfig",True,False,"Job Platform"))
module2.addParameter(Parameter("poolXMLCatName","","string","self","poolXMLCatName",True,False,"POOL XML slice"))
#module2.addParameter(Parameter("outputData",'',"string","self","outputData",True,False,"list of output data"))

#define module 3
module3 = ModuleDefinition('LogChecker')#during constraction class creates duplicating copies of the params
module3.setDescription('Check LogFile module')
module3.setBody('from WorkflowLib.Module.LogChecker import *\n')

# we add parameters and link them to the level of step
module3.addParameter(Parameter("DataType","","string","self","DataType",True, False, "data type"))
module3.addParameter(Parameter("SourceData","","string","self","SourceData",True,False,"InputData"))
module3.addParameter(Parameter("CONFIG_NAME","","string","self","CONFIG_NAME",True, False, "Configuration Name"))
module3.addParameter(Parameter("CONFIG_VERSION","","string","self","CONFIG_VERSION",True, False, "Configuration Version"))
module3.addParameter(Parameter("NUMBER_OF_EVENTS","","string","self","NUMBER_OF_EVENTS",True, False, "number of events requested"))
module3.addParameter(Parameter("NUMBER_OF_EVENTS_INPUT","","string","","",True,True,"number of events as input"))
module3.addParameter(Parameter("NUMBER_OF_EVENTS_OUTPUT","","string","","",False,True,"number of events as input"))
module3.addParameter(Parameter("nb_events_input","","string","self","nb_events_input",True,False,"number of events as input"))
module3.addParameter(Parameter("appName","","string","self","appName",True, False, "Application Name"))
module3.addParameter(Parameter("appVersion","","string","self","appVersion",True, False, "Application Version"))
module3.addParameter(Parameter("appType","","string","self","appType",True,False,"Application Version"))
module3.addParameter(Parameter("appLog","","string","self","appLog",True,False,"list of logfile"))
module3.addParameter(Parameter("poolXMLCatName","","string","self","poolXMLCatName",True,False,"POOL XML slice"))
module3.addParameter(Parameter("inputData","","string","self","inputData",True,False,"InputData"))
module3.addParameter(Parameter("OUTPUT_MAX","","string","self","OUTPUT_MAX",True,False,"nb max of output to keep"))
module3.addParameter(Parameter("EMAIL","@{EMAILNAME}","string","","",True,False,"EMAIL adress"))


#define module 4
module4 = ModuleDefinition('BookkeepingReport')
module4.setDescription('Bookkeeping Report module')
module4.setBody('from WorkflowLib.Module.BookkeepingReport import * \n')
module4.addParameter(Parameter("STEP_ID","","string","self","STEP_ID",True,False,"EMAIL adress"))
module4.addParameter(Parameter("nb_events_input","","string","self","nb_events_input",True,False,"number of events as input"))
module4.addParameter(Parameter("NUMBER_OF_EVENTS","","string","self","NUMBER_OF_EVENTS",True, False, "number of events requested"))
module4.addParameter(Parameter("NUMBER_OF_EVENTS_INPUT","","string","","",True,False,"number of events as input"))
module4.addParameter(Parameter("NUMBER_OF_EVENTS_OUTPUT","","string","","",True,False,"number of events as input"))
module4.addParameter(Parameter("DataType","","string","self","DataType",True, False, "data type"))
module4.addParameter(Parameter("CONFIG_NAME","","string","self","CONFIG_NAME",True, False, "Configuration Name"))
module4.addParameter(Parameter("CONFIG_VERSION","","string","self","CONFIG_VERSION",True, False, "Configuration Version"))
module4.addParameter(Parameter("appName","","string","self","appName",True, False, "Application Name"))
module4.addParameter(Parameter("appVersion","","string","self","appVersion",True, False, "Application Version"))
module4.addParameter(Parameter("inputData","","string","self","inputData",True,False,"InputData"))
module4.addParameter(Parameter("inputDataType","","string","self","inputDataType",True, False, "Input Data Type"))
module4.addParameter(Parameter("EVENTTYPE","","string","self","EVENTTYPE",True, False, "Event Type"))
module4.addParameter(Parameter("outputData",'',"string","self","outputData",True,False,"list of output data"))
module4.addParameter(Parameter("appType","","string","self","appType",True,False,"Application Version"))
module4.addParameter(Parameter("poolXMLCatName","","string","self","poolXMLCatName",True,False,"POOL XML slice"))
module4.addParameter(Parameter("SourceData","","string","self","SourceData",True,False,"InputData"))
module4.addParameter(Parameter("appLog","","string","self","appLog",True,False,"list of logfile"))
module4.addParameter(Parameter("listoutput",[],"list","self","listoutput",True,False,"list of output data"))


#define module 5
module5 = ModuleDefinition('JobFinalization')
module5.setDescription('Job Finalization module')
module5.setBody('from WorkflowLib.Module.JobFinalization import * \n')
module5.addParameter(Parameter("listoutput",[],"list","self","listoutput",True,False,"list of output data"))
module5.addParameter(Parameter("poolXMLCatName","","string","self","poolXMLCatName",True,False,"POOL XML slice"))
module5.addParameter(Parameter("inputData","","string","self","inputData",True,False,"InputData"))
module5.addParameter(Parameter("SourceData","","string","self","SourceData",True,False,"InputData"))
module5.addParameter(Parameter("DataType","","string","self","DataType",True, False, "data type"))
module5.addParameter(Parameter("CONFIG_NAME","","string","self","CONFIG_NAME",True, False, "Configuration Name"))
module5.addParameter(Parameter("CONFIG_VERSION","","string","self","CONFIG_VERSION",True, False, "Configuration Version"))

#define module 6
module6 = ModuleDefinition('Dummy')
module6.setDescription('Dummy module')
module6.setBody('from WorkflowLib.Module.Dummy import * \n')


###############   STEPS ##################################
#### step 1 we are creating step definition
step1 = StepDefinition('Gaudi_App_Step')
step1.addModule(module2) # Creating instance of the module 'Gaudi_App_Step'
moduleInstance2 = step1.createModuleInstance('GaudiApplication', 'module2')
#moduleInstance2 = step1.createModuleInstance('Dummy', 'module6')

step1.addModule(module3) # Creating instance of the module 'LogChecker'
moduleInstance3 = step1.createModuleInstance('LogChecker', 'module3')

step1.addModule(module4) # Creating instance of the module 'LogChecker'
moduleInstance4 = step1.createModuleInstance('BookkeepingReport', 'module4')
moduleInstance4.setLink('NUMBER_OF_EVENTS_INPUT',moduleInstance3.getName(),'NUMBER_OF_EVENTS_INPUT')
moduleInstance4.setLink('NUMBER_OF_EVENTS_OUTPUT',moduleInstance3.getName(),'NUMBER_OF_EVENTS_OUTPUT')
# in principle we can link parameters of moduleInstance2 with moduleInstance1 but
# in this case we going to use link with the step

# now we can add parameters for the STEP but instead of typing them we can just use old one from modules
step1.addParameterLinked(module3.parameters)
step1.addParameterLinked(module2.parameters)

# and we can add additional parameter which will be used as a global
step1.addParameter(Parameter("STEP_ID","@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}","string","","",True, False, "Temporary fix"))
step1.addParameter(Parameter("EVENTTYPE","30000000","string","","",True, False, "Event Type"))
step1.addParameter(Parameter("outputData","@{STEP_ID}.rdst","string","","",True,False,"output data"))
step1.setValue("appLog","@{appName}_@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}.log")
step1.unlink(["appLog","appName", "appType"])
step1.unlink(["poolXMLCatName","SourceData", "DataType", "CONFIG_NAME","CONFIG_VERSION","NUMBER_OF_EVENTS"])
step1.addParameter(Parameter("listoutput",[],"list","","",True,False,"list of output data"))

#outdata = "@{STEP_ID}.{appType}"
step2 = StepDefinition('Job_Finalization')
step2.addModule(module5)
moduleInstance6 = step2.createModuleInstance('JobFinalization','module5')
step2.addParameterLinked(module5.parameters)
step2.addParameter(Parameter("listoutput",[],"list","","",True,False,"list of output data"))
step2.unlink(["poolXMLCatName","SourceData", "DataType", "CONFIG_NAME","CONFIG_VERSION"])


##############  WORKFLOW #################################
workflow1 = Workflow(name=wkf_name)
workflow1.setDescription('Workflow of GaudiApplication')

workflow1.addStep(step1)
step1_prefix="step1_"
stepInstance1 = workflow1.createStepInstance('Gaudi_App_Step', 'Step1')
# lets link all parameters them up with the level of workflow
stepInstance1.linkUp(stepInstance1.parameters, step1_prefix)
stepInstance1.setLink("systemConfig","self", "SystemConfig") # capital letter corrected
# except "STEP_ID", "appLog"
stepInstance1.unlink(["listoutput", "STEP_ID", "appLog","appName", "optionsFile", "optionsLine", "appType", "outputData", "EVENTTYPE"])
stepInstance1.setValue("appName", "Brunel")
stepInstance1.setValue("appType", "rdst")
stepInstance1.setValue("optionsFile", "RealDataRdst.opts")
stepInstance1.setValue("optionsLine",opt_brunel)
stepInstance1.linkUp("CONFIG_NAME")
stepInstance1.linkUp("CONFIG_VERSION")
stepInstance1.linkUp("poolXMLCatName")
stepInstance1.linkUp("DataType")
stepInstance1.linkUp("SourceData")
stepInstance1.linkUp("NUMBER_OF_EVENTS")
stepInstance1.setLink("inputData","self", "InputData") # KGG linked with InputData of the Workflow
list_out=[{"outputDataName":"@{PRODUCTION_ID}_@{JOB_ID}_@{STEP_NUMBER}.@{appType}","outputType":"rdst","outputDataSE":"Tier1-RDST"}]
stepInstance1.setValue("listoutput",list_out)

workflow1.addStep(step2)
step2_prefix="step2_"
stepInstance2 = workflow1.createStepInstance('Job_Finalization', 'Step2')
stepInstance2.linkUp(stepInstance2.parameters, step2_prefix)
stepInstance2.linkUp("CONFIG_NAME")
stepInstance2.linkUp("CONFIG_VERSION")
stepInstance2.linkUp("DataType")
stepInstance2.linkUp("SourceData")
stepInstance2.linkUp("poolXMLCatName")
stepInstance2.unlink(["listoutput","inputData"])
stepInstance2.setLink("listoutput",stepInstance1.getName(),"listoutput")

# Now lets define parameters on the top
# lets specify parameters on the level of workflow
workflow1.addParameterLinked(step1.parameters, step1_prefix)
# and finally we can unlink them because we inherit them linked
workflow1.unlink(workflow1.parameters)

workflow1.setValue(step1_prefix+"appVersion", Brunel_version)
workflow1.setValue(step1_prefix+"nb_events_input", "@{NUMBER_OF_EVENTS}")
workflow1.addParameter(Parameter("poolXMLCatName","pool_xml_catalog.xml","string","","",True, False, "Application Name"))
workflow1.removeParameter(step1_prefix+"inputData") # KGG wrong parameter
workflow1.setValue(step1_prefix+"inputDataType","MDF")
workflow1.setValue(step1_prefix+"OUTPUT_MAX","20")
# remove unwanted
workflow1.removeParameter(step1_prefix+"systemConfig")
#add syspem config which common for all modules
workflow1.addParameter(Parameter("SystemConfig","slc4_ia32_gcc34","JDLReqt","","",True, False, "Application Name"))

workflow1.addParameter(Parameter("InputData",indata,"JDL","","",True, False, "Application Name"))
workflow1.addParameter(Parameter("JobType","test","JDL","","",True, False, "Job TYpe"))
workflow1.addParameter(Parameter("Owner","joel","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("StdError","std.err","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("StdOutput","std.out","JDL","","",True, False, "user Name"))

workflow1.addParameter(Parameter("MaxCPUTime",300000,"JDLReqt","","",True, False, "Application Name"))
#workflow1.addParameter(Parameter("Site","LCG.CERN.ch","JDLReqt","","",True, False, "Site"))
workflow1.addParameter(Parameter("Platform","gLite","JDLReqt","","",True, False, "platform"))

# and finally we can unlink them because we inherit them linked
workflow1.unlink(workflow1.parameters)

workflow1.addParameter(Parameter("PRODUCTION_ID","00003033","string","","",True, False, "Temporary fix"))
workflow1.addParameter(Parameter("JOB_ID","00000011","string","","",True, False, "Temporary fix"))
workflow1.addParameter(Parameter("EMAILNAME","joel.closier@cern.ch","string","","",True, False, "Email to send a report from the LogCheck module"))
workflow1.addParameter(Parameter("DataType","data","string","","",True, False, "type of Datatype"))
workflow1.addParameter(Parameter("SourceData",indata,"string","","",True, False, "Application Name"))
workflow1.addParameter(Parameter("CONFIG_NAME","LHCb","string","","",True, False, "Configuration Name"))
workflow1.addParameter(Parameter("CONFIG_VERSION","CCRC08","string","","",True, False, "Configuration Version"))
workflow1.addParameter(Parameter("NUMBER_OF_EVENTS",nb_evt_step1,"string","","",True, False, "number of events requested"))
if os.path.exists('wkf_CCRC_Brunel.xml'):
  print 'Removed existing workflow'
  os.remove('wkf_CCRC_Brunel.xml')
workflow1.toXMLFile('wkf_CCRC_Brunel.xml')
#print 'Creating code for the workflow'
print workflow1.createCode()
#eval(compile(workflow1.createCode(),'<string>','exec'))
#workflow1.execute()
