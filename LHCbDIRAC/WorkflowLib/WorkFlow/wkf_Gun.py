
from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *

simDescription = 'ParticuleGun'
eventTypeSignal = "55230100"
wkf_name = "ParticuleGun_"+eventTypeSignal
#eventTypeSignal = "30000000"
numberEventSignal = 20000
emailList = 'lhcb-datacrash@cern.ch'
generatorName = "Pythia"
WorkflowLib_version = 'wkf-v6r3'
Gauss_version = "v35r1" #v35r0
Gauss_optfile = "Gauss-2008.py;$DECFILESROOT/options/@{eventType}.opts;$GAUSSOPTS/RichExtendedInfo.opts"
Gauss_optionsType = "py"
extraPackages = '' #semicolon separated list if necessary
system_os = "slc4_ia32_gcc34"
soft_package = "Gauss."+Gauss_version
outputDataFileMask = "sim" #semicolon separated list if necessary
Gauss_SE = "CERN-RDST"

#opt_gauss = "importOptions(\"$DECFILESROOT/options/@{eventType}.opts\")"
opt_gauss = "MessageSvc().Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc().timeFormat = '%Y-%m-%d %H:%M:%S UTC'"
opt_gauss = opt_gauss + ";OutputStream(\"GaussTape\").Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\""


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
module6.setBody('from WorkflowLib.Module.JobFinalization import * \n')

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
step1.addParameter(Parameter("optionsType","","string","","",True, False, "options Type for this application"))
step1.addParameter(Parameter("applicationLog","","string","","",True,False,"name of the output file of the application"))
step1.addParameter(Parameter("optionsFile","","string","","",True,False,"Options File"))
step1.addParameter(Parameter("optionsLine","","string","","",True,False,"option to be added last","option"))
step1.addParameter(Parameter("optionsLinePrev","","string","","",True,False,"options to be added first","option"))
step1.addParameter(Parameter("numberOfEvents","","string","","",True,False,"number of events"))
step1.addParameter(Parameter("numberOfEventsInput","","string","","",True,False,"number of events as input"))
step1.addParameter(Parameter("numberOfEventsOutput","","string","","",True,False,"number of events as input"))
step1.addParameter(Parameter("inputDataType","","string","","",True, False, "Input Data Type"))
step1.addParameter(Parameter("listoutput",[],"list","","",True,False,"list of output data"))
step1.addParameter(Parameter("extraPackages","","string","","",True,False,"Extra software packages to setup"))

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
stepInstance1.setValue("generatorName", generatorName)
stepInstance1.setValue("inputDataType","None")
stepInstance1.setValue("applicationName", "Gauss")
stepInstance1.setValue("applicationVersion", Gauss_version)
stepInstance1.setValue("applicationType", "sim")
stepInstance1.setValue("optionsType", Gauss_optionsType)
stepInstance1.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
stepInstance1.setValue("outputData","@{STEP_ID}.@{applicationType}")
stepInstance1.setValue("optionsFile", Gauss_optfile)
stepInstance1.setValue("optionsLine",opt_gauss)
stepInstance1.setValue("optionsLinePrev","None")
stepInstance1.setValue("extraPackages",extraPackages)
list1_out=[{"outputDataName":"@{STEP_ID}.@{applicationType}","outputDataType":"sim","outputDataSE":Gauss_SE}]
stepInstance1.setValue("listoutput",list1_out)


workflow1.addStep(step3)
step4_prefix="step4_"
stepInstance4 = workflow1.createStepInstance('Job_Finalization', 'Step4')
# Now lets define parameters on the top
# lets specify parameters on the level of workflow

workflow1.addParameter(Parameter("InputSandbox","LFN:/lhcb/applications/WorkflowLib-"+WorkflowLib_version+".tar.gz","JDL","","",True, False, "Job Type"))
workflow1.addParameter(Parameter("JobType","MCSimulation","JDL","","",True, False, "Job Type"))
workflow1.addParameter(Parameter("Owner","joel","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("BannedSites","LCG.CERN.ch;LCG.CNAF.it;LCG.RAL.uk;LCG.PIC.es;LCG.IN2P3.fr;LCG.NIKHEF.nl;LCG.GRIDKA.de","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("StdError","std.err","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("StdOutput","std.out","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("OUTPUT_MAX","20","string","","",True,False,"nb max of output to keep"))

workflow1.addParameter(Parameter("MaxCPUTime",300000,"JDLReqt","","",True, False, "Application Name"))
workflow1.addParameter(Parameter("SystemConfig",system_os,"JDLReqt","","",True, False, "Application Name"))

# and finally we can unlink them because we inherit them linked
workflow1.unlink(workflow1.parameters)

workflow1.addParameter(Parameter("PRODUCTION_ID","00006044","string","","",True, False, "Temporary fix"))
workflow1.addParameter(Parameter("JOB_ID","00000104","string","","",True, False, "Temporary fix"))
workflow1.addParameter(Parameter("emailAddress",emailList,"string","","",True, False, "Email to send a report from the LogCheck module"))
workflow1.addParameter(Parameter("dataType","MC","string","","",True, False, "type of Datatype"))
workflow1.addParameter(Parameter("poolXMLCatName","pool_xml_catalog.xml","string","","",True, False, "Application Name"))
workflow1.addParameter(Parameter("configName","MC","string","","",True, False, "Configuration Name"))
workflow1.addParameter(Parameter("configVersion","2008","string","","",True, False, "Configuration Version"))
workflow1.addParameter(Parameter("simDescription",simDescription,"string","","",True, False, "Simulation description"))
workflow1.addParameter(Parameter("outputDataFileMask",outputDataFileMask,"string","","",True, False, "Only upload files with the extensions in this parameter"))
if os.path.exists('wkf_MC.xml'):
  print 'Removed existing workflow'
  os.remove('wkf_MC.xml')
workflow1.toXMLFile('wkf_'+eventTypeSignal+'.xml')
#w4 = fromXMLFile("/afs/cern.ch/user/g/gkuznets/test1.xml")
#print 'Creating code for the workflow'
#print workflow1.showCode()
print workflow1.createCode()
#eval(compile(workflow1.createCode(),'<string>','exec'))
#workflow1.execute()
