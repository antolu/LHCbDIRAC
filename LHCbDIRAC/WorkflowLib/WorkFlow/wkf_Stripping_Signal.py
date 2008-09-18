from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *

# Variable which need to be set
wkf_name = "DC06_Stripping_Signal"
eventTypeSignal = "11962600"
nb_evt_step1 = -1
nb_evt_step2 = -1
Brunel_version = "v31r12"
#Brunel_optfile = "RealData-ETC.opts"
Brunel_optfile = "DC06-ETC.opts"
DaVinci_version = "v19r7"
DaVinci_optfile = "DVOfficialStrippingFile.opts"
system_os = "slc4_ia32_gcc34"
outputDataFileMask = "dst;root" #semicolon separated list if necessary

opt_dav = "EvtTupleSvc.Output = {}"
opt_dav = opt_dav+";DiLeptonForBd2LLKstar.DaughterFilter.Selections =  { 'e+ : KinFilterCriterion, PVIPFilterCriterion' }"
opt_dav = opt_dav+";ApplicationMgr.OutStream -= {'DstWriter'}"
opt_dav = opt_dav+";ApplicationMgr.OutStream = {'Sequencer/SeqWriteTag'}"
opt_dav = opt_dav+";ApplicationMgr.TopAlg -= { \"GaudiSequencer/SeqPreselHWZ2bbl\" }"
opt_dav = opt_dav+";MessageSvc.Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc.timeFormat = '%Y-%m-%d %H:%M:%S UTC'"
opt_dav = opt_dav+";WR.Output = \"Collection=\'EVTTAGS/TagCreator/1\' ADDRESS=\'/Event\' DATAFILE=\'@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\""
opt_dav_prev = "None"
#opt_dav_prev = "DaVinciMainSeq.Members += { \"CheckPV\" }; CheckPV.MaxPVs = 2 ;StdNoPIDsKaonsSeq.Members = { \"CheckPV\" };StdLooseKaonsSeq.Members = { \"CheckPV\" };StdTightKaonsSeq.Members = { \"CheckPV\" };StdNoPIDsPionsSeq.Members = { \"CheckPV\" };StdLoosePionsSeq.Members = { \"CheckPV\" };StdTightPionsSeq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdLooseVTTPionsSeq.Members = { \"CheckPV\" };StdLooseDownstreamPionsSeq.Members = { \"CheckPV\" };StdNoPIDsProtonsSeq.Members = { \"CheckPV\" };StdLooseProtonsSeq.Members = { \"CheckPV\" };StdTightProtonsSeq.Members = { \"CheckPV\" };StdLooseVTTProtonsSeq.Members = { \"CheckPV\" };StdLooseDownstreamProtonsSeq.Members = { \"CheckPV\" };StdNoPIDsMuonsSeq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdLooseMuonsSeq.Members = { \"CheckPV\" };StdTightMuonsSeq.Members = { \"CheckPV\" };StdNoPIDsElectronsSeq.Members = { \"CheckPV\" };StdLooseElectronsSeq.Members = { \"CheckPV\" };StdTightElectronsSeq.Members = { \"CheckPV\" };StdLooseResolvedPi0sSeq.Members = { \"CheckPV\" };StdLoosePhotonsSeq.Members = { \"CheckPV\" };StdLooseCnvPhotonsSeq.Members = { \"CheckPV\" };StdLooseAllPhotonsSeq.Members = { \"CheckPV\" };StdLooseD02KPiSeq.Members = { \"CheckPV\" };StdLooseD02PiPiSeq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdLooseD02KKSeq.Members = { \"CheckPV\" };StdLooseD02KsPiPiSeq.Members = { \"CheckPV\" };StdLooseD02KsKKSeq.Members = { \"CheckPV\" };StdLooseD02KKPiPiNoCCSeq.Members = { \"CheckPV\" };StdLooseD02KPiPiPiCASeq.Members = { \"CheckPV\" };StdLooseD2KPiPiSeq.Members = { \"CheckPV\" };StdLooseDs2KKPiSeq.Members = { \"CheckPV\" };StdLooseDstarWithD02KPiSeq.Members = { \"CheckPV\" };StdLooseDstarWithD02PiPiSeq.Members = { \"CheckPV\" };StdLooseDstarWithD02KKSeq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdUnbiasedHeavyDimuonSeq.Members = { \"CheckPV\" };StdUnbiasedJpsi2MuMuSeq.Members = { \"CheckPV\" };StdLooseBiasedJpsi2MuMuSeq.Members = { \"CheckPV\" };StdLooseJpsi2eeSeq.Members = { \"CheckPV\" };StdLooseKsLLSeq.Members = { \"CheckPV\" };StdTightKsLLSeq.Members = { \"CheckPV\" };StdLooseKsDDSeq.Members = { \"CheckPV\" };StdTightKstar2KPiSeq.Members = { \"CheckPV\" };StdLooseKstar2KPiSeq.Members = { \"CheckPV\" };StdLooseRho02PiPiSeq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdLoosePhi2KKSeq.Members = { \"CheckPV\" };StdLooseLambda1115_LL_Seq.Members = { \"CheckPV\" };StdLooseLambda1115_DD_Seq.Members = { \"CheckPV\" };StdLooseLambda1115_LU_Seq.Members = { \"CheckPV\" };StdLooseLambda1115_UL_Seq.Members = { \"CheckPV\" };StdLooseLambda1115_LD_Seq.Members = { \"CheckPV\" };StdLooseLambda1115_DL_Seq.Members = { \"CheckPV\" };StdTightLambda1115_LL_Seq.Members = { \"CheckPV\" };StdTightLambda1115_DD_Seq.Members = { \"CheckPV\" };StdTightLambda1115_LU_Seq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdTightLambda1115_UL_Seq.Members = { \"CheckPV\" };StdTightLambda1115_LD_Seq.Members = { \"CheckPV\" };StdTightLambda1115_DL_Seq.Members = { \"CheckPV\" };StdLooseD02KPiSeq.Members = { \"CheckPV\" };StdLooseD02PiPiSeq.Members = { \"CheckPV\" };StdLooseD02KKSeq.Members = { \"CheckPV\" };StdLooseD02KsPiPiSeq.Members = { \"CheckPV\" };StdLooseD02KsKKSeq.Members = { \"CheckPV\" };StdLooseD02KsKPiSeq.Members = { \"CheckPV\" };StdLooseD02KKPiPiNoCCSeq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdLooseD02KPiPiPiCASeq.Members = { \"CheckPV\" };StdLooseD2KPiPiSeq.Members = { \"CheckPV\" };StdTightD2KPiPiSeq.Members = { \"CheckPV\" };StdLooseDs2KKPiSeq.Members = { \"CheckPV\" };StdLooseDstarWithD02KPiSeq.Members = { \"CheckPV\" };StdLooseDstarWithD02PiPiSeq.Members = { \"CheckPV\" };StdLooseDstarWithD02KKSeq.Members = { \"CheckPV\" };StdNoPIDsElectronsSeq.Members = { \"CheckPV\" };StdLooseElectronsSeq.Members = { \"CheckPV\" };StdTightElectronsSeq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdUnbiasedHeavyDimuonSeq.Members = { \"CheckPV\" };StdLoosePenguinDimuonSeq.Members = { \"CheckPV\" };StdUnbiasedJpsi2MuMuSeq.Members = { \"CheckPV\" };StdLooseBiasedJpsi2MuMuSeq.Members = { \"CheckPV\" };StdLooseJpsi2eeSeq.Members = { \"CheckPV\" };StdLooseBiasedJpsi2eeSeq.Members = { \"CheckPV\" };StdNoPIDsKaonsSeq.Members = { \"CheckPV\" };StdLooseKaonsSeq.Members  += { \"CheckPV\" };StdTightKaonsSeq.Members  += { \"CheckPV\" };StdLooseKsLLSeq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdTightKsLLSeq.Members = { \"CheckPV\" };StdLooseKsDDSeq.Members = { \"CheckPV\" };StdTightKstar2KPiSeq.Members = { \"CheckPV\" };StdLooseKstar2KPiSeq.Members = { \"CheckPV\" };StdLooseLambda1115_LL_Seq.Members = { \"CheckPV\" };StdLooseLambda1115_DD_Seq.Members = { \"CheckPV\" };StdLooseLambda1115_LU_Seq.Members = { \"CheckPV\" };StdLooseLambda1115_UL_Seq.Members = { \"CheckPV\" };StdLooseLambda1115_LD_Seq.Members = { \"CheckPV\" };StdLooseLambda1115_DL_Seq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdTightLambda1115_LL_Seq.Members = { \"CheckPV\" };StdTightLambda1115_DD_Seq.Members = { \"CheckPV\" };StdTightLambda1115_LU_Seq.Members = { \"CheckPV\" };StdTightLambda1115_UL_Seq.Members = { \"CheckPV\" };StdTightLambda1115_LD_Seq.Members = { \"CheckPV\" };StdTightLambda1115_DL_Seq.Members = { \"CheckPV\" };StdNoPIDsMuonsSeq.Members = { \"CheckPV\" };StdLooseMuonsSeq.Members = { \"CheckPV\" };StdTightMuonsSeq.Members = { \"CheckPV\" };StdLoosePhi2KKSeq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdLoosePhotonsSeq.Members = { \"CheckPV\" };StdLooseCnvPhotonsSeq.Members = { \"CheckPV\" };StdLooseAllPhotonsSeq.Members = { \"CheckPV\" };StdNoPIDsPionsSeq.Members = { \"CheckPV\" };StdLoosePionsSeq.Members = { \"CheckPV\" };StdLooseVTTPionsSeq.Members = { \"CheckPV\" };StdNoPIDsVTTPionsSeq.Members = { \"CheckPV\" };StdLooseDownstreamPionsSeq.Members = { \"CheckPV\" };StdTightPionsSeq.Members = { \"CheckPV\" };StdNoPIDsProtonsSeq.Members = { \"CheckPV\" }"
#opt_dav_prev = opt_dav_prev+";StdLooseProtonsSeq.Members = { \"CheckPV\" };StdLooseVTTProtonsSeq.Members = { \"CheckPV\" };StdLooseDownstreamProtonsSeq.Members = { \"CheckPV\" };StdTightProtonsSeq.Members = { \"CheckPV\" };StdLooseResolvedPi0sSeq.Members = { \"CheckPV\" };StdLooseRho02PiPiSeq.Members = { \"CheckPV\" }"

#opt_brunel = "#include \"$BRUNELOPTS/SuppressWarnings.opts\""
#opt_brunel = opt_brunel+";MessageSvc.Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc.timeFormat = '%Y-%m-%d %H:%M:%S UTC'"
opt_brunel = "MessageSvc.Format = '%u % F%18W%S%7W%R%T %0W%M';MessageSvc.timeFormat = '%Y-%m-%d %H:%M:%S UTC'"
opt_brunel = opt_brunel+";EventLoopMgr.OutputLevel = 3"
opt_brunel = opt_brunel+";DstWriter.Output = \"DATAFILE=\'PFN:@{outputData}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\""
opt_brunel = opt_brunel+";EvtTupleSvc.Output = {\"EVTTAGS2 DATAFILE=\'PFN:@{etcf}\' TYP=\'POOL_ROOTTREE\' OPT=\'RECREATE\'\"}"
opt_brunel = opt_brunel+";HistogramPersistencySvc.OutputFile = \"\""
opt_brunel = opt_brunel+";IODataManager.AgeLimit = 2"
#indata = "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_1.sim;LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_2.sim;LFN:/lhcb/production/DC06/phys-v2-lumi2/00001820/SIM/0000/00001820_00000001_3.sim"
#indata = "LFN:/lhcb/data/CCRC08/RDST/00000130/0000/00000130_00007084_1.rdst"
indata = "LFN:/lhcb/MC/DC06/DST/00003017/0000/00003017_00004115_5.dst"
#gridjkaindata = "LFN:/lhcb/data/CCRC08/RDST/00000130/0000/00000130_00000282_1.rdst"
#IN2P3indata = "LFN:/lhcb/data/CCRC08/RDST/00000130/0000/00000130_00008670_1.rdst"
#indata =  "LFN:/lhcb/data/CCRC08/RDST/00000130/0001/00000130_00010149_1.rdst;LFN:/lhcb/data/CCRC08/RAW/LHCb/CCRC/22848/022848_0000021562.raw"

#indata = "LFN:/lhcb/data/CCRC08/RDST/00000130/0000/00000130_00007084_1.rdst;LFN:/lhcb/data/CCRC08/RAW/LHCb/CCRC/22808/022808_0000018192.raw"
#indata = "LFN:/lhcb/data/CCRC08/RDST/00000106/0000/00000106_00007918_1.rdst;/lhcb/data/CCRC08/RAW/LHCb/CCRC/420217/420217_0000116193.raw"

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
#moduleInstance2 = step1.createModuleInstance('Dummy', 'module7')

step1.addModule(module3) # Creating instance of the module 'LogChecker'
moduleInstance3 = step1.createModuleInstance('LogChecker', 'module3')

step1.addModule(module4) # Creating instance of the module 'LogChecker'
moduleInstance4 = step1.createModuleInstance('BookkeepingReport', 'module4')

# now we can add parameters for the STEP but instead of typing them we can just use old one from modules
step1.addParameterLinked(module3.parameters)
step1.addParameterLinked(module2.parameters)

# and we can add additional parameter which will be used as a global
step1.addParameter(Parameter("eventType","","string","","",True, False, "Event Type"))
step1.addParameter(Parameter("inputData","","string","","",True,False,"InputData"))
step1.addParameter(Parameter("outputData","","string","","",True,False,"etc name"))
step1.addParameter(Parameter("etcf","@{STEP_ID}.root","string","","",True,False,"etc name"))
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
stepInstance1 = workflow1.createStepInstance('Gaudi_App_Step', 'DaVinci')
stepInstance1.setValue("eventType", eventTypeSignal)
stepInstance1.setValue("numberOfEvents", nb_evt_step1)
stepInstance1.setValue("numberOfEventsInput", 0)
stepInstance1.setValue("numberOfEventsOutput", 0)
stepInstance1.setValue("inputDataType","DST")
stepInstance1.setValue("applicationName", "DaVinci")
stepInstance1.setValue("applicationVersion", DaVinci_version)
stepInstance1.setValue("applicationType", "root")
stepInstance1.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
stepInstance1.setValue("outputData","@{STEP_ID}.@{applicationType}")
stepInstance1.setValue("optionsFile", DaVinci_optfile)
stepInstance1.setValue("optionsLine",opt_dav)
stepInstance1.setValue("optionsLinePrev",opt_dav_prev)
stepInstance1.setLink("inputData","self","InputData") # KGG linked with InputData of the Workflow
list1_out=[{"outputDataName":"@{STEP_ID}.@{applicationType}","outputDataType":"FETC","outputDataSE":"CERN-RDST,Tier1_MC_M-DST,Any:Tier1_MC-DST"}]
stepInstance1.setValue("listoutput",list1_out)

step2_prefix="step2_"
stepInstance2 = workflow1.createStepInstance('Gaudi_App_Step', 'Brunel')
stepInstance2.setValue("eventType", eventTypeSignal)
stepInstance2.setValue("numberOfEvents", nb_evt_step2)
stepInstance2.setValue("inputDataType","FETC")
stepInstance2.setValue("applicationName", "Brunel")
stepInstance2.setValue("applicationVersion", Brunel_version)
stepInstance2.setValue("applicationType", "dst")
stepInstance2.setValue("applicationLog", "@{applicationName}_@{STEP_ID}.log")
stepInstance2.setValue("outputData","@{STEP_ID}.@{applicationType}")
stepInstance2.setValue("etcf","@{STEP_ID}.root")
stepInstance2.setValue("optionsFile", Brunel_optfile)
stepInstance2.setValue("optionsLine",opt_brunel)
stepInstance2.setValue("optionsLinePrev","None")
#stepInstance2.setValue("outputDataSE","Tier1_M-DST")
stepInstance2.setLink("inputData",stepInstance1.getName(),"outputData")
list2_out=[{"outputDataName":"@{STEP_ID}.@{applicationType}","outputDataType":"dst","outputDataSE":"CERN-RDST,Tier1_MC_M-DST,Any:Tier1_MC-DST"},{"outputDataName":"@{STEP_ID}.root","outputDataType":"SETC","outputDataSE":"CERN-RDST,Tier1_MC_M-DST,Any:Tier1_MC-DST"}]
stepInstance2.setValue("listoutput",list2_out)

workflow1.addStep(step3)
step3_prefix="step3_"
stepInstance3 = workflow1.createStepInstance('Job_Finalization', 'Step3')
# Now lets define parameters on the top
# lets specify parameters on the level of workflow
#workflow1.addParameterLinked(step1.parameters, step1_prefix)
#workflow1.addParameterLinked(step1.parameters, step2_prefix)
#workflow1.addParameterLinked(step3.parameters, step3_prefix)
# and finally we can unlink them because we inherit them linked
#workflow1.unlink(workflow1.parameters)

workflow1.addParameter(Parameter("InputSandbox","LFN:/lhcb/applications/WorkflowLib-wkf-v2r1.tar.gz","JDL","","",True, False, "Job TYpe"))
workflow1.addParameter(Parameter("InputData",indata,"JDL","","",True, False, "Application Name"))
workflow1.addParameter(Parameter("JobType","MCStripping","JDL","","",True, False, "Job Type"))
workflow1.addParameter(Parameter("Owner","joel","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("StdError","std.err","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("StdOutput","std.out","JDL","","",True, False, "user Name"))

workflow1.addParameter(Parameter("MaxCPUTime",300000,"JDLReqt","","",True, False, "Application Name"))
#workflow1.addParameter(Parameter("Platform","gLite","JDLReqt","","",True, False, "platform"))
workflow1.addParameter(Parameter("SystemConfig",system_os,"JDLReqt","","",True, False, "Application Name"))

# and finally we can unlink them because we inherit them linked
workflow1.unlink(workflow1.parameters)

workflow1.addParameter(Parameter("PRODUCTION_ID","00010030","string","","",True, False, "Temporary fix"))
workflow1.addParameter(Parameter("JOB_ID","00000011","string","","",True, False, "Temporary fix"))
workflow1.addParameter(Parameter("emailAddress","lhcb-datacrash@cern.ch","string","","",True, False, "Email to send a report from the LogCheck module"))
workflow1.addParameter(Parameter("dataType","MC","string","","",True, False, "type of Datatype"))
workflow1.addParameter(Parameter("sourceData",indata,"string","","",True, False, "Application Name"))
workflow1.addParameter(Parameter("poolXMLCatName","pool_xml_catalog.xml","string","","",True, False, "Application Name"))
workflow1.addParameter(Parameter("configName","LHCb","string","","",True, False, "Configuration Name"))
workflow1.addParameter(Parameter("configVersion","DC06","string","","",True, False, "Configuration Version"))
workflow1.addParameter(Parameter("DCconfigName","DC06","string","","",True, False, "Configuration Name"))
workflow1.addParameter(Parameter("DCconfigVersion","Stripping-v31-lumi2","string","","",True, False, "Configuration Version"))
workflow1.addParameter(Parameter("systemConfig",system_os,"string","","",True, False, "Application Name"))
#workflow1.addParameter(Parameter("NUMBER_OF_EVENTS","5","string","","",True, False, "number of events requested"))
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
