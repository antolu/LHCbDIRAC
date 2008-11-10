__RCSID__ = "$Id: wkf_Fest_Merge.py,v 1.1 2008/11/10 12:47:04 paterson Exp $"

from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *

#Dynamic parameters and test data

wkf_name = "Fest_MDF_Merge"
WorkflowLib_version = 'wkf-v7r1'
inputData = '/lhcb/test/2008/MDF/00003078/0000/00003078_00009964_2.mdf'
outputDataSE = 'CERN_M-DST'
jobType = 'MergeMDF'
systemConfig = 'slc4_ia32_gcc34'
configName = 'test'
configVersion = '2008'
cpuTime = 5*60*60
dataType = "fest"
outputDataFileMask = 'mdf'
#workflow definition

MergeMDFModule = ModuleDefinition('MergeMDF')
MergeMDFModule.setDescription('Merge MDF Files Module')
MergeMDFModule.setBody('from WorkflowLib.Module.MergeMDF import MergeMDF\n')

MergeMDFStep = StepDefinition('Merge_MDF_Step')
MergeMDFStep.addModule(MergeMDFModule)
moduleInstance = MergeMDFStep.createModuleInstance('MergeMDF', 'MergeMDFModule')
MergeMDFStep.addParameterLinked(MergeMDFModule.parameters)
MergeMDFStep.addParameter(Parameter("outputDataSE","","string","","",True, False, "Output data SE."))
MergeMDFStep.addParameter(Parameter("listoutput",[],"list","","",True,False,"list of output data"))

# create workflow using definition

workflow1 = Workflow(name=wkf_name)
workflow1.setDescription('Workflow of MergeMDF')
workflow1.addStep(MergeMDFStep)

MergeMDFStep_prefix="MergeMDF_"
stepInstance1 = workflow1.createStepInstance('Merge_MDF_Step', 'MergeMDF')

stepInstance1.setValue("outputDataSE", outputDataSE)
outputFile=[{"outputDataName":"@{STEP_ID}.mdf","outputDataType":"MDF","outputDataSE":outputDataSE}]
stepInstance1.setValue("listoutput",outputFile)

workflow1.addParameter(Parameter("InputSandbox","LFN:/lhcb/applications/WorkflowLib-"+WorkflowLib_version+".tar.gz","JDL","","",True, False, "Job Type"))
workflow1.addParameter(Parameter("JobType","MergeMDF","JDL","","",True, False, "Job Type"))
workflow1.addParameter(Parameter("StdError","std.err","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("StdOutput","std.out","JDL","","",True, False, "user Name"))
workflow1.addParameter(Parameter("MaxCPUTime",cpuTime,"JDLReqt","","",True, False, "Application Name"))
workflow1.addParameter(Parameter("SystemConfig",systemConfig,"JDLReqt","","",True, False, "Application Name"))
workflow1.addParameter(Parameter("InputData",inputData,"JDL","","",True,False,"Input data"))

# and finally we can unlink them because we inherit them linked
workflow1.unlink(workflow1.parameters)
workflow1.addParameter(Parameter("PRODUCTION_ID","00001111","string","","",True, False, "Temporary fix"))
workflow1.addParameter(Parameter("JOB_ID","00001111","string","","",True, False, "Temporary fix"))
workflow1.addParameter(Parameter("dataType",dataType,"string","","",True, False, "type of Datatype"))
workflow1.addParameter(Parameter("configName",configName,"string","","",True, False, "Configuration Name"))
workflow1.addParameter(Parameter("configVersion",configVersion,"string","","",True, False, "Configuration Version"))
workflow1.addParameter(Parameter("outputDataFileMask",outputDataFileMask,"string","","",True, False, "Only upload files with the extensions in this parameter"))
if os.path.exists('wkf_'+wkf_name+'.xml'):
  print 'Removed existing workflow'
  os.remove('wkf_'+wkf_name+'.xml')
workflow1.toXMLFile('wkf_'+wkf_name+'.xml')
#w4 = fromXMLFile("/afs/cern.ch/user/g/gkuznets/test1.xml")
#print 'Creating code for the workflow'
#print workflow1.showCode()
print workflow1.createCode()
#eval(compile(workflow1.createCode(),'<string>','exec'))
#workflow1.execute()
