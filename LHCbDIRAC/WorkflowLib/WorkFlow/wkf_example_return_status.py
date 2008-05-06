from DIRAC.Core.Workflow.Parameter import *
from DIRAC.Core.Workflow.Module import *
from DIRAC.Core.Workflow.Step import *
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Core.Workflow.WorkflowReader import *

# this example made to show how return status shall propagate via several steps

module1 = ModuleDefinition('BaseModule')#during constraction class creates duplicating copies of the params
module1.setDescription('Gaudi Application module')
module1.setBody('from WorkflowLib.Module.BaseModule import BaseModule\n')
module1.addParameter(Parameter("enable","true","string","","",True, False, "enabling module"))
module1.addParameter(Parameter("result_prev","None","string","","",True, False, "result from prev modile"))
module1.addParameter(Parameter("result","None","string","","",False, True, "result"))
# we are ignoring parameter "result_prev" t keep initial value given by module

module2 = ModuleDefinition('BaseModuleFinalization')#during constraction class creates duplicating copies of the params
module2.setDescription('Gaudi Application module')
module2.setBody('from WorkflowLib.Module.BaseModuleFinalization import BaseModuleFinalization\n')
module2.addParameter(Parameter("enable","true","string","","",True, False, "enabling module"))

###############   STEPS ##################################
step1 = StepDefinition('Gaudi_App_Step')
step1.addModule(module1) # Creating instance of the module 'Gaudi_App_Step'
step1.addModule(module2) # Creating instance of the module 'Gaudi_App_Step'
moduleInstance1 = step1.createModuleInstance('BaseModule', 'module_inst1')

moduleInstance2 = step1.createModuleInstance('BaseModule', 'module_inst2')
moduleInstance2.setLink('result_prev',moduleInstance1.getName(),'result') # we are forming a chain
moduleInstance3 = step1.createModuleInstance('BaseModule', 'module_inst3')
moduleInstance3.setLink('result_prev',moduleInstance2.getName(),'result') # we are forming a chain

moduleInstance4 = step1.createModuleInstance('BaseModuleFinalization', 'module_inst4')
moduleInstance4.setLink('result_1',moduleInstance1.getName(),'result') # we are forming a chain
moduleInstance4.setLink('result_2',moduleInstance2.getName(),'result') # we are forming a chain
moduleInstance4.setLink('result_3',moduleInstance3.getName(),'result') # we are forming a chain

