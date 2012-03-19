"""
Collection of utilities function
"""

from DIRAC.Core.Workflow.Step import StepDefinition
from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Parameter import Parameter

#############################################################################

def getStepDefinition( stepName, modulesNameList = [], importLine = """""", parametersList = [] ):
  """ Given a name, a list modules, and a list of parameters , return a step definition.
      Step definition = Parameters + Module Instances
  """

  if not importLine:
    importLine = "LHCbDIRAC.Workflow.Modules"

  stepDef = StepDefinition( stepName )

  for moduleName in modulesNameList:

    #create the module definition
    moduleDef = ModuleDefinition( moduleName )
    moduleDef.setDescription( getattr( __import__( "%s.%s" % ( importLine, moduleName ),
                                                   globals(), locals(), ['__doc__'] ),
                                       "__doc__" ) )
    moduleDef.setBody( """\nfrom %s.%s import %s\n""" % ( importLine, moduleName, moduleName ) )


    #FIXME: really necessary?
#    if moduleName in ( 'GaudiApplication', 'AnalyseLogFile' ):
#      stepDef.addParameterLinked( moduleDef.parameters )
#    if moduleName in ( 'BookkeepingReport' ):
#      moduleDef.addParameter( Parameter( "STEP_ID", "", "string", "self", "STEP_ID", True, False, "StepID" ) )

    #add the module to the step, and instance it
    stepDef.addModule( moduleDef )
    stepDef.createModuleInstance( module_type = moduleName, name = moduleName )

  #add parameters to the module definition
  for pName, pType, pValue, pDesc in parametersList:
    p = Parameter( pName, pValue, pType, "", "", True, False, pDesc )
    stepDef.addParameter( Parameter( parameter = p ) )

  return stepDef

#############################################################################

def addStepToWorkflow( workflow, stepDefinition, name ):
  """ Add a stepDefinition to a workflow, instantiating it, and giving it a name
  """

  workflow.addStep( stepDefinition )
  workflow.createStepInstance( stepDefinition.getType(), name )

  return workflow

#############################################################################
