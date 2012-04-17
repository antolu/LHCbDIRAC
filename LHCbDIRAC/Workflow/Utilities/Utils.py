"""
Collection of utilities function
"""

from DIRAC.Core.Workflow.Step import StepDefinition
from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Parameter import Parameter

from DIRAC import gLogger
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

def makeRunList( runInput ):
  """ makeRunList return a list of runs starting from a string.
      Example:
      makeRunList("1234:1236,12340,12342,1520:1522") --> ['1234','1235','1236','12340','12342','1520','1521','1522']
  """

  import string
  from DIRAC import S_OK, S_ERROR

  try:
    #remove blank spaces
    l = string.join( runInput.split(), "" )
    i = l.split( "," )
    runList = []
    for part in i:
      if part.find( ':' ):
        pp = part.split( ":" )
        for p in range( int( pp[0] ), int( pp[len( pp ) - 1] ) + 1 ):
          runList.append( str( p ) )
      else:
        runList.append( str( part ) )
    return S_OK( runList )
  except Exception, e:
    return S_ERROR( "Could not parse runList", e )

#############################################################################
