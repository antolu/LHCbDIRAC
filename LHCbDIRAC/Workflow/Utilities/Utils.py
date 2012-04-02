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

"""
makeRunList return a list of runs starting from a string.

Example:

makeRunList("1234:1236,12340,12342,1520:1522") --> ['1234','1235','1236','12340','12342','1520','1521','1522']
 
"""

def makeRunList(runInput):

  import string

  res={'OK':False,'RunList':[],'Message':''}
  try:
    #remove blank spaces
    l = string.join(runInput.split(),"")
    i = l.split(",")
    runList=[]
    for part in i:
       if part.find(':'):
         pp = part.split(":")
         print pp 
         for p in range(int(pp[0]),int(pp[len(pp)-1])+1):
           runList.append(str(p))
       else:
          runList.append(str(part))
    res['OK']=True
    res['RunList']=runList
    res['Message']='Successfully parsed run input list'
    gLogger.info(res['Message'])
  except:
    res['OK']=False
    res['Message']='Run List string not correctly parsed!'
    print res['Message']
    gLogger.error(res['Message'])
  return res
