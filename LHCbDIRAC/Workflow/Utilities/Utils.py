"""
Collection of utilities function
"""

import os, time

from DIRAC.Core.Workflow.Step import StepDefinition
from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Parameter import Parameter

#############################################################################

def getStepDefinition( stepName, modulesNameList = [], importLine = """""", parametersList = [] ):
  """ Given a name, a list modules, and a list of parameters , return a step definition.
      Step definition = Parameters + Module Instances
  """
  #TODO: generic enough, can be moved to DIRAC

  if not importLine:
    importLine = "LHCbDIRAC.Workflow.Modules"

  stepDef = StepDefinition( stepName )

  for moduleName in modulesNameList:

    #create the module definition
    moduleDef = ModuleDefinition( moduleName )
    try:
      moduleDef.setDescription( getattr( __import__( "%s.%s" % ( importLine, moduleName ),
                                                     globals(), locals(), ['__doc__'] ),
                                        "__doc__" ) )
      moduleDef.setBody( """\nfrom %s.%s import %s\n""" % ( importLine, moduleName, moduleName ) )
    except ImportError:
      importLine = "DIRAC.Core.Workflow.Modules"
      moduleDef.setDescription( getattr( __import__( "%s.%s" % ( importLine, moduleName ),
                                                     globals(), locals(), ['__doc__'] ),
                                        "__doc__" ) )
      moduleDef.setBody( """\nfrom %s.%s import %s\n""" % ( importLine, moduleName, moduleName ) )
      importLine = "LHCbDIRAC.Workflow.Modules"

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
  return workflow.createStepInstance( stepDefinition.getType(), name )

#############################################################################

def getStepCPUTimes( step_commons ):
  """ CPU times of a step
  """
  exectime = 0
  if step_commons.has_key( 'StartTime' ):
    exectime = time.time() - step_commons['StartTime']

  cputime = 0
  if step_commons.has_key( 'StartStats' ):
    #5-tuple: utime, stime, cutime, cstime, elapsed_time
    stats = os.times()
    cputimeNow = stats[ 0 ] + stats[ 1 ] + stats[ 2 ] + stats[ 3 ]
    cputimeBefore = step_commons[ 'StartStats' ][ 0 ] + step_commons[ 'StartStats' ][ 1 ] \
                  + step_commons[ 'StartStats' ][ 2 ] + step_commons[ 'StartStats' ][ 3 ]
    cputime = cputimeNow - cputimeBefore

  return exectime, cputime

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
  except Exception:
    return S_ERROR( "Could not parse runList " )

#############################################################################
