""" UnitTest class for LHCbDIRAC Interfaces/API
"""

__RCSID__ = "$Id"

import unittest
import string

from DIRAC.Core.Workflow.Parameter import Parameter
#TODO: fix Mock location 
from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
from DIRAC.Core.Workflow.Workflow import *
from DIRAC.Interfaces.API.Job import Job
from LHCbDIRAC.Workflow.Utilities.Utils import getStepDefinition

#############################################################################

class APITestCase( unittest.TestCase ):
  """ Base class
  """
  def setUp( self ):

    self.job = Job()
    pass

class Success( APITestCase ):


  def test__getStepDefinition( self ):
    importLine = """
from LHCbDIRAC.Workflow.Modules.<MODULE> import <MODULE>
"""
    #modules
    gaudiApp = ModuleDefinition( 'GaudiApplication' )
    gaudiApp.setDescription( 'Gaudi Application class' )
    body = string.replace( importLine, '<MODULE>', 'GaudiApplication' )
    gaudiApp.setBody( body )

    genBKReport = ModuleDefinition( 'BookkeepingReport' )
    genBKReport.setDescription( 'Bookkeeping Report class' )
    body = string.replace( importLine, '<MODULE>', 'BookkeepingReport' )
    genBKReport.setBody( body )
    genBKReport.addParameter( Parameter( "STEP_ID", "", "string", "self", "STEP_ID", True, False, "StepID" ) )

    #step
    gaudiAppDefn = StepDefinition( 'Gaudi_App_Step' )
    gaudiAppDefn.addModule( gaudiApp )
    gaudiAppDefn.createModuleInstance( 'GaudiApplication', 'GaudiApplication' )
    gaudiAppDefn.addModule( genBKReport )
    gaudiAppDefn.createModuleInstance( 'BookkeepingReport', 'BookkeepingReport' )

    gaudiAppDefn.addParameterLinked( gaudiApp.parameters )

    stepDef = getStepDefinition( 'Gaudi_App_Step', ['GaudiApplication', 'BookkeepingReport'] )

    self.assert_( str( gaudiAppDefn ) == str( stepDef ) )


    self.job._addParameter( gaudiAppDefn, 'name', 'type', 'value', 'desc' )
    self.job._addParameter( gaudiAppDefn, 'name1', 'type1', 'value1', 'desc1' )


    stepDef = getStepDefinition( 'Gaudi_App_Step', ['GaudiApplication', 'BookkeepingReport'],
                                 parametersList = [[ 'name', 'type', 'value', 'desc' ],
                                                   [ 'name1', 'type1', 'value1', 'desc1' ]] )


    self.assert_( str( gaudiAppDefn ) == str( stepDef ) )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( APITestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( Success ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
