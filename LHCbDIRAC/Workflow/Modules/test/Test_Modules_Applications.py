""" Unit tests for Workflow Modules utilities
"""

#pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import unittest
import copy
import os

from mock import MagicMock, patch

from DIRAC import gLogger

from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from LHCbDIRAC.BookkeepingSystem.Client.test.mock_BookkeepingClient import bkc_mock
from LHCbDIRAC.Workflow.Modules.test.mock_Commons import prod_id, prod_job_id, wms_job_id, \
                                                         workflowStatus, stepStatus, step_id, step_number,\
                                                         step_commons, wf_commons

from LHCbDIRAC.Workflow.Modules.GaudiApplication import GaudiApplication
from LHCbDIRAC.Workflow.Modules.GaudiApplicationScript import GaudiApplicationScript
from LHCbDIRAC.Workflow.Modules.RootApplication import RootApplication
from LHCbDIRAC.Workflow.Modules.LHCbScript import LHCbScript
from LHCbDIRAC.Workflow.Modules.ErrorLogging import ErrorLogging


class ModulesApplicationsTestCase( unittest.TestCase ):
  """ Base class for the Modules Applications test cases
  """

  def setUp( self ):

    gLogger.setLevel( 'DEBUG' )
    self.maxDiff = None

  def tearDown( self ):

    for fileProd in ['prodConf_someApp_123_00000456_123_00000456_321.py', 'appLog', 'gaudi_extra_options.py',
                     'applicationError.txt', 'someApp', 'applicationLog.txt']:
      try:
        os.remove( fileProd )
      except OSError:
        continue

#############################################################################
# GaudiApplication.py
#############################################################################

class GaudiApplicationSuccess( ModulesApplicationsTestCase ):

  @patch( "LHCbDIRAC.Workflow.Modules.GaudiApplication.RunApplication", side_effect = MagicMock() )
  @patch( "LHCbDIRAC.Workflow.Modules.GaudiApplication.ModuleBase._manageAppOutput", side_effect = MagicMock() )
  @patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
  def test_execute( self, _patch, _patched, _ppatched ):

    ga = GaudiApplication( bkClient = bkc_mock, dm = dm_mock )
    ga.siteName = 'LCG.PIPPO.org'

    #no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in copy.deepcopy( step_commons ):
        self.assertTrue( ga.execute( prod_id, prod_job_id, wms_job_id,
                                     workflowStatus, stepStatus,
                                     wf_cs, s_cs,
                                     step_number, step_id )['OK'] )


#############################################################################
# GaudiApplicationScript.py
#############################################################################

class GaudiApplicationScriptSuccess( ModulesApplicationsTestCase ):

  @patch( "LHCbDIRAC.Workflow.Modules.GaudiApplicationScript.RunApplication", side_effect = MagicMock() )
  @patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
  def test_execute( self, _patch, _patched ):

    gas = GaudiApplicationScript( bkClient = bkc_mock, dm = dm_mock )

    #no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in copy.deepcopy( step_commons ):
        s_cs['script'] = 'cat'
        self.assertTrue( gas.execute( prod_id, prod_job_id, wms_job_id,
                                      workflowStatus, stepStatus,
                                      wf_cs, s_cs,
                                      step_number, step_id )['OK'] )

#############################################################################
# LHCbScript.py
#############################################################################

class LHCbScriptSuccess( ModulesApplicationsTestCase ):

  def test_execute( self ):

    lhcbScript = LHCbScript()

    lhcbScript.jobType = 'merge'
    lhcbScript.stepInputData = ['foo', 'bar']

    lhcbScript.production_id = prod_id
    lhcbScript.prod_job_id = prod_job_id
    lhcbScript.jobID = wms_job_id
    lhcbScript.workflowStatus = workflowStatus
    lhcbScript.stepStatus = stepStatus
    lhcbScript.workflow_commons = wf_commons
    lhcbScript.step_commons = step_commons[0]
    lhcbScript.step_number = step_number
    lhcbScript.step_id = step_id
    lhcbScript.executable = 'ls'
    lhcbScript.applicationLog = 'applicationLog.txt'

    # no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        lhcbScript.workflow_commons = wf_cs
        lhcbScript.step_commons = s_cs
        lhcbScript._setCommand()
        res = lhcbScript._executeCommand()
        self.assertIsNone( res )


class LHCbScriptFailure( ModulesApplicationsTestCase ):

  def test_execute( self ):

    lhcbScript = LHCbScript()

    lhcbScript.jobType = 'merge'
    lhcbScript.stepInputData = ['foo', 'bar']

    lhcbScript.production_id = prod_id
    lhcbScript.prod_job_id = prod_job_id
    lhcbScript.jobID = wms_job_id
    lhcbScript.workflowStatus = workflowStatus
    lhcbScript.stepStatus = stepStatus
    lhcbScript.workflow_commons = wf_commons
    lhcbScript.step_commons = step_commons[0]
    lhcbScript.step_number = step_number
    lhcbScript.step_id = step_id

    # no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in copy.deepcopy( step_commons ):
        lhcbScript.workflow_commons = wf_cs
        lhcbScript.step_commons = s_cs
        res = lhcbScript.execute()
        self.assertFalse( res['OK'] )

#############################################################################
# RootApplication.py
#############################################################################

class RootApplicationSuccess( ModulesApplicationsTestCase ):

  @patch( "LHCbDIRAC.Workflow.Modules.RootApplication.RunApplication", side_effect = MagicMock() )
  @patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
  def test_execute( self, _patch, _patched ):

    with open('someApp', 'w') as fd:
      fd.write('pippo')

    ra = RootApplication( bkClient = bkc_mock, dm = dm_mock )
    ra.applicationName = 'aRoot.py'
    ra.applicationVersion = 'v1r1'
    ra.rootType = 'py'

    #no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in copy.deepcopy( step_commons ):
        self.assertTrue( ra.execute( prod_id, prod_job_id, wms_job_id,
                                     workflowStatus, stepStatus,
                                     wf_cs, s_cs,
                                     step_number, step_id )['OK'] )


#############################################################################
# ErrorLogging.py
#############################################################################

class ErrorLoggingSuccess( ModulesApplicationsTestCase ):

  @patch( "LHCbDIRAC.Workflow.Modules.ErrorLogging.RunApplication", side_effect = MagicMock() )
  @patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
  def test_execute( self, _patch, _patched ):

    er = ErrorLogging( bkClient = bkc_mock, dm = dm_mock )

    #no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in copy.deepcopy( step_commons ):
        self.assertTrue( er.execute( prod_id, prod_job_id, wms_job_id,
                                     workflowStatus, stepStatus,
                                     wf_cs, s_cs,
                                     step_number, step_id )['OK'] )



if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ModulesApplicationsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiApplicationSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiApplicationScriptSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( LHCbScriptSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( LHCbScriptFailure ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( RootApplicationSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( ErrorLoggingSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
