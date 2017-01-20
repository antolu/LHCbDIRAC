""" Unit tests for Workflow Modules utilities
"""

#pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import unittest
import copy
import os

from mock import MagicMock, patch

from DIRAC import S_OK, gLogger

from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from LHCbDIRAC.BookkeepingSystem.Client.test.mock_BookkeepingClient import bkc_mock
from LHCbDIRAC.Workflow.Modules.test.mock_Commons import prod_id, prod_job_id, wms_job_id, \
                                                         workflowStatus, stepStatus, step_id, step_number,\
                                                         step_commons, wf_commons

from LHCbDIRAC.Workflow.Modules.GaudiApplication import GaudiApplication
from LHCbDIRAC.Workflow.Modules.GaudiApplicationScript import GaudiApplicationScript
from LHCbDIRAC.Workflow.Modules.RootApplication import RootApplication
from LHCbDIRAC.Workflow.Modules.LHCbScript import LHCbScript

def mock_getScriptsLocation():
  return S_OK( { 'LbLogin.sh':'/this/is/a/path/groupLoginPath',
                 'projectEnv':'/this/is/a/path/projectScriptPath',
                 'MYSITEROOT':'softwareArea'} )

def mock_runEnvironmentScripts( _var ):
  return S_OK({'a':'aa', 'b':'bb'})



class ModulesApplicationsTestCase( unittest.TestCase ):
  """ Base class for the Modules Applications test cases
  """

  def setUp( self ):

    gLogger.setLevel( 'DEBUG' )
    self.maxDiff = None
    self.ga = GaudiApplication( bkClient = bkc_mock, dm = dm_mock )
    self.gas = GaudiApplicationScript( bkClient = bkc_mock, dm = dm_mock )
    self.ga.siteName = 'LCG.PIPPO.org'
    self.ra = RootApplication( bkClient = bkc_mock, dm = dm_mock )
    self.ra.applicationName = 'aRoot.py'
    self.ra.applicationVersion = 'v1r1'
    self.ra.rootType = 'py'
    self.lhcbScript = LHCbScript()

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
  def test_execute( self, _patch, _patched ):

    #no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in copy.deepcopy( step_commons ):
        self.assertTrue( self.ga.execute( prod_id, prod_job_id, wms_job_id,
                                          workflowStatus, stepStatus,
                                          wf_cs, s_cs,
                                          step_number, step_id )['OK'] )


#############################################################################
# GaudiApplicationScript.py
#############################################################################

class GaudiApplicationScriptSuccess( ModulesApplicationsTestCase ):

  @patch( "LHCbDIRAC.Workflow.Modules.GaudiApplicationScript.RunApplication", side_effect = MagicMock() )
  def test_execute( self, _patch ):

    #no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in copy.deepcopy( step_commons ):
        s_cs['script'] = 'cat'
        self.assertTrue( self.gas.execute( prod_id, prod_job_id, wms_job_id,
                                           workflowStatus, stepStatus,
                                           wf_cs, s_cs,
                                           step_number, step_id )['OK'] )

#############################################################################
# LHCbScript.py
#############################################################################

class LHCbScriptSuccess( ModulesApplicationsTestCase ):

  @patch( 'LHCbDIRAC.Workflow.Modules.LHCbScript.getScriptsLocation', side_effect = mock_getScriptsLocation )
  @patch( 'LHCbDIRAC.Workflow.Modules.LHCbScript.runEnvironmentScripts', side_effect = mock_runEnvironmentScripts )
  def test_execute( self, _mockScriptsLocation, _mockrunEnvironmentScripts ):

    self.lhcbScript.jobType = 'merge'
    self.lhcbScript.stepInputData = ['foo', 'bar']

    self.lhcbScript.production_id = prod_id
    self.lhcbScript.prod_job_id = prod_job_id
    self.lhcbScript.jobID = wms_job_id
    self.lhcbScript.workflowStatus = workflowStatus
    self.lhcbScript.stepStatus = stepStatus
    self.lhcbScript.workflow_commons = wf_commons
    self.lhcbScript.step_commons = step_commons[0]
    self.lhcbScript.step_number = step_number
    self.lhcbScript.step_id = step_id
    self.lhcbScript.executable = 'ls'
    self.lhcbScript.applicationLog = 'applicationLog.txt'

    # no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        self.lhcbScript.workflow_commons = wf_cs
        self.lhcbScript.step_commons = s_cs
        self.lhcbScript._setCommand()
        res = self.lhcbScript._executeCommand()
        self.assertIsNone( res )


class LHCbScriptFailure( ModulesApplicationsTestCase ):

  @patch( 'LHCbDIRAC.Workflow.Modules.LHCbScript.getScriptsLocation', side_effect = mock_getScriptsLocation )
  @patch( 'LHCbDIRAC.Workflow.Modules.LHCbScript.runEnvironmentScripts', side_effect = mock_runEnvironmentScripts )
  def test_execute( self, _mockScriptsLocation, _mockrunEnvironmentScripts ):

    self.lhcbScript.jobType = 'merge'
    self.lhcbScript.stepInputData = ['foo', 'bar']

    self.lhcbScript.production_id = prod_id
    self.lhcbScript.prod_job_id = prod_job_id
    self.lhcbScript.jobID = wms_job_id
    self.lhcbScript.workflowStatus = workflowStatus
    self.lhcbScript.stepStatus = stepStatus
    self.lhcbScript.workflow_commons = wf_commons
    self.lhcbScript.step_commons = step_commons[0]
    self.lhcbScript.step_number = step_number
    self.lhcbScript.step_id = step_id

    # no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in copy.deepcopy( step_commons ):
        self.lhcbScript.workflow_commons = wf_cs
        self.lhcbScript.step_commons = s_cs
        res = self.lhcbScript.execute()
        self.assertFalse( res['OK'] )

#############################################################################
# RootApplication.py
#############################################################################

class RootApplicationSuccess( ModulesApplicationsTestCase ):

  @patch( "LHCbDIRAC.Workflow.Modules.RootApplication.RunApplication", side_effect = MagicMock() )
  def test_execute( self, _patch ):

    with open('someApp', 'w') as fd:
      fd.write('pippo')

    #no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in copy.deepcopy( step_commons ):
        self.assertTrue( self.ra.execute( prod_id, prod_job_id, wms_job_id,
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
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
