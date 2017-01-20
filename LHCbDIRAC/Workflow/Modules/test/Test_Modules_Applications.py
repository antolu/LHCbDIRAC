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


class ModulesApplicationsTestCase( unittest.TestCase ):
  """ Base class for the Modules Applications test cases
  """

  def setUp( self ):

    gLogger.setLevel( 'DEBUG' )
    self.maxDiff = None
    self.ga = GaudiApplication( bkClient = bkc_mock, dm = dm_mock )
    self.gas = GaudiApplicationScript( bkClient = bkc_mock, dm = dm_mock )
    self.ga.siteName = 'LCG.PIPPO.org'

  def tearDown( self ):

    for fileProd in ['prodConf_someApp_123_00000456_123_00000456_321.py', 'appLog', 'gaudi_extra_options.py', 'applicationError.txt']:
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

 #################################################

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

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ModulesApplicationsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiApplicationSuccess ) )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiApplicationScriptSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
