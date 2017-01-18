""" Unit tests for Workflow Modules utilities
"""

#pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import unittest
import copy

from mock import patch

from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from LHCbDIRAC.BookkeepingSystem.Client.test.mock_BookkeepingClient import bkc_mock

from DIRAC import gLogger

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

    self.prod_id = '123'
    self.prod_job_id = '00000456'
    self.wms_job_id = 12345
    self.workflowStatus = {'OK':True}
    self.stepStatus = {'OK':True}


#############################################################################
# GaudiApplication.py
#############################################################################

class GaudiApplicationSuccess( ModulesApplicationsTestCase ):
  #################################################

  def test_execute( self ):

    #no errors, no input data
    for wf_commons in copy.deepcopy( self.wf_commons ):
      self.assertTrue( self.ga.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
                                        self.workflowStatus, self.stepStatus,
                                        wf_commons, self.step_commons,
                                        self.step_number, self.step_id,
                                        MagicMock() )['OK'] )

#############################################################################
# GaudiApplicationScript.py
#############################################################################

# class GaudiApplicationScriptSuccess( ModulesTestCase ):
#
#  #################################################
#
#  def test_execute( self ):
# #FIXME: difficult to mock
#
#    self.step_commons['script'] = 'cat'
#    #no errors, no input data
#    for wf_commons in copy.deepcopy( self.wf_commons ):
#      self.assertTrue( self.gas.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                        self.workflowStatus, self.stepStatus,
#                                        wf_commons, self.step_commons,
#                                        self.step_number, self.step_id,
#                                        ['aa', 'bb'] )['OK'] )
  pass

if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase( ModulesApplicationsTestCase )
  suite.addTest( unittest.defaultTestLoader.loadTestsFromTestCase( GaudiApplicationSuccess ) )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )
