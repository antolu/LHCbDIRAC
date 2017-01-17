""" Unit tests for Workflow Modules utilities
"""

#pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import unittest

from mock import patch

from DIRAC import gLogger
from LHCbDIRAC.Workflow.Modules.GaudiApplication import GaudiApplication

from LHCbDIRAC.Workflow.Modules.GaudiApplicationScript import GaudiApplicationScript


class ModulesApplicationsTestCase( unittest.TestCase ):
  """ Base class for the Modules Applications test cases
  """

  def setUp( self ):

    gLogger.setLevel( 'DEBUG' )
    self.maxDiff = None
    self.ga = GaudiApplication( bkClient = self.bkc_mock, dm = self.dm_mock )
    self.gas = GaudiApplicationScript( bkClient = self.bkc_mock, dm = self.dm_mock )


#############################################################################
# GaudiApplication.py
#############################################################################

class GaudiApplicationSuccess( ModulesApplicationsTestCase ):
  #################################################

#  def test_execute( self ):
# FIXME: difficult to mock
#
#    #no errors, no input data
#    for wf_commons in copy.deepcopy( self.wf_commons ):
#      self.assertTrue( self.ga.execute( self.prod_id, self.prod_job_id, self.wms_job_id,
#                                        self.workflowStatus, self.stepStatus,
#                                        wf_commons, self.step_commons,
#                                        self.step_number, self.step_id,
#                                        MagicMock() )['OK'] )

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
