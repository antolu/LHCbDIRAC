""" Unit tests for Workflow Modules
"""

#pylint: disable=protected-access, missing-docstring, invalid-name, line-too-long

import unittest
import os
import copy
import shutil

from mock import MagicMock, patch

from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock

from DIRAC import gLogger

# mocks
from LHCbDIRAC.Workflow.Modules.test.mock_Commons import prod_id, prod_job_id, wms_job_id, \
                                                         workflowStatus, stepStatus, step_id, step_number,\
                                                         step_commons, wf_commons

from LHCbDIRAC.BookkeepingSystem.Client.test.mock_BookkeepingClient import bkc_mock



# sut
from LHCbDIRAC.Workflow.Modules.BookkeepingReport import BookkeepingReport

__RCSID__ = "$Id$"

class ModulesTestCase( unittest.TestCase ):
  """ Base class for the Modules test cases
  """
  def setUp( self ):

    gLogger.setLevel( 'DEBUG' )
    self.maxDiff = None

    self.xf_o_mock = MagicMock()
    self.xf_o_mock.inputFileStats = {'a':1, 'b':2}
    self.xf_o_mock.outputFileStats = {'a':1, 'b':2}
    self.xf_o_mock.analyse.return_value = True

  def tearDown( self ):
    for fileProd in ['appLog', 'foo.txt', 'aaa.Bhadron.dst', 'bbb.Calibration.dst', 'bar.py', 'aLongLog.log',
                     'bookkeeping_123_00000456_321.xml',
                     'aLongLog.log.gz', 'ccc.charm.mdst', 'ccc.charm.mdst', 'prova.txt', 'aLog.log',
                     'BAR.txt', 'FooBAR.ext.txt', 'foo_1.txt', 'bar_2.py', 'bar.txt',
                     'ErrorLogging_Step1_coredump.log', '123_00000456_request.xml', 'lfn1', 'lfn2', 'XMLSummaryFile',
                     'aaa.bhadron.dst', 'bbb.calibration.dst', 'ProductionOutputData', 'data.py', '123_00000456_request.json',
                     '00000123_00000456.tar', 'someOtherDir', 'DISABLE_WATCHDOG_CPU_WALLCLOCK_CHECK', 'myfoo.blah',
                     'prodConf_someApp__.py', 'prodConf_someApp___.py']:
      try:
        os.remove( fileProd )
      except OSError:
        continue

    for directory in ['./job', 'job']:
      try:
        shutil.rmtree( directory )
      except:
        continue


#############################################################################
# BookkeepingReport.py
#############################################################################

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class BookkeepingReportFailure( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        bkr = BookkeepingReport( bkClient = bkc_mock, dm = dm_mock )
        self.assertFalse( bkr.execute( prod_id, prod_job_id, wms_job_id,
                                       workflowStatus, stepStatus,
                                       wf_cs, s_cs,
                                       step_number, step_id, False )['OK'] )

        s_cs.pop( 'XMLSummary', '' )
        bkr = BookkeepingReport( bkClient = bkc_mock, dm = dm_mock )
        self.assertFalse( bkr.execute( prod_id, prod_job_id, wms_job_id,
                                       workflowStatus, stepStatus,
                                       wf_cs, s_cs,
                                       step_number, step_id, False )['OK'] )

@patch( "LHCbDIRAC.Workflow.Modules.ModuleBase.RequestValidator", side_effect = MagicMock() )
class BookkeepingReportSuccess( ModulesTestCase ):

  #################################################

  def test_execute( self, _patch ):

    # no errors, no input data
    for wf_cs in copy.deepcopy( wf_commons ):
      for s_cs in step_commons:
        bkr = BookkeepingReport( bkClient = bkc_mock, dm = dm_mock )
        # no errors, no input data
        bkr.siteName = 'DIRAC.Test.ch'
        s_cs['XMLSummary_o'] = self.xf_o_mock
        res = bkr.execute( prod_id, prod_job_id, wms_job_id,
                           workflowStatus, stepStatus,
                           wf_cs, s_cs,
                           step_number, step_id, True )
        self.assertTrue( res['OK'] )


        res = bkr.execute( prod_id, prod_job_id, wms_job_id,
                           workflowStatus, stepStatus,
                           wf_cs, s_cs,
                           step_number, step_id, False )
        self.assertTrue( res['OK'] )

#############################################################################
# Test Suite run
#############################################################################

if __name__ == '__main__':
  unittest.main()

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
