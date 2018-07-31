""" Test class for BookkeepingReport
"""

#pylint: disable=missing-docstring, invalid-name

__RCSID__ = "$Id$"

from itertools import product

import pytest

# mocks
from DIRAC.DataManagementSystem.Client.test.mock_DM import dm_mock
from LHCbDIRAC.Workflow.Modules.test.mock_Commons import prod_id, prod_job_id, wms_job_id, \
                                                         workflowStatus, stepStatus, step_id, step_number,\
                                                         step_commons, wf_commons
from LHCbDIRAC.BookkeepingSystem.Client.test.mock_BookkeepingClient import bkc_mock

# sut
from LHCbDIRAC.Workflow.Modules.BookkeepingReport import BookkeepingReport


bkr = BookkeepingReport(bkClient=bkc_mock, dm=dm_mock)

allCombinations = list(product(wf_commons, step_commons))

@pytest.mark.parametrize("wf_cs, s_cs", allCombinations)
def test_execute(wf_cs, s_cs):
  assert bkr.execute(prod_id, prod_job_id, wms_job_id,
                     workflowStatus, stepStatus,
                     wf_cs, s_cs,
                     step_number, step_id, False )['OK'] is True
