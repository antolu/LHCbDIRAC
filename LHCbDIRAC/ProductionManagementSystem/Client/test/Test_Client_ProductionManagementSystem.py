""" Test of the ProductionRequest and Production modules
"""

import pytest

from LHCbDIRAC.ProductionManagementSystem.Client.Production import Production

# Production.py
prod = Production()


@pytest.mark.parametrize("input, expected", [
    (['T1', 'T2'], [{'outputDataType': 't1'}, {'outputDataType': 't2'}]),
    (['T1', 'HIST'], [{'outputDataType': 't1'}, {'outputDataType': 'hist'}])
])
def test__constructOutputFilesList(input, expected):
  res = prod._constructOutputFilesList(input)
  assert res == expected
