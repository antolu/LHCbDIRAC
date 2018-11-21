""" Test Interfaces API DiracProduction
"""

__RCSID__ = "$Id$"

# import unittest
# import mock

import os

import LHCbDIRAC.Interfaces.API.DiracProduction as moduleTested
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob


def test_LJ():
  lj = LHCbJob()
  open('optionsFiles', 'a').close()
  res = lj.setApplication('appName', 'v1r0', 'optionsFiles', systemConfig='x86_64-slc6-gcc-44-opt')
  assert res['OK'] is True
  res = lj.setApplication('appName', 'v1r0', 'optionsFiles', systemConfig='x86_64-slc5-gcc-41-opt')
  assert res['OK'] is True
  res = lj.setApplication('appName', 'v1r0', 'optionsFiles', systemConfig='x86_64-slc5-gcc-43-opt')
  assert res['OK'] is True
  os.remove('optionsFiles')


def test_instantiate():
  """ tests that we can instantiate one object of the tested class
  """
  testClass = moduleTested.DiracProduction
  prod = testClass(1)
  assert 'DiracProduction' == prod.__class__.__name__
