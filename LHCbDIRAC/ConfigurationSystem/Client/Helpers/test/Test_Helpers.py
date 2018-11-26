""" unit tests for Configuration Helpers
"""

import mock
import pytest

from DIRAC import gLogger

import LHCbDIRAC.ConfigurationSystem.Client.Helpers.Resources as moduleTested

gLogger.setLevel('DEBUG')


@pytest.mark.parametrize("binaryTagsSet, expected", [
    (set(), None),
    ({'x86_64-slc6-gcc48-opt'}, 'x86_64-slc6'),
    ({'x86_64-slc6-gcc62-opt'}, 'x86_64-slc6.sse4_2'),
    ({'x86_64+avx2-slc6-gcc62-opt'}, 'x86_64-slc6.avx2'),
    ({'x86_64+avx2+fma-centos7-gcc7-opt'}, 'x86_64-centos7.avx2+fma'),
    ({'x86_64-slc6-gcc48-opt', 'x86_64-slc6-gcc62-opt'}, 'x86_64-slc6.sse4_2'),
    ({'x86_64-centos7-gcc49-opt', 'x86_64-slc6-gcc62-opt'}, 'x86_64-centos7.sse4_2'),
    ({'x86_64-centos7-gcc49-opt', 'x86_64-slc6-gcc62-opt', 'x86_64+avx2-centos7-gcc62-opt'}, 'x86_64-centos7.avx2'),
    ({'x86_64-centos7-gcc49-opt', 'x86_64+avx-slc6-gcc62-opt', 'x86_64+avx2-centos7-gcc62-opt'}, 'x86_64-centos7.avx2'),
    ({'x86_64+fma-centos7-gcc49-opt', 'x86_64+avx-slc6-gcc62-opt', 'x86_64+avx2-centos7-gcc62-opt'},
     'x86_64-centos7.avx2'),
    ({'x86_64+avx2+fma-centos7-gcc49-opt', 'x86_64+avx-slc6-gcc62-opt', 'x86_64+avx2+fma-centos7-gcc62-opt'},
     'x86_64-centos7.avx2+fma'),
    ({'x86_64+avx2+fma-centos7-gcc49-opt', 'x86_64+avx-slc6-gcc7-opt', 'x86_64+avx2+fma-centos7-gcc62-opt'},
     'x86_64-centos7.avx2+fma'),
    ({'x86_64+fma-centos7-gcc7-opt', 'x86_64+avx-slc6-gcc62-opt'},
     'x86_64-centos7.avx'),
])
def test_getPlatformForJob(binaryTagsSet, expected):
  fbtMock = mock.MagicMock()
  fbtMock.return_value = binaryTagsSet
  moduleTested._findBinaryTags = fbtMock

  res = moduleTested.getPlatformForJob(mock.MagicMock())
  assert res == expected


@pytest.mark.parametrize("platform, expectedRes, expectedValue", [
    ('', True, []),
    ('ANY', True, []),
    (['ANY'], True, []),
    ([None], True, []),
    (['bih', 'boh'], False, []),
    ('x86_64-slc6', True, ['x86_64-slc6', 'x86_64-centos7', 'x86_64-slc6.avx2']),
    ('x86_64-centos7', True, ['x86_64-centos7', 'x86_64-centos7.avx2+fma']),
    ('x86_64-slc5', True, ['x86_64-slc5', 'x86_64-slc6', 'x86_64-slc5.avx2+fma', 'x86_64-slc5.sse4_2']),
    ('x86_64-slc5.avx2', True, ['x86_64-slc5.avx2+fma', 'x86_64-slc6.avx2+fma', 'x86_64-slc6.avx2']),
    ('x86_64-slc6.avx2+fma', True, ['x86_64-slc6.avx2+fma', 'x86_64-centos7.avx2+fma']),
])
def test_getDIRACPlatform(platform, expectedRes, expectedValue):
  res = moduleTested.getDIRACPlatform(platform)
  assert res['OK'] is expectedRes
  if res['OK']:
    assert set(expectedValue).issubset(set(res['Value'])) is True
