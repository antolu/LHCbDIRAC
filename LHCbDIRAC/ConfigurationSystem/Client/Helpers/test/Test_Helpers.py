###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
""" unit tests for Configuration Helpers
"""

import mock
import pytest

from DIRAC import gLogger

import LHCbDIRAC.ConfigurationSystem.Client.Helpers.Resources as moduleTested

gLogger.setLevel('DEBUG')


@pytest.mark.parametrize("binaryTagsSet, expected", [
    (set(), None),
    ({frozenset({'x86_64-slc6-gcc48-opt'})}, 'x86_64-slc6'),
    ({frozenset({'x86_64-slc6-gcc62-opt'})}, 'nehalem-slc6'),
    ({frozenset({'x86_64+avx2-slc6-gcc62-opt'})}, 'haswell-slc6'),
    ({frozenset({'x86_64+avx2+fma-centos7-gcc7-opt'})}, 'haswell-centos7'),
    ({frozenset({'x86_64-slc6-gcc48-opt'}),
      frozenset({'x86_64-slc6-gcc62-opt'})}, 'nehalem-slc6'),
    ({frozenset({'x86_64-centos7-gcc49-opt'}),
      frozenset({'x86_64-slc6-gcc62-opt'})}, 'nehalem-centos7'),
    ({frozenset({'x86_64-centos7-gcc49-opt'}),
      frozenset({'x86_64-slc6-gcc62-opt'}),
      frozenset({'x86_64+avx2-centos7-gcc62-opt'})}, 'haswell-centos7'),
    ({frozenset({'x86_64-centos7-gcc49-opt'}),
      frozenset({'x86_64+avx-slc6-gcc62-opt'}),
      frozenset({'x86_64+avx2-centos7-gcc62-opt'})}, 'haswell-centos7'),
    ({frozenset({'x86_64+fma-centos7-gcc49-opt'}),
      frozenset({'x86_64+avx-slc6-gcc62-opt'}),
      frozenset({'x86_64+avx2-centos7-gcc62-opt'})},
     'haswell-centos7'),
    ({frozenset({'x86_64+avx2+fma-centos7-gcc49-opt'}),
      frozenset({'x86_64+avx-slc6-gcc62-opt'}),
      frozenset({'x86_64+avx2+fma-centos7-gcc62-opt'})},
     'haswell-centos7'),
    ({frozenset({'x86_64+avx2+fma-centos7-gcc49-opt'}),
      frozenset({'x86_64+avx-slc6-gcc7-opt'}),
      frozenset({'x86_64+avx2+fma-centos7-gcc62-opt'})},
     'haswell-centos7'),
    ({frozenset({'x86_64+fma-centos7-gcc7-opt'}),
      frozenset({'x86_64+avx-slc6-gcc62-opt'})},
     'haswell-centos7'),
    ({frozenset({'x86_64-slc5-gcc49-opt'}),
      frozenset({'x86_64+fma-centos7-gcc7-opt'})},
     'haswell-any'),
    ({frozenset({'x86_64-slc5-gcc49-opt'}),
      frozenset({'x86_64+fma-centos7-gcc7-opt', 'x86_64+fma-slc6-gcc7-opt'})},
     'haswell-slc6'),
    ({frozenset({'x86_64-slc5-gcc49-opt', 'x86_64-slc6-gcc49-opt'}),
      frozenset({'x86_64+fma-centos7-gcc7-opt', 'x86_64-slc6-gcc7-opt'})},
     'nehalem-slc6'),
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
    ('x86_64-slc6', True, ['cannonlake-centos7', 'skylake_avx512-centos7', 'skylake-centos7', 'broadwell-centos7',
                           'haswell-centos7', 'ivybridge-centos7', 'sandybridge-centos7', 'westmere-centos7',
                           'nehalem-centos7', 'core2-centos7', 'x86_64-centos7', 'cannonlake-slc6',
                           'skylake_avx512-slc6', 'skylake-slc6', 'broadwell-slc6', 'haswell-slc6', 'ivybridge-slc6',
                           'sandybridge-slc6', 'westmere-slc6', 'nehalem-slc6', 'core2-slc6', 'x86_64-slc6']),
    ('x86_64-centos7', True, ['cannonlake-centos7', 'skylake_avx512-centos7', 'skylake-centos7', 'broadwell-centos7',
                              'haswell-centos7', 'ivybridge-centos7', 'sandybridge-centos7', 'westmere-centos7',
                              'nehalem-centos7', 'core2-centos7', 'x86_64-centos7']),
    ('x86_64-slc5', True, ['cannonlake-slc6', 'skylake_avx512-slc6', 'skylake-slc6', 'broadwell-slc6', 'haswell-slc6',
                           'ivybridge-slc6', 'sandybridge-slc6', 'westmere-slc6', 'nehalem-slc6', 'core2-slc6',
                           'x86_64-slc6', 'cannonlake-slc5', 'skylake_avx512-slc5', 'skylake-slc5', 'broadwell-slc5',
                           'haswell-slc5', 'ivybridge-slc5', 'sandybridge-slc5', 'westmere-slc5', 'nehalem-slc5',
                           'core2-slc5', 'x86_64-slc5']),
    ('x86_64-slc5.avx2', True, ['cannonlake-slc6', 'skylake_avx512-slc6', 'skylake-slc6', 'broadwell-slc6',
                                'haswell-slc6', 'cannonlake-slc5', 'skylake_avx512-slc5', 'skylake-slc5',
                                'broadwell-slc5', 'haswell-slc5']),
    ('x86_64-slc6.avx2+fma', True, ['cannonlake-centos7', 'skylake_avx512-centos7', 'skylake-centos7',
                                    'broadwell-centos7', 'haswell-centos7', 'cannonlake-slc6', 'skylake_avx512-slc6',
                                    'skylake-slc6', 'broadwell-slc6', 'haswell-slc6']),
])
def test_getDIRACPlatform(platform, expectedRes, expectedValue):
  res = moduleTested.getDIRACPlatform(platform)
  assert res['OK'] is expectedRes
  if res['OK']:
    assert set(expectedValue).issubset(set(res['Value'])) is True


@pytest.mark.parametrize("applicationName, applicationVersion, expected", [
    ('DaVinci', 'v44r4', ['x86_64+avx2+fma-centos7-gcc62-opt', 'x86_64-centos7-gcc62-dbg', 'x86_64-centos7-gcc62-do0',
                          'x86_64-centos7-gcc62-opt', 'x86_64-centos7-gcc7-dbg', 'x86_64-centos7-gcc7-opt',
                          'x86_64-slc6-gcc62-dbg', 'x86_64-slc6-gcc62-opt']),
    ('DaVinci', 'v0r0', None),
    ('InvalidProject', 'v1r0', None),
    ('CASTELAO', 'v1r5p1', ['x86_64-centos7-gcc62-do0', 'x86_64-centos7-gcc62-opt', 'x86_64-centos7-gcc62-dbg',
                            'x86_64-slc6-gcc62-opt', 'x86_64-centos7-gcc7-dbg', 'x86_64+avx2+fma-centos7-gcc62-opt',
                            'x86_64-slc6-gcc62-dbg', 'x86_64-centos7-gcc7-opt']),
])
def test_listPlatforms(applicationName, applicationVersion, expected):
  # Good RPC and good fallback
  result = moduleTested._listPlatforms(
      applicationName, applicationVersion,
      moduleTested.DEFAULT_XMLRPCURL,
      moduleTested.DEFAULT_FALLBACKCACHEPATH)
  assert result is None is expected or set(result) == set(expected)

  # Bad RPC and good fallback
  result = moduleTested._listPlatforms(
      applicationName, applicationVersion,
      'https://lbsoftdb.cern.invalid/read/',
      moduleTested.DEFAULT_FALLBACKCACHEPATH)
  assert result is None is expected or set(result) == set(expected)

  # Good RPC and bad fallback
  result = moduleTested._listPlatforms(
      applicationName, applicationVersion,
      moduleTested.DEFAULT_XMLRPCURL,
      '/cvmfs/lhcb.cern.invalid/lib/var/lib/softmetadata/project-platforms.json')
  assert result is None is expected or set(result) == set(expected)

  # Bad RPC and bad fallback
  result = moduleTested._listPlatforms(
      applicationName, applicationVersion,
      'https://lbsoftdb.cern.invalid/read/',
      '/cvmfs/lhcb.cern.invalid/lib/var/lib/softmetadata/project-platforms.json')
  assert result is None
