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
""" LHCbDIRAC's Resources helper
"""
import json

import LbPlatformUtils
from six.moves import xmlrpc_client

from DIRAC import S_OK, S_ERROR, gLogger
import DIRAC.ConfigurationSystem.Client.Helpers.Resources

try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

getQueues = DIRAC.ConfigurationSystem.Client.Helpers.Resources.getQueues
getDIRACPlatforms = DIRAC.ConfigurationSystem.Client.Helpers.Resources.getDIRACPlatforms
getCompatiblePlatforms = DIRAC.ConfigurationSystem.Client.Helpers.Resources.getCompatiblePlatforms

DEFAULT_XMLRPCURL = "https://lbsoftdb.cern.ch/read/"
DEFAULT_FALLBACKCACHEPATH = '/cvmfs/lhcb.cern.ch/lib/var/lib/softmetadata/project-platforms.json'


def getDIRACPlatform(platform):
  """ Returns list of compatible platforms.

      Used in JobDB.py instead of DIRAC.ConfigurationSystem.Client.Helpers.Resources.getDIRACPlatform

      :param str platform: a string (or a list with 1 string in)
                           representing a DIRAC platform, e.g. x86_64-centos7.avx2+fma
      :returns: S_ERROR or S_OK() with a list of DIRAC platforms that can run platform (e.g. slc6 can run on centos7)
  """

  # In JobDB.py this function is called with a list in input
  # In LHCb it should always be 1 and 1 only. If it's more there's an issue.
  if isinstance(platform, list):
    if len(platform) > 1:
      return S_ERROR("More than 1 platform specified for the job")
    platform = platform[0]

  if not platform or platform.lower() == 'any':
    return S_OK([])

  return S_OK(LbPlatformUtils.compatible_platforms(platform))


def getPlatformForJob(workflow):
  """ Looks inside the steps definition to find all requested CMTConfigs ("binary tag"),
      then translates it in a DIRAC platform.

      A binary tag is in the form of
        arch+microarch-osversion-gccversion-opt
      e.g.: x86_64+avx2+fma-centos7-gcc7-opt, x86_64-slc6-gcc49-opt

      We want to know what the worklow (the job) requires, so we need to "compose" the requested config
      and then get the minimum DIRAC platform that can run it.
      If, for example, the step1 and step2 respectively requires
        x86_64+avx2+fma-slc6-gcc7-opt
        x86_64-centos7-gcc62-opt
      then we conclude that to run this job we need
        x86_64+avx2+fma-centos7-gcc7-opt
      and so the DIRAC platform x86_64-centos7.avx2+fma

      :returns: a DIRAC platform (a string) or None
  """
  binaryTags = _findBinaryTags(workflow)

  if not binaryTags:
    gLogger.debug("Resources.getPlatformForJob: this job has no specific binary tag requested in any of its steps")
    return None

  return LbPlatformUtils.lowest_common_requirement(binaryTags)


def _findBinaryTags(wf):
  """ developer function
      :returns: set of binary tags found in the workflow, each element is a
      frozenset of platform alternatives
  """
  binaryTags = set()
  for step_instance in wf.step_instances:
    systemConfig = step_instance.findParameter('SystemConfig')
    # If present, prefer the step defined system config
    if systemConfig and systemConfig.value.lower() != 'any':
      binaryTags.add(frozenset([systemConfig.value]))
      continue

    # Else, if there is an application, query the SoftConfDB
    applicationName = step_instance.findParameter('applicationName')
    applicationVersion = step_instance.findParameter('applicationVersion')
    if not (applicationName and applicationVersion):
      continue

    platforms = _listPlatforms(applicationName.value, applicationVersion.value,
                               DEFAULT_XMLRPCURL, DEFAULT_FALLBACKCACHEPATH)

    if platforms:
      binaryTags.add(frozenset(platforms))

  return binaryTags


def _listPlatforms(applicationName, applicationVersion, xmlrpcUrl, fallbackPath):
  """ developer function
      :returns: set of binary tags found for a given application and version
  """
  applicationName = applicationName.upper()
  applicationVersion = applicationVersion.lower()
  platforms = None

  proxy = xmlrpc_client.ServerProxy(xmlrpcUrl, allow_none=True)
  try:
    platforms = proxy.listPlatforms(applicationName, applicationVersion)
  except xmlrpc_client.Fault as e:
    gLogger.error("Failed to find platform in SoftConfDB for", "%s/%s %s" %
                  (applicationName, applicationVersion, e))
  except Exception as e:
    gLogger.error("Unknown exception when querying SoftConfDB", repr(e))

  # If the XML RPC endpoint is down, try to use the cache on CVMFS
  if platforms is None:
    try:
      with open(fallbackPath, 'rt') as fp:
        fallbackCache = json.load(fp)
    except FileNotFoundError:
      gLogger.error("SoftConfDB JSON cache not found in", fallbackPath)
    else:
      try:
        platforms = fallbackCache[applicationName][applicationVersion]
      except KeyError as e:
        gLogger.error("Failed to find platform in cache for", "%s/%s in %s (%s)" %
                      (applicationName, applicationVersion, fallbackPath, e))

  return platforms
