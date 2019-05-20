""" LHCbDIRAC's Resources helper
"""

import LbPlatformUtils

from DIRAC import S_OK, S_ERROR, gLogger

import DIRAC.ConfigurationSystem.Client.Helpers.Resources
getQueues = DIRAC.ConfigurationSystem.Client.Helpers.Resources.getQueues
getDIRACPlatforms = DIRAC.ConfigurationSystem.Client.Helpers.Resources.getDIRACPlatforms
getCompatiblePlatforms = DIRAC.ConfigurationSystem.Client.Helpers.Resources.getCompatiblePlatforms


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

  archSet = set()
  microarchSet = set()
  osVersionSet = set()
  gccVersionSet = set()
  binaryTags = _findBinaryTags(workflow)

  if not binaryTags:
    gLogger.debug("Resources.getPlatformForJob: this job has no specific binary tag requested in any of its steps")
    return None

  return LbPlatformUtils.lowest_common_requirement(binaryTags)


def _findBinaryTags(wf):
  """ developer function
      :returns: set of binary tags found in the workflow
  """
  binaryTags = set()
  for step_instance in wf.step_instances:
    for parameter in step_instance.parameters:
      if parameter.name.lower() == 'systemconfig':  # this is where the CMTConfigs ("binary tags") are stored
        if parameter.value and parameter.value.lower() != 'any':  # 'ANY' is a wildcard
          binaryTags.add(parameter.value)
  return binaryTags
