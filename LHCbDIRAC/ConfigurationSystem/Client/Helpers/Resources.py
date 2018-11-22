""" LHCbDIRAC's Resources helper
"""

import LbPlatformUtils  # pylint: disable=import-error

from DIRAC import S_OK, S_ERROR, gLogger


def getDIRACPlatform(platform):
  """ Returns list of compatible platforms.
      Used in JobDB.py

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

  # understanding what's what
  try:
    microarch = platform.split('.')[1]
  except IndexError:  # this is the case when there's no microarchitecture in platform
    microarch = None
  finally:
    archV = platform.split('-')[0]

  osV = platform.split('.')[0].split('-')[1]

  # find the other arch that can run os
  compatibleArchList = [archV]
  for canRun, archComp in LbPlatformUtils.ARCH_COMPATIBILITY.iteritems():
    if archV in archComp:
      compatibleArchList.append(canRun)

  # find the microarchitecture that can run the microarchitecture presented in platform
  if microarch:
    compatibleMicroArchsList = LbPlatformUtils.MICROARCH_LEVELS[0:LbPlatformUtils.MICROARCH_LEVELS.index(microarch) + 1]
  else:
    compatibleMicroArchsList = LbPlatformUtils.MICROARCH_LEVELS

  # find the other OS that can run os
  compatibleOSList = [osV]
  for canRun, osComp in LbPlatformUtils.OS_COMPATIBILITY.iteritems():
    if osV in osComp:
      compatibleOSList.append(canRun)

  compatiblePlatforms = {platform}
  for ar in compatibleArchList:
    for co in compatibleOSList:
      if len(compatibleOSList) > 1 and compatibleOSList.index(co) != 0 and not microarch:
        compatiblePlatforms.add(ar + '-' + co)
      for cm in compatibleMicroArchsList:
        compatiblePlatforms.add(ar + '-' + co + '.' + cm)

  return S_OK(list(compatiblePlatforms))


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
  for bt in binaryTags:
    # arch+microarch-osversion-gccversion-opt
    fulllarch, osversion, gccversion = bt.split('-')[0:3]
    try:
      arch, microarch = fulllarch.split('+', 1)
      archSet.add(arch)
      microarchSet.add(microarch)
    except ValueError:
      archSet.add(fulllarch)
    osVersionSet.add(osversion)
    gccVersionSet.add(gccversion)

  archCompatibilitiesInAList = []
  archCompatibilities = dict(LbPlatformUtils.ARCH_COMPATIBILITY)
  while True:
    archsCompatible = set()
    for archCompatibleList in archCompatibilities.itervalues():
      for archCompatible in archCompatibleList:
        archsCompatible.add(archCompatible)
    for o in archCompatibilities.iterkeys():
      if o not in archsCompatible:
        highestArch = o
    try:
      archCompatibilities.pop(highestArch)
    except KeyError:
      break
    archCompatibilitiesInAList.append(highestArch)

  for ar in archCompatibilitiesInAList:
    if ar in archSet:
      break

  ma = ''
  for mal in LbPlatformUtils.MICROARCH_LEVELS:
    if mal in microarchSet:
      ma = '+' + mal
      break

  osCompatibilityInAList = []
  osCompatibilities = dict(LbPlatformUtils.OS_COMPATIBILITY)
  while True:
    ossCompatible = set()
    for osCompatibleList in osCompatibilities.itervalues():
      for osCompatible in osCompatibleList:
        ossCompatible.add(osCompatible)
    for o in osCompatibilities.iterkeys():
      if o not in ossCompatible:
        highestOS = o
    try:
      osCompatibilities.pop(highestOS)
    except KeyError:
      break
    osCompatibilityInAList.append(highestOS)

  for osV in osCompatibilityInAList:
    if osV in osVersionSet:
      break

  gccV = sorted(list(gccVersionSet))[-1]

  # Now, we compose the binary tag
  composedBinaryTag = ar + ma + '-' + osV + '-' + gccV + '-opt'
  gLogger.verbose("Resources.getPlatformForJob: binary tag composed: %s" % composedBinaryTag)

  try:
    platform = LbPlatformUtils.requires(composedBinaryTag)
  except BaseException as error:
    gLogger.exception("Exception while getting platform, don't set it", lException=error)
    return None
  return platform


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
