""" LHCbDIRAC's Resources helper
"""

import LbPlatformUtils


def getDIRACPlatform(platform):
  """ Returns list of compatible platforms.
      Used in JobDB.py

      :param str platform: a string representing a DIRAC platform, e.g. x86_64-centos7.avx2+fma
      :returns: a list of DIRAC platforms that can run platform
  """

  os = platform.split('.')[0].split('-')[1]

  # find the other OS that can run os
  compatibleOSList = [os]
  for canRun, osComp in LbPlatformUtils.OS_COMPATIBILITY.iteritems():
    if os in osComp:
      compatibleOSList.append(canRun)

  # find the microarchitecture that can run the microarchitecture presented in platform
  try:
    microarch = platform.split('.')[1]
  except IndexError:  # this is the case when there's no microarchitecture in platform
    microarch = None

  if microarch:
    compatibleMicroArchsList = LbPlatformUtils.MICROARCH_LEVELS[0:LbPlatformUtils.MICROARCH_LEVELS.index(microarch)]
  else:
    compatibleMicroArchsList = LbPlatformUtils.MICROARCH_LEVELS

  compatiblePlatforms = []
  for co in compatibleOSList:
    for cm in compatibleMicroArchsList:
      compatiblePlatforms.append('x86_64-' + co + '.' + cm)

  return compatiblePlatforms
