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
"""
   LHCbDIRAC - LHCb extension of DIRAC

   References:
    DIRAC: https://github.com/DIRACGrid/DIRAC
    LHCbDIRAC: https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC

   The distributed data production and analysis system of LHCb.
"""

import os

from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)  # pylint: disable=redefined-builtin

rootPath = os.path.dirname(os.path.realpath(__path__[0]))

# Define Version

majorVersion = 9
minorVersion = 3
patchLevel = 6
preVersion = 0

version = "v%sr%s" % (majorVersion, minorVersion)
buildVersion = "v%dr%d" % (majorVersion, minorVersion)
if patchLevel:
  version = "%sp%s" % (version, patchLevel)
  buildVersion = "%s build %s" % (buildVersion, patchLevel)
if preVersion:
  version = "%s-pre%s" % (version, preVersion)
  buildVersion = "%s pre %s" % (buildVersion, preVersion)
