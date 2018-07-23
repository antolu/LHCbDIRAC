"""
   LHCbDIRAC - LHCb extension of DIRAC

   References:
    DIRAC: https://github.com/DIRACGrid/DIRAC
    LHCbDIRAC: https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC

   The distributed data production and analysis system of LHCb.
"""

import os

from pkgutil import extend_path
__path__ = extend_path( __path__, __name__ )  # pylint: disable=redefined-builtin

rootPath = os.path.dirname( os.path.realpath( __path__[0] ) )

# Define Version

majorVersion = 9
minorVersion = 1
patchLevel = 20 
preVersion = 0

version = "v%sr%s" % ( majorVersion, minorVersion )
buildVersion = "v%dr%d" % ( majorVersion, minorVersion )
if patchLevel:
  version = "%sp%s" % ( version, patchLevel )
  buildVersion = "%s build %s" % ( buildVersion, patchLevel )
if preVersion:
  version = "%s-pre%s" % ( version, preVersion )
  buildVersion = "%s pre %s" % ( buildVersion, preVersion )
