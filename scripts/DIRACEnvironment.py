# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/DIRACEnvironment.py,v 1.1 2007/05/16 12:45:35 acasajus Exp $
__RCSID__ = "$Id: DIRACEnvironment.py,v 1.1 2007/05/16 12:45:35 acasajus Exp $"
"""
   Magic file to be imported by all python scripts to properly discover and 
   setup the DIRAC environment
"""

import os, sys

try:
  import DIRAC
except ImportError:
  """
     from the location of the script that import this one (this should only be
     possible if they are in the same directory) tries to setup the PYTHONPATH
  """
  scriptsPath = os.path.realpath( os.path.dirname( __file__ ) )
  rootPath = os.path.realpath( "%s/.." % scriptsPath )  

  sys.path.insert( 0, rootPath )

  try:
    import DIRAC
  except ImportError:
    print "Can not import DIRAC."
    print "Check if %s contains a proper DIRAC distribution" % rootPath
    raise
    sys.exit(-1)

