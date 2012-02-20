#!/usr/bin/env python
"""
  Use dirac-proxy-init to get a proxy
"""


__RCSID__ = "$Id: "

import os, sys
out = os.system( "dirac-proxy-init -o LogLevel=NOTICE -t '%s'" % "' '".join( sys.argv[1:] ) )
sys.exit( out / 256 )
