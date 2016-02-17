#!/usr/bin/env python
"""
  Use dirac-proxy-init to get a proxy
"""


__RCSID__ = "$Id$"

import os, sys
if os.getenv( 'X509_CERT_DIR' ) == None:
  sys.exit( 'the variable X509_CERT_DIR do not exist' )

if not os.path.isdir( os.environ['X509_CERT_DIR'] ):
  sys.exit( 'the directory %s does not exist' % os.environ['X509_CERT_DIR'] )

if os.getenv( 'X509_VOMS_DIR' ) == None:
  sys.exit( 'the variable X509_VOMS_DIR do not exist' )

if not os.path.isdir( os.environ['X509_VOMS_DIR'] ):
  sys.exit( 'the directory %s does not exist' % os.environ['X509_VOMS_DIR'] )

out = os.system( "dirac-proxy-init -o LogLevel=NOTICE -t '%s'" % "' '".join( sys.argv[1:] ) )
sys.exit( out / 256 )
