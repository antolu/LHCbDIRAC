#!/usr/bin/env python
"""
  Use dirac-proxy-init to get a proxy
"""

import os
import sys

__RCSID__ = "$Id$"


if os.getenv( 'X509_CERT_DIR' ) is None:
  sys.exit( 'the variable X509_CERT_DIR do not exist' )

if not os.path.isdir( os.environ['X509_CERT_DIR'] ):
  sys.exit( 'the directory %s does not exist' % os.environ['X509_CERT_DIR'] )

if os.getenv( 'X509_VOMS_DIR' ) is None:
  sys.exit( 'the variable X509_VOMS_DIR do not exist' )

if not os.path.isdir( os.environ['X509_VOMS_DIR'] ):
  sys.exit( 'the directory %s does not exist' % os.environ['X509_VOMS_DIR'] )

out = os.system( "dirac-proxy-init -o LogLevel=NOTICE --strict --rfc '%s'" % "' '".join( sys.argv[1:] ) )
sys.exit( out / 256 )
