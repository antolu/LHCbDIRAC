"$Id: "

#!/usr/bin/env python
import os, sys
out = os.system( "dirac-proxy-init -o LogLevel=ALWAYS -t '%s'" % "' '".join( sys.argv[1:] ) )
sys.exit( out / 256 )
