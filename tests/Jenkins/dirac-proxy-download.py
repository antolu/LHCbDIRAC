#!/usr/bin/env python
""" Get a proxy from the proxy manager
"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import os
uid = os.getuid()
from DIRAC.FrameworkSystem.Client.ProxyManagerClient        import gProxyManager

res = gProxyManager.downloadProxyToFile( '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=fstagni/CN=693025/CN=Federico Stagni',
                                         'lhcb_prmgr', limited = False, requiredTimeLeft = 1200,
                                         cacheTime = 43200, filePath = '/tmp/x509up_u%s' % uid, proxyToConnect = False,
                                         token = False )

if not res['OK']:
  print "Error downloading proxy", res['Message']
  exit( 1 )
