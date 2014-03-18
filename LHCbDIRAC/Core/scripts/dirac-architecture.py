#!/usr/bin/env python
""" Returns the architecture tag to be used for CMTCONFIG or architecture
"""

__RCSID__ = "$Id"

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine( ignoreErrors = True )

import sys, platform
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatform

if __name__ == "__main__" :
  dist = platform.linux_distribution()
  if not dist[0]:
    # is it mac?
    dist = platform.mac_ver()
    if not dist[0]:
      # windows, really?
      dist = platform.win32_ver()

  OS = '_'.join( dist ).replace( ' ', '' )

  error = False
  res = getDIRACPlatform( '_'.join( [platform.machine(), OS] ) )
  if not res['OK']:
    msg = "ERROR: %s" % res['Message']
    error = True
  elif not res['Value']:
    msg = "ERROR, %s not found" % '_'.join( [platform.machine(), OS] )
    error = True
  else:
    msg = res['Value'][0]
    
  print msg
  
  if error:
    from DIRAC import gConfig
    site = gConfig.getValue( 'LocalSite/Site' )
    ce = gConfig.getValue( 'LocalSite/GridCE' )
    queue = gConfig.getValue( 'LocalSite/CEQueue' )
    body = "*** THIS IS AN AUTOMATED MESSAGE" + '\n\n' + msg + '\n\n'
    body = body + "At site %s, CE = %s, queue = %s" % ( site, ce, queue )
    from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
    NotificationClient().sendMail('federico.stagni@cern.ch', 'Problem with DIRAC architecture', 
                                  body, 'federico.stagni@cern.ch', localAttempt = False)
    
