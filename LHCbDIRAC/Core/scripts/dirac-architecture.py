#!/usr/bin/env python
""" Returns the architecture tag to be used for CMTCONFIG or architecture
"""

__RCSID__ = "$Id$"

import platform

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine( ignoreErrors = True )

from DIRAC import exit as dExit
from LHCbDIRAC.Core.Utilities.ProductionEnvironment import getPlatform

if __name__ == "__main__" :
  try:
    # Get the platform name. If an error occurs, an exception is thrown
    platform = getPlatform()
    print platform
    dExit( 0 )

  except Exception as e:
    msg = "Exception getting platform: " + repr( e )

    from DIRAC import gConfig
    from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
    from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

    mailAddress = Operations().getValue( 'EMail/JobFailures', 'Vladimir.Romanovskiy@cern.ch' )
    site = gConfig.getValue( 'LocalSite/Site' )
    ce = gConfig.getValue( 'LocalSite/GridCE' )
    queue = gConfig.getValue( 'LocalSite/CEQueue' )
    body = "*** THIS IS AN AUTOMATED MESSAGE ***" + '\n\n' + msg + '\n\n'
    body = body + "At site %s, CE = %s, queue = %s" % ( site, ce, queue ) + '\n\n'
    body = body + "Consider inserting it in the OSCompatibility section of the CS"

    for mA in mailAddress.replace( ' ', '' ).split( ',' ):
      NotificationClient().sendMail( mailAddress, "Problem with DIRAC architecture",
                                     body, 'federico.stagni@cern.ch', localAttempt = False )
    print msg
    dExit( 1 )
