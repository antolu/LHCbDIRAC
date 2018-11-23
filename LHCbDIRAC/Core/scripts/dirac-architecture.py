#!/usr/bin/env python
"""
  Returns the platform supported by the current WN
"""

__RCSID__ = "$Id$"


def sendMail(msg=''):
  """ send a notification mail when no platform is found
  """
  from DIRAC import gConfig
  from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
  from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations

  mailAddress = Operations().getValue('EMail/JobFailures', 'Vladimir.Romanovskiy@cern.ch')
  site = gConfig.getValue('LocalSite/Site')
  ce = gConfig.getValue('LocalSite/GridCE')
  queue = gConfig.getValue('LocalSite/CEQueue')
  body = "*** THIS IS AN AUTOMATED MESSAGE ***" + '\n\n' + msg + '\n\n'
  body = body + "At site %s, CE = %s, queue = %s" % (site, ce, queue) + '\n\n'

  for mA in mailAddress.replace(' ', '').split(','):
    NotificationClient().sendMail(mailAddress, "Problem with DIRAC architecture",
                                  body, 'federico.stagni@cern.ch', localAttempt=False)


if __name__ == "__main__":
  from DIRAC.Core.Base import Script
  Script.registerSwitch('', 'Full', '   Print additional information on compatible LHCb platforms')
  Script.parseCommandLine(ignoreErrors=True)

  from DIRAC import gLogger, exit as dExit
  import LbPlatformUtils  # pylint: disable=import-error

  try:
    # Get the platform name. If an error occurs, an exception is thrown
    platform = LbPlatformUtils.dirac_platform()
    if not platform:
      gLogger.fatal("There is no platform corresponding to this machine")
      sendMail("There is no platform corresponding to this machine")
      dExit(1)
    print platform
    dExit(0)

  except Exception as e:
    msg = "Exception getting platform: " + repr(e)
    gLogger.exception(msg, lException=e)
    sendMail(msg)
    dExit(1)
