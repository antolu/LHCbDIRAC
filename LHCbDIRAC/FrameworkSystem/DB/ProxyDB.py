""" Simple extension of ProxyDB for just modifying the message sent.

    Ideally, we would not need this code at all, and the message sent should just be loaded from somewhere else.
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import Registry
from DIRAC.FrameworkSystem.DB.ProxyDB import ProxyDB as DIRACProxyDB

class ProxyDB( DIRACProxyDB ):
  """ Simple extension for just taking care of the message sent
  """

  def _notifyProxyAboutToExpire( self, userDN, userGroup, lTime, notifLimit ):
    result = Registry.getUsernameForDN( userDN )
    if not result[ 'OK' ]:
      return False
    userName = result[ 'Value' ]
    userEMail = Registry.getUserOption( userName, "Email", "" )
    if not userEMail:
      gLogger.error( "Could not discover user email", userName )
      return False
    daysLeft = int( lTime / 86400 )
    msgSubject = "Your proxy uploaded to LHCbDIRAC will expire in %d days" % daysLeft
    msgBody = """\
Dear %s,

  The proxy you uploaded to LHCbDIRAC will expire in aproximately %d days. The proxy
  information is:

  DN:    %s
  Group: %s

  If you plan on keep using this credentials, please upload a newer proxy to
  LHCbDIRAC by executing (from lxplus.cern.ch, for example):

  $ lb-run -c best LHCbDIRAC/prod lhcb-proxy-init -g %s

  If you have been issued different certificate, please make sure you have a
  proxy uploaded with that certificate.

Cheers,
 LHCbDIRAC's Proxy Manager
""" % ( userName, daysLeft, userDN, userGroup, userGroup )
    fromAddr = self.getFromAddr()
    result = self.__notifClient.sendMail( userEMail, msgSubject, msgBody, fromAddress = fromAddr )
    if not result[ 'OK' ]:
      gLogger.error( "Could not send email", result[ 'Message' ] )
      return False
    return True
