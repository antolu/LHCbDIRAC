########################################################################
# $Id$
########################################################################

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Base import Script
Script.parseCommandLine()

__RCSID__ = "$Id: "


class BookkeepingClientOld:

  def __init__(self):
    pass

  #############################################################################
  def echo(self,string):
    res = self.server.echo(string)
    print res

  #############################################################################
  def sendBookkeeping(self, name, data):
      """
      Send XML file to BookkeepingManager.
      name- XML file name
      data - XML file
      """
      server = RPCClient('Bookkeeping/BookkeepingManagerOld')
      result = server.sendBookkeeping(name, data)
      return result

  #############################################################################
