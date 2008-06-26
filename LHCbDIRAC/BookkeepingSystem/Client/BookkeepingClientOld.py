########################################################################
# $Id: BookkeepingClientOld.py,v 1.1 2008/06/26 16:53:10 zmathe Exp $
########################################################################

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient

__RCSID__ = "$Id: "


class BookkeepingClientOld:

  def __init__(self):
    self.server = RPCClient('BookkeepingOld/BookkeepingManagerOld')

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
      result = self.server.sendBookkeeping(name, data)
      return result
  
  #############################################################################
