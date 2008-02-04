"""

"""
import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient



class BookkeepingClient:

  def __init__(self):
    self.server = RPCClient('Bookkeeping/BookkeepingManager')

  def echo(self,string):
    res = self.server.echo(string)
    print res
    
  def sendBookkeeping(self, name, data):
      """
      """
      result = self.server.sendBookkeeping(name, data)
      return result
