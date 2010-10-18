########################################################################
# $Id$
########################################################################

"""

"""
import DIRAC
from DIRAC                           import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient      import RPCClient
from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.Utilities            import DEncode
from DIRAC.Core.Base                 import Script
import types,cPickle,os, tempfile

Script.parseCommandLine( ignoreErrors = True )

__RCSID__ = "$Id$"

class BookkeepingClient:

  def __init__( self, rpcClient = None ):
    self.rpcClient = rpcClient

  def __getServer(self,timeout=600):
    if self.rpcClient:
      return self.rpcClient
    else:
      return RPCClient('Bookkeeping/BookkeepingManager', timeout=timeout)

  #############################################################################
  def echo(self,string):
    server = self.__getServer()
    res = server.echo(string)
    print res

  #############################################################################
  def sendBookkeeping(self, name, data):
      """
      Send XML file to BookkeepingManager.
      name- XML file name
      data - XML file
      """
      server = self.__getServer()
      result = server.sendBookkeeping(name, data)
      return result
  
  #############################################################################
  def getAvailableSteps(self):
    server = self.__getServer()
    res = server.getAvailableSteps()
    print res