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
  def getAvailableSteps(self, dict = {}):
    server = self.__getServer()
    return server.getAvailableSteps(dict)
  
  #############################################################################
  def getAvailableFileTypes(self):
    server = self.__getServer()
    retVal = server.getAvailableFileTypes()
    if retVal['OK']:
      records = []
      parameters = ["FileType","Description"]
      for record in retVal['Value']:
        records += [[record[0],record[1]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    return retVal
  
  #############################################################################
  def insertFileTypes(self, ftype, desc):
    server = self.__getServer()
    return server.insertFileTypes(ftype, desc)
  
  #############################################################################
  def insertStep(self, dict):
    server = self.__getServer()
    return server.insertStep(dict)
  
  #############################################################################
  def deleteStep(self, stepid):
    server = self.__getServer()
    return server.deleteStep(int(stepid))
  
  #############################################################################
  def updateStep(self, dict):
    server = self.__getServer()
    return server.updateStep(dict)
  
  #############################################################################
  def getStepInputFiles(self, StepId):
    server = self.__getServer()
    return server.getStepInputFiles(int(StepId))
  
  #############################################################################
  def getStepOutputFiles(self, StepId):
    server = self.__getServer()
    return server.getStepOutputFiles(int(StepId))