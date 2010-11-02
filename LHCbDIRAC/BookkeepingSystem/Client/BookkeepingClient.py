########################################################################
# $Id$
########################################################################

"""
dict = {'EventTypeId': 93000000, 
        'ConfigVersion': 'Collision10', 
        'ProcessingPass': '/Real Data', 
        'ConfigName': 'LHCb', 
        'ConditionDescription': 'Beam3500GeV-VeloClosed-MagDown',
        'Production':7421
         }
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
  def setStepInputFiles(self, stepid, files):
    server = self.__getServer()
    return server.setStepInputFiles(stepid, files)
  
  #############################################################################
  def setStepOutputFiles(self, stepid, files):
    server = self.__getServer()
    return server.setStepOutputFiles(stepid, files)
  
  #############################################################################
  def getStepOutputFiles(self, StepId):
    server = self.__getServer()
    return server.getStepOutputFiles(int(StepId))
  
  #############################################################################
  def getAvailableConfigNames(self):
    server = self.__getServer()
    return server.getAvailableConfigNames()
  
  #############################################################################
  def getConfigVersions(self, dict):
    server = self.__getServer()
    return server.getConfigVersions(dict)
  
  #############################################################################
  def getConditions(self, dict):
    server = self.__getServer()
    return server.getConditions(dict)
  
  #############################################################################
  def getProcessingPass(self, dict, path = '/'):
    server = self.__getServer()
    return server.getProcessingPass(dict, path)
  
  #############################################################################
  def getProductions(self, dict):
    server = self.__getServer()
    return server.getProductions(dict)
  
  #############################################################################
  def getFileTypes(self, dict):
    server = self.__getServer()
    return server.getFileTypes(dict)
  
  #############################################################################
  def getFiles(self, dict):
    bkk = TransferClient('Bookkeeping/BookkeepingManager')
    s = cPickle.dumps(dict)
    file = tempfile.NamedTemporaryFile()
    params = str(s)
    result = bkk.receiveFile(file.name, params)
    if not result['OK']:
      return result
    else:
      value = cPickle.load(open(file.name))
      file.close()
      return S_OK(value)
    return S_ERROR()
  
  #############################################################################  
  def getAvailableDataQuality(self):
    server = self.__getServer()
    return server.getAvailableDataQuality()