# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/API/Transformation.py $
__RCSID__ = "$Id: Transformation.py 19505 $"

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import string, os, shutil, types, pprint

from DIRAC                                                                import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.Client.Transformation                     import Transformation as DIRACTransformation
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient              import BookkeepingClient 

COMPONENT_NAME='Transformation'

class Transformation(DIRACTransformation):

  #############################################################################
  def __init__(self,transID=0):
    DIRACTransformation.__init__(self,transID=transID,transClient=TransformationClient())

    self.supportedPlugins += ['ByRun','ByRunBySize','ByRunCCRC_RAW','CCRC_RAW','LHCbMCDSTBroadcast','LHCbDSTBroadcast','RAWShares','AtomicRun'] 
    if not  self.paramValues.has_key('BkQuery'):
      self.paramValues['BkQuery'] = {}
    if not self.paramValues.has_key('BkQueryID'):
      self.paramValues['BkQueryID'] = 0
    
  def generateBkQuery(self,test=False,printOutput=False):
    parameters = ['SimulationConditions','DataTakingConditions','ProcessingPass','FileType','EventType','ConfigName','ConfigVersion','ProductionID','DataQualityFlag']
    queryDict = {'FileType':'DST'}
    parameterDefaults = queryDict.copy()
    for parameter in parameters:
      default = parameterDefaults.get(parameter, 'All')
      res = self._promptUser("Please enter %s" % parameter,choices=[],default=default)
      if not res['OK']:
        return res
      if res['Value'] != default:
        queryDict[parameter] = res['Value']
    if not queryDict:
      return S_ERROR("At least one of the parameters must be set")
    if (queryDict.has_key('SimulationConditions')) and (queryDict.has_key('DataTakingConditions')):
      return S_ERROR("SimulationConditions and DataTakingConditions can not be defined simultaneously")
    if test:
      self.testBkQuery(queryDict,printOutput=printOutput)
    return S_OK(queryDict)

  def testBkQuery(self,bkQuery,printOutput=False):
    client = BookkeepingClient()
    res = client.getFilesWithGivenDataSets(bkQuery)
    if not res['OK']:
      return self._errorReport(res,'Failed to perform BK query')
    gLogger.info('The supplied query returned %d files' % len(res['Value']))
    if printOutput:
      self._prettyPrint(res)
    return S_OK(res['Value'])  

  def setBkQuery(self,queryDict,test=False):
    if test:
      res = self.testBkQuery(queryDict)
      if not res['OK']:
        return res
    transID = self.paramValues['TransformationID']
    if self.exists and transID:
      res = self.transClient.createTransformationQuery(transID,queryDict)
      if not res['OK']:
        return res
      self.item_called = 'BkQueryID'
      self.paramValues[self.item_called] = res['Value']
    self.item_called = 'BkQuery'
    self.paramValues[self.item_called] = queryDict
    return S_OK()

  def getBkQuery(self,printOutput=False):
    if self.paramValues['BkQuery']:
      return S_OK(self.paramValues['BkQuery'])
    res = self.__executeOperation('getBookkeepingQueryForTransformation',printOutput=printOutput)
    if not res['OK']:
      return res
    self.item_called = 'BkQuery'
    self.paramValues[self.item_called] = res['Value']
    return S_OK(res['Value'])
    
  def deleteTransformationBkQuery(self):
    if not self.paramValues['BkQueryID']:
      gLogger.info("The BK Query is not defined")
      return S_OK()
    transID = self.paramValues['TransformationID']
    if self.exists and transID:
      res = self.transClient.deleteTransformationBookkeepingQuery(transID)
      if not res['OK']:
        return res
    self.item_called = 'BkQueryID'
    self.paramValues[self.item_called] = 0
    self.item_called = 'BkQuery'
    self.paramValues[self.item_called] = {}
    return S_OK()

  #############################################################################
  def addTransformation(self,addFiles=True, printOutput=False):
    res = self._checkCreation()
    if not res['OK']:
      return self._errorReport(res,'Failed transformation sanity check')
    if printOutput:
      gLogger.info("Will attempt to create transformation with the following parameters")
      self._prettyPrint(self.paramValues)

    bkQuery = self.paramValues['BkQuery']
    if bkQuery:
      res = self.setBkQuery(bkQuery)
      if not res['OK']:
        return self._errorReport(res,'Failed BK query sanity check')
    
    res = self.transClient.addTransformation(self.paramValues['TransformationName'],
                                             self.paramValues['Description'],
                                             self.paramValues['LongDescription'],
                                             self.paramValues['Type'],
                                             self.paramValues['Plugin'],
                                             self.paramValues['AgentType'],
                                             self.paramValues['FileMask'],
                                             transformationGroup = self.paramValues['TransformationGroup'],
                                             groupSize           = self.paramValues['GroupSize'],
                                             inheritedFrom       = self.paramValues['InheritedFrom'],
                                             body                = self.paramValues['Body'],
                                             maxTasks            = self.paramValues['MaxNumberOfTasks'],
                                             eventsPerTask        = self.paramValues['EventsPerTask'],
                                             addFiles            = addFiles,
                                             bkQuery             = self.paramValues['BkQuery'])
    if not res['OK']:
      self._prettyPrint(res)
      return res
    transID = res['Value']
    res = self.transClient.getTransformationParameters(transID,['BkQueryID'])
    if not res['OK']:
      self._prettyPrint(res)
      return res
    self.setBkQueryID(res['Value'])
    self.exists = True
    self.setTransformationID(transID)
    gLogger.info("Created transformation %d" % transID)
    for paramName,paramValue in self.paramValues.items():
      if not self.paramTypes.has_key(paramName):
        if not paramName in ['BkQueryID','BkQuery']:
          res = self.transClient.setTransformationParameter(transID,paramName,paramValue)
          if not res['OK']:
            gLogger.error("Failed to add parameter", "%s %s" % (paramName,res['Message']))
            gLogger.info("To add this parameter later please execute the following.")
            gLogger.info("oTransformation = Transformation(%d)" % transID)
            gLogger.info("oTransformation.set%s(...)" % paramName)
    return S_OK()

  def _checkByRunPlugin(self):
    return self._checkStandardPlugin()

  def _checkByRunBySizePlugin(self):
    return self._checkStandardPlugin()

  def _checkByRunCCRC_RAWPlugin(self):
    return self._checkStandardPlugin()

  def _checkCCRC_RAWPlugin(self):
    return self._checkStandardPlugin()

  def _checkRAWSharesPlugin(self):
    return S_OK()

  def _checkAtomicRunPlugin(self):
    return S_OK()

  def _checkLHCbMCDSTBroadcastPlugin(self):
    return S_OK()

  def _checkLHCbDSTBroadcastPlugin(self):            
    return S_OK()                            
