# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Interfaces/API/Transformation.py $
__RCSID__ = "$Id: Transformation.py 19505 $"

from DIRAC.Core.Base import Script
Script.parseCommandLine()

import string, os, shutil, types, pprint

from DIRAC                                                        import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Interfaces.API.Transformation                          import Transformation as DIRACTransformation
from LHCbDIRAC.TransformationSystem.Client.TransformationDBClient import TransformationDBClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient         import BookkeepingClient 

COMPONENT_NAME='Transformation'

class Transformation(DIRACTransformation):

  #############################################################################
  def __init__(self,transID=0):
    DIRACTransformation.__init__(self,transID=transID)
    self.transClient = TransformationDBClient()
    #TODO REMOVE THIS
    self.transClient.setServer("ProductionManagement/ProductionManager")

    self.supportedPlugins += ['ByRun','ByRunBySize','ByRunCCRC_RAW','CCRC_RAW'] # TODO INCLUDE REPLICATION PLUGINS
    self.paramValues['BKQuery'] = {}
    self.paramValues['BKQueryID'] = 0
    
  def generateBKQuery(self,test=False):
    defaultParams = {     'SimulationConditions'     : 'All',
                          'DataTakingConditions'     : 'All',
                          'ProcessingPass'           : 'All',
                          'FileType'                 : 'All',
                          'EventType'                : 0,
                          'ConfigName'               : 'All', 
                          'ConfigVersion'            : 'All',
                          'ProductionID'             : 0,
                          'DataQualityFlag'          : 'All'}
    queryDict = {}
    for parameter, default in defaultParams.items():
      res = self._promptUser("Please enter %s" % parameter,default=default)
      if not res['OK']:
        return res
      if res['Value'].lower() != default.lower():
        queryDict[parameter] = res['Value']
    if not queryDict:
      return S_ERROR("Some of the parameters must be set")
    if (queryDict['SimulationConditions'] != "All") and (queryDict['DataTakingConditions'] != "All"):
      return S_ERROR("SimulationConditions and DataTakingConditions can not be defined simultaneously")
    if test:
      self.testBKQuery(queryDict)
    return S_OK(queryDict)

  def testBKQuery(self,bkQuery,printOutput=False):
    client = BookkeepingClient()
    res = client.getFilesWithGivenDataSets(bkQuery)
    if not res['OK']:
      return self._errorReport(res,'Failed to perform BK query')
    gLogger.info('The supplied query returned %d files' % len(res['Value']))
    if printOutput:
      self._prettyPrint(res)
    return S_OK(res['Value'])  

  def setBKQuery(self,queryDict,test=False):
    if test:
      res = self.testBKQuery(queryDict)
      if not res['OK']:
        return res
    transID = self.paramValues['TransformationID']
    if self.exists and transID:
      res = self.transClient.createTransformationQuery(transID,queryDict)
      if not res['OK']:
        return res
      self.item_called = 'BKQueryID'
      self.paramValues[self.item_called] = res['Value']
    self.item_called = 'BKQuery'
    self.paramValues[self.item_called] = queryDict
    return S_OK()

  def getBKQuery(self,printOutput=False):
    if self.paramValues['BKQuery']:
      return S_OK(self.paramValues['BKQuery'])
    res = self.__executeOperation('getBookkeepingQueryForTransformation',printOutput=printOutput)
    if not res['OK']:
      return res
    self.item_called = 'BKQuery'
    self.paramValues[self.item_called] = res['Value']
    return S_OK(res['Value'])
    
  def removeTransformationBKQuery(self):
    if not self.paramValues['BKQueryID']:
      gLogger.info("The BK Query is not defined")
      return S_OK()
    queryID = self.paramValues['BKQueryID']
    if self.exists and transID:
      res = self.transClient.deleteBookkeepingQuery(queryID)
      if not res['OK']:
        return res
      res = self.transClient.setTransformationParameter('BKQuery',0)
      if not res['OK']:
        return res
    self.item_called = 'BKQueryID'
    self.paramValues[self.item_called] = 0
    self.item_called = 'BKQuery'
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

    bkQuery = self.paramValues['BKQuery']
    if bkQuery:
      res = self.setBKQuery(bkQuery)
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
                                             maxJobs             = self.paramValues['MaxNumberOfJobs'],
                                             eventsPerJob        = self.paramValues['EventsPerJob'],
                                             addFiles            = addFiles,
                                             bkQuery             = self.paramValues['BKQuery'])
    if not res['OK']:
      self._prettyPrint(res)
      return res
    transID = res['Value']
    self.exists = True
    self.setTransformationID(transID)
    gLogger.info("Created transformation %d" % transID)
    for paramName,paramValue in self.paramValues.items():
      if not self.paramTypes.has_key(paramName):
        if not paramName in ['BKQueryID','BKQuery']:
          res = self.transClient.setTransformationParameter(transID,paramName,paramValue)
          if not res['OK']:
            gLogger.error("Failed to add parameter", "%s %s" % (paramName,res['Message']))
            gLogger.info("To add this parameter later please execute the following.")
            gLogger.info("oTransformation = Transformation(%d)" % transID)
            gLogger.info("oTransformation.set%s(...)" % paramName)
    return S_OK()

  def _checkByRun(self):
    return self._checkStandardPlugin()

  def _checkByRunBySize(self):
    return self._checkStandardPlugin()

  def _checkByRunCCRC_RAW(self):
    return self._checkStandardPlugin()

  def _checkCCRC_RAW(self):
    return self._checkStandardPlugin()
