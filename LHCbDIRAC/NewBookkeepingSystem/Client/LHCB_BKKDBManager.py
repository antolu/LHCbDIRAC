########################################################################
# $Id$
########################################################################

"""
LHCb Bookkeeping database manager
"""

from DIRAC                                                                      import gLogger, S_OK, S_ERROR
from LHCbDIRAC.NewBookkeepingSystem.Client.BaseESManager                        import BaseESManager
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient                    import BookkeepingClient
from LHCbDIRAC.NewBookkeepingSystem.Client                                      import objects
from LHCbDIRAC.NewBookkeepingSystem.Client.Help                                 import Help
from DIRAC.DataManagementSystem.Client.ReplicaManager                           import ReplicaManager
import os
import types
import sys

__RCSID__ = "$Id$"

INTERNAL_PATH_SEPARATOR = "/"

############################################################################# 
class LHCB_BKKDBManager(BaseESManager):
    
  LHCB_BKDB_FOLDER_TYPE = "LHCB_BKDB_Folder"
  LHCB_BKDB_FILE_TYPE = "LHCB_BKDB_File"
                                         
  LHCB_BKDB_FOLDER_PROPERTIES = ['name', 
                                'fullpath',
                                        ]    
    # watch out for this ad hoc solution
    # if any changes made check all functions
    #
  LHCB_BKDB_PREFIXES_CONFIG =     ['ConfigName',  # configname
                                   'ConfigVersion',  #configversion
                                   'Simulation/DataTaking',
                                   'ProcessingPass',
                                   'EventTypeid',    # event type
                                   'Production',   #production 
                                   'FileType',    #file type 
                                   ''                                 
                                   ]
  
  LHCB_BKDB_PREFIXES_PRODUCTION = ['PROD',
                                   'EVT',
                                   'FTY',
                                   ''
                                  ]
  
  LHCB_BKDB_PREFIXES_RUN =     ['RUN',
                                'PAS',
                                'EVT',
                                'FTY',
                                 ''
                                  ]
  
  LHCB_BKDB_PREFIXES_EVENTTYPE = ['ConfigName',
                                  'ConfigVersion',
                                  'EventTypeid',
                                  'Simulation/DataTaking',
                                  'ProcessingPass',
                                  'Production',
                                  'FileType',
                                   '',
                                  ]
  LHCB_BKDB_PREFIXES=[]

  LHCB_BKDB_PARAMETERS = ['Configuration', 'Event type' ,'Productions', 'Runlookup' ]
  
  LHCB_BKDB_PARAMETERS_SHORTNAME = {LHCB_BKDB_PARAMETERS[0]:'sim', LHCB_BKDB_PARAMETERS[1]:'evt' ,LHCB_BKDB_PARAMETERS[2]:'prod', LHCB_BKDB_PARAMETERS[3]:'run' }
  
  LHCB_QUERIES_TYPE = ['adv','std']
  LHCB_BKDB_PREFIX_SEPARATOR = "_"
  
  ############################################################################# 
  def __init__(self, rpcClinet = None):
    super(LHCB_BKKDBManager, self).__init__()
    self._BaseESManager___fileSeparator = INTERNAL_PATH_SEPARATOR    
    #self.__pathSeparator = INTERNAL_PATH_SEPARATOR
    self.db_ = BookkeepingClient(rpcClinet)
    self.rm_ = ReplicaManager()
    
    self.helper_ = Help()
    
    self.__entityCache = {'/':(objects.Entity({'name':'/', 'fullpath':'/','expandable':True}), 0)} 
    self.parameter_ = self.LHCB_BKDB_PARAMETERS[0]
    self.files_ = []

    self.treeLevels_ = -1
    self.advancedQuery_ = False
    print "First please choose which kind of queries you want to use!"
    print "The default value is Configuration!"
    print "The possible parameters:"
    print self.getPossibleParameters()
    print "For Example:"
    print "client.setParameter('Processing Pass')"
    print "If you need help, you will use client.help() command."
    
    self.dataQualities_ = {}
    #retVal = self.db_.getAvailableFileTypes()
    #if not retVal['OK']:
    #  return retVal
    #else:
    # self.__filetypes = [ i[0] for i in retVal['Value']['Records']]
    self.__filetypes = []
        
  ############################################################################# 
  def _updateTreeLevels(self, level):
    self.treeLevels_ = level
  
  ############################################################################# 
  def setVerbose(self, Value):
    objects.VERBOSE = Value
  
  ############################################################################# 
  def setAdvancedQueries(self, Value):
    self.advancedQuery_ = Value
    
  ############################################################################# 
  def _getTreeLevels(self):
    return self.treeLevels_
  
  ############################################################################# 
  def help(self):
    if self.parameter_ == self.LHCB_BKDB_PARAMETERS[0]:
      self.helper_.helpConfig(self._getTreeLevels())
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[1]:
      self.helper_.helpEventType(self._getTreeLevels())  
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[2]:
      self.helper_.helpProcessing(self._getTreeLevels())
  
  ############################################################################# 
  def getPossibleParameters(self):
    return self.LHCB_BKDB_PARAMETERS
  
  #############################################################################
  def getCurrentParameter(self):
    return self.LHCB_BKDB_PARAMETERS_SHORTNAME[self.parameter_]
  
  #############################################################################
  def getQueriesTypes(self):
    if self.advancedQuery_:
      return self.LHCB_QUERIES_TYPE[0]
    else:
      return self.LHCB_QUERIES_TYPE[1]
      
  ############################################################################# 
  def setParameter(self, name):
    if self.LHCB_BKDB_PARAMETERS.__contains__(name):
      self.parameter_ = name
      self.treeLevels_ = -1
      if name == 'Configuration':
        self.LHCB_BKDB_PREFIXES = self.LHCB_BKDB_PREFIXES_CONFIG
      elif name == 'Productions':
        self.LHCB_BKDB_PREFIXES = self.LHCB_BKDB_PREFIXES_PRODUCTION
      elif name == 'Event type':
        self.LHCB_BKDB_PREFIXES = self.LHCB_BKDB_PREFIXES_EVENTTYPE
      elif name == 'Runlookup':
        self.LHCB_BKDB_PREFIXES = self.LHCB_BKDB_PREFIXES_RUN
   
    else:
      gLogger.error("Wrong Parameter!")
  
  #############################################################################
  def getLogicalFiles(self):
    return self.files_ 
  
  #############################################################################
  def getFilesPFN(self):
    lfns = self.files_
    res = self.rm_.getCatalogReplicas(lfns)
    return res
  
  ############################################################################# 
  def list(self, path="/", SelectionDict = {}, SortDict={}, StartItem=0, Maxitems=0):
    gLogger.debug(path)

    if self.parameter_ == self.LHCB_BKDB_PARAMETERS[0]:
      return self._listConfigs(path, SelectionDict, SortDict, StartItem, Maxitems) 
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[1]:
      return self._listEventTypes(path, SelectionDict, SortDict, StartItem, Maxitems)
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[2]:
      return self._listProduction(path, SelectionDict, SortDict, StartItem, Maxitems)
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[3]:
      return self._listRuns(path, SelectionDict, SortDict, StartItem, Maxitems)
  
  ############################################################################# 
  def getLevelAndPath(self, path):
    if path == '/':
      return 0,[],'' # it is the first level
    path = self.getAbsolutePath(path)['Value'] # shall we do this here or in the _processedPath()?
    processedPath = self._processPath(path)
    tmpPath = list(processedPath)
    if self.parameter_ == self.LHCB_BKDB_PARAMETERS[1]:
      level,procpass = self.__getEvtLevel(tmpPath, [], level=0, start = False, end = False, processingpath='', startlevel=4)
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[3]:
      level,procpass = self.__getRunLevel(tmpPath, [], level=0, start = False, end = False, processingpath='', startlevel=1)
    else:
      level,procpass = self.__getLevel(tmpPath, [])
    self._updateTreeLevels(level)
    return level,processedPath,procpass
  
  #############################################################################
  # This method recursive visite all the tree nodes and found the processing pass
  def __getLevel(self, path, visited = [], level=0, start = False, end = False, processingpath='', startlevel=3): 
    
    for i in path:
      if level == startlevel and start==False:
        for j in visited:
          path.remove(j)
        level += 1
        return self.__getLevel(path, visited, level, start=True)
      else:
        level+=1
        try: 
          result = type(long(i)) == types.LongType
          if start and result:
            end = True 
        except Exception,ex:
          pass #print 'i',ex     
        if start and not end:
          level = startlevel
          processingpath +='/'+i
        elif end and level<=startlevel+1:
          level = startlevel + 1
      visited += [i]
    return level, processingpath
  
  #############################################################################
  def __getRunLevel(self, path, visited = [], level=0, start = False, end = False, processingpath='', startlevel=1): 
    
    for i in path:
      if level == startlevel and start==False:
        for j in visited:
          path.remove(j)
        level += 1
        return self.__getRunLevel(path, visited, level, start=True)
      else:
        level+=1
        try: 
          result = type(long(i)) == types.LongType
          if start and result:
            end = True 
        except Exception,ex:
          pass #print 'i',ex     
        if start and not end:
          level = startlevel
          processingpath +='/'+i
        elif end and level<=startlevel+1:
          level = startlevel + 1
      visited += [i]
    return level, processingpath
  
  #############################################################################
  # This method recursive visite all the tree nodes and found the processing pass
  def __getEvtLevel(self, path, visited = [], level=0, start = False, end = False, processingpath='', startlevel=4): 
    
    for i in path:
      if level == startlevel and start==False:
        for j in visited:
          path.remove(j)
        level += 1
        return self.__getEvtLevel(path, visited, level, start=True)
      else:
        level+=1
        try: 
          result = (type(long(i)) == types.LongType)   
        except Exception,ex:
           result = i in self.__filetypes
        if start and result:
            end = True          
        if start and not end:
          level = startlevel
          processingpath +='/'+i
        elif end and level<=startlevel+1:
          level = startlevel + 1
      visited += [i]
    return level, processingpath
          
  ############################################################################# 
  def _listConfigs(self, path, SelectionDict, SortDict, StartItem, Maxitems):
    entityList = list()
    levels, processedPath,procpass = self.getLevelAndPath(path)
    
    if levels == 0: #configname
      self.clevelHeader_0(path, levels, processedPath)
      entityList += self.clevelBody_0(path, levels,)
    
    if levels == 1: #config version
      dict = self.clevelHeader_1(path, levels, processedPath) 
      entityList += self.clevelBody_1(path, levels, dict)  
    
    if levels == 2: # sim or data desc 
      dict = self.clevelHeader_2(path, levels, processedPath) 
      entityList += self.clevelBody_2(path, levels, dict)
    
    if levels == 3: #processing 
      dict = self.clevelHeader_3(path, levels, processedPath) 
      entityList += self.clevelBody_3(path, levels, dict, procpass)
    
    if levels == 4 and self.advancedQuery_: #prod
      dict = self.clevelHeader_4(path, levels, processedPath, procpass) 
      entityList += self.clevelBody_4(path, levels, dict)
    elif levels == 4 and not self.advancedQuery_:
      processedPath += ['ALL']
      dict = self.clevelHeader_5(path, 5, processedPath, procpass) 
      entityList += self.clevelBody_5(path, 5, dict)
     
    if levels == 5 and self.advancedQuery_: #file type
      dict = self.clevelHeader_5(path, levels, processedPath, procpass) 
      entityList += self.clevelBody_5(path, levels, dict)
    elif levels == 5 and not self.advancedQuery_:
      levels = 6
    
    if levels == 6 and StartItem == 0 and Maxitems == 0: #files
      dict = self.clevelHeader_6(path, levels, processedPath, procpass) 
      entityList += self.clevelBody_6(path, levels, dict)
    elif levels == 6 and (StartItem != 0 or Maxitems != 0): #files
      dict = self.clevelHeader_6(path, levels, processedPath, procpass) 
      entityList += self.clevelBodyLimited_6(path, levels, dict, SelectionDict, SortDict, StartItem, Maxitems)
    return entityList
  
  ############################################################################# 
  def __addAll(self, path,levels,description):
    if self.advancedQuery_:
      return self._getEntityFromPath(path, "ALL", levels, description)
    else:
       return None
    
  def __createPath(self, processedPath, name):
    path = ''
    for i in processedPath:
     s = '/'+i[0]+'_'+i[1]
     path += s
    
    path +='/'+name[0]+'_'+name[1]
    return path


  ############################################################################# 
  def clevelHeader_0(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug ("Configurations names:")
    gLogger.debug ("-----------------------------------------------------------")

    # list root
    gLogger.debug("listing Configuration Names")
  
  ############################################################################# 
  def clevelBody_0(self, path, levels):
    entityList = list()
    result = self.db_.getAvailableConfigNames()
    
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult['Records']:
        entityList += [self._getEntityFromPath(path, record[0], levels,None,{},'getAvailableConfigNames')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def clevelHeader_1(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("listing configversions")
    dict = {'ConfigName': processedPath[0]}
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name      | %s " % (processedPath[0]))
    
    gLogger.debug("Available Config Versions:")
    return dict
  
  def clevelBody_1(self, path, levels, dict):
    entityList = list()
    result = self.db_.getConfigVersions(dict)
    if result['OK']:
      dbResult = result['Value']
      description = dbResult["ParameterNames"][0]
      for record in dbResult['Records']:
        entityList += [self._getEntityFromPath(path, record[0], levels, description,dict,'getConfigVersions')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
  
  
  ############################################################################# 
  def clevelHeader_2(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("listing Simulation Conditions!")
    dict = {'ConfigName': processedPath[0],
            'ConfigVersion':processedPath[1]}
              
    gLogger.debug( "-----------------------------------------------------------")
    gLogger.debug("Selected parameters:" )                             
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Available Simulation Conditions:")
    return dict
  
  ############################################################################# 
  def clevelBody_2(self, path, levels, dict):
    entityList = list()
    result = self.db_.getConditions(dict) 
    if result['OK']:
      dbResult = result['Value']
      if dbResult["TotalRecords"] > 1:
        add = self.__addAll(path, levels, 'Simulation Conditions/DataTaking')
        if add:
          entityList += [add]
      for record in dbResult['Records']:
        value = {}
        j = 0
        for i in dbResult['ParameterNames']:
          value[i] = record[j]
          j+=1
        entityList += [self._getSpecificEntityFromPath(path, value, record[1], levels, None, 'Simulation Conditions/DataTaking',dict,'getConditions')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])

    return entityList
  

  ############################################################################# 
  def clevelHeader_3(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("listing processing pass")
    dict = {'ConfigName': processedPath[0],
            'ConfigVersion':processedPath[1],
            'ConditionDescription':processedPath[2]}
    
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    gLogger.debug( "-----------------------------------------------------------")

    gLogger.debug("Available processing pass:\n")
    return dict
  
  ############################################################################# 
  def clevelBody_3(self, path, levels, dict, procpass):
    entityList = list()
    dict['ProcessingPass']=procpass
    result = self.db_.getProcessingPass(dict, procpass)
    if result['OK']:
      dbResult = result['Value']
      if dbResult[0]['TotalRecords'] > 0: # it is a processing pass
        add = self.__addAll(path, levels, 'Processing Pass')
        if add:
          entityList += [add]
      for record in dbResult[0]['Records']:  
        entityList += [self._getEntityFromPath(path, record[0], levels, 'Processing Pass',dict,'getProcessingPass')]
      self._cacheIt(entityList)
      if dbResult[1]['TotalRecords'] > 0:
        value = {}
        for record in dbResult[1]['Records']:  
          value = {'Event Type':record[0],'Description':record[1]} 
          entityList += [self._getSpecificEntityFromPath(path, value, str(record[0]), levels, None, 'Event types',dict,'getProcessingPass')]
        self._cacheIt(entityList) 
    else:
      gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def clevelHeader_4(self, path, levels, processedPath, procpass):
    entityList = list()
    
    gLogger.debug("listing event types")
    r = procpass.split('/')[1:]
    for i in r:
      processedPath.remove(i)
    
    dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1], 'ConditionDescription': processedPath[2],'EventTypeId': processedPath[3]}
    dict['ProcessingPass']=procpass
    
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters: ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    gLogger.debug("-----------------------------------------------------------")

    gLogger.debug("Available event types types:")
    return dict
  
  
  ############################################################################# 
  def clevelBody_4(self, path, levels, dict):
    entityList = list()
    result = self.db_.getProductions(dict)
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult['Records']:
         entityList += [self._getEntityFromPath(path, str(record[0]), levels, 'Production(s)/Run(s)',dict,'getProductions')]
      self._cacheIt(entityList)
    else:
        gLogger.error(result['Message'])
    return entityList    
  
  ############################################################################# 
  def clevelHeader_5(self, path, levels, processedPath, procpass):
    entityList = list()
    
    gLogger.debug("listing event types")
    r = procpass.split('/')[1:]
    for i in r:
      processedPath.remove(i)
    
    if self.advancedQuery_:
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1], 'ConditionDescription': processedPath[2],'EventTypeId': processedPath[3],'Production':processedPath[4]}
    else:
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1], 'ConditionDescription': processedPath[2],'EventTypeId': processedPath[3]}
    dict['ProcessingPass']=procpass
        
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters: ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    gLogger.debug("-----------------------------------------------------------")

    gLogger.debug("Available event types types:")
    return dict
  
  
  ############################################################################# 
  def clevelBody_5(self, path, levels, dict):
    entityList = list()
    result = self.db_.getFileTypes(dict)
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult['Records']:
         entityList += [self._getEntityFromPath(path, record[0], levels, 'FileTypes',dict,'getFileTypes')]
      self._cacheIt(entityList)
    else:
        gLogger.error(result['Message'])
    return entityList    
  
  ############################################################################# 
  def clevelHeader_6(self, path, levels, processedPath, procpass):
    entityList = list()
    gLogger.debug("listing event types")    
    
    r = procpass.split('/')[1:]
    for i in r:
      processedPath.remove(i)
    
    if self.advancedQuery_: 
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1], 'ConditionDescription': processedPath[2],'EventTypeId': processedPath[3],'Production':processedPath[4],'FileType':processedPath[5]}
    else:
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1], 'ConditionDescription': processedPath[2],'EventTypeId': processedPath[3],'FileType':processedPath[4]}
    dict['ProcessingPass']=procpass
   
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters: ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    gLogger.debug("-----------------------------------------------------------")
    
    return dict
  
  ############################################################################# 
  def clevelBody_6(self, path, levels, dict):
    entityList = list()
    dict['Quality']=self.__getSelectedQualities()
    result = self.db_.getFiles(dict)
    if result['OK']:
      for record in result['Value']['Records']:
        value = {'name':record[0],'EventStat':record[1], 'FileSize':record[2], 'CreationDate':record[3], 'JobStart':record[4], 'JobEnd':record[5], 'WorkerNode':record[6],
                 'FileType':dict['FileType'],'RunNumber':record[8], 'FillNumber':record[9], 'FullStat':record[10], 'DataqualityFlag':record[11],'EventTypeId':dict['EventTypeId'],
                 'EventInputStat':record[12], 'TotalLuminosity':record[13], 'Luminosity':record[14], 'InstLuminosity':record[15]}
        self.files_ += [record[0]]
        entityList += [self._getEntityFromPath(path, value, levels,'List of files',dict,'getFiles')]
      self._cacheIt(entityList)
    else:
      return result        
    return entityList
  
  #############################################################################
  def clevelBodyLimited_6(self, path, levels, dict,SelectionDict, SortDict, StartItem, Maxitems):
    entityList = list()
    dict['Quality']=self.__getSelectedQualities()
    result = self.__getFiles(dict, SortDict, StartItem, Maxitems)
    for record in result['Records']:
      value = {'name':record[0],'EventStat':record[1], 'FileSize':record[2], 'CreationDate':record[3], 'JobStart':record[4], 'JobEnd':record[5], 'WorkerNode':record[6],
               'FileType':dict['FileType'],'RunNumber':record[8], 'FillNumber':record[9], 'FullStat':record[10], 'DataqualityFlag':record[11],'EventTypeId':dict['EventTypeId'],
               'EventInputStat':record[12], 'TotalLuminosity':record[13], 'Luminosity':record[14], 'InstLuminosity':record[15]}
      self.files_ += [record[0]]
      entityList += [self._getEntityFromPath(path, value, levels,'List of files',dict,'getFiles')]
    self._cacheIt(entityList)        
    return entityList
  
  ############################################################################# 
  def _listEventTypes(self, path, SelectionDict, SortDict, StartItem, Maxitems):
    entityList = list()
    levels, processedPath,procpass = self.getLevelAndPath(path)
    
    if levels == 0: #configname
      self.clevelHeader_0(path, levels, processedPath)
      entityList += self.clevelBody_0(path, levels,)
    
    if levels == 1: #config version
      dict = self.clevelHeader_1(path, levels, processedPath) 
      entityList += self.clevelBody_1(path, levels, dict)  

    if levels == 2: #event type
      dict = self.clevelHeader_2(path, levels, processedPath) 
      entityList += self.elevelBody_2(path, levels, dict)
   
    if levels == 3: # sim ot daq desq
      dict = self.elevelHeader_3(path, levels, processedPath) 
      entityList += self.elevelBody_3(path, levels, dict)
    
    if levels == 4: #processing pass
      dict = self.elevelHeader_4(path, levels, processedPath)
      entityList += self.elevelBody_4(path, levels, dict,procpass)
   
    if self.advancedQuery_ and levels == 5:
      dict = self.elevelHedaer_5(path, levels, processedPath, procpass)
      entityList += self.clevelBody_5(path, levels, dict)
    elif levels == 5:
      levels = 6
    
    if levels == 6 and StartItem == 0 and Maxitems == 0: #files
      dict = self.elevelHeader_6(path, levels, processedPath, procpass) 
      entityList += self.clevelBody_6(path, levels, dict)
    elif levels == 6 and (StartItem != 0 or Maxitems != 0): #files
      dict = self.elevelHeader_6(path, levels, processedPath, procpass) 
      entityList += self.clevelBodyLimited_6(path, levels, dict, SelectionDict, SortDict, StartItem, Maxitems)
       
    return entityList
  
   ############################################################################# 
  def elevelBody_2(self, path, levels, dict):      
    entityList = list()
    result = self.db_.getEventTypes(dict)
    if result['OK']:
      dbResult = result['Value']
      if len(dbResult) > 1:
        add = self.__addAll(path, levels, 'Event types')
        if add:
          entityList += [add]        
      for record in dbResult['Records']:  
        value = {'Event Type':record[0],'Description':record[1]} 
        entityList += [self._getSpecificEntityFromPath(path, value, str(record[0]), levels, None, 'Event types',dict,'getEventTypes')]
      self._cacheIt(entityList)      
    else:
      gLogger.error(result['Message'])
      return result
    return entityList

  ############################################################################# 
  def elevelHeader_3(self, path, levels, processedPath):
    gLogger.debug("listing simulation conditions")
        
    dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1], 'EventTypeId': processedPath[2]}
    
    
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    gLogger.debug("-----------------------------------------------------------")
    
    gLogger.debug("Available conditions:")
    return dict
  
  ############################################################################# 
  def elevelBody_3(self,path, levels, dict):
    entityList = list()
    
    result = self.db_.getConditions(dict) 
    if result['OK']:
      dbResult = result['Value']
      if dbResult["TotalRecords"] > 1:
        add = self.__addAll(path, levels, 'Simulation Conditions/DataTaking')
        if add:
          entityList += [add]
      for record in dbResult['Records']:
        value = {}
        j = 0
        for i in dbResult['ParameterNames']:
          value[i] = record[j]
          j+=1
        entityList += [self._getSpecificEntityFromPath(path, value, record[1], levels, None, 'Simulation Conditions/DataTaking',dict,'getConditions')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])

    return entityList

  ############################################################################# 
  def elevelHeader_4(self, path, levels, processedPath):
    gLogger.debug("listing processing pass")
    
    dict = {'ConfigName': processedPath[0],
            'ConfigVersion':processedPath[1],
            'EventTypeId':processedPath[2],
            'ConditionDescription':processedPath[3]
             }
    
    
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters: ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    gLogger.debug("-----------------------------------------------------------")

    gLogger.debug("Available processing pass types:")
    return dict
  
  ############################################################################# 
  def elevelBody_4(self, path, levels, dict, procpass):
    entityList = list()

    result = self.db_.getStandardProcessingPass(dict,  procpass)
    if result['OK']:
      dbResult = result['Value']
      if dbResult[0]['TotalRecords'] > 0: # it is a processing pass
        add = self.__addAll(path, levels, 'Processing Pass')
        if add:
          entityList += [add]
        for record in dbResult[0]['Records']:  
          entityList += [self._getEntityFromPath(path, record[0], levels, 'Processing Pass',dict,'getProcessingPass')]
        self._cacheIt(entityList)
      elif self.advancedQuery_:
        dict['ProcessingPass'] = procpass
        result = self.db_.getProductions(dict)
        if result['OK']:
          dbResult = result['Value'] 
          for record in dbResult['Records']:
             entityList += [self._getEntityFromPath(path, str(record[0]), levels, 'Production(s)/Run(s)',dict,'getProductions')]
          self._cacheIt(entityList)
      else:
        dict['ProcessingPass'] = procpass
        result = self.db_.getFileTypes(dict)
        if result['OK']:
          dbResult = result['Value']
          for record in dbResult['Records']:
             entityList += [self._getEntityFromPath(path, record[0], levels, 'FileTypes',dict,'getFileTypes')]
          self._cacheIt(entityList)  
    else:
      gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def elevelHedaer_5(self, path, levels, processedPath, procpass):
    r = procpass.split('/')[1:]
    for i in r:
      processedPath.remove(i)
    
    if self.advancedQuery_:
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1],'EventTypeId': processedPath[2],'ConditionDescription': processedPath[3],'Production':processedPath[4]}
    else:
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1], 'EventTypeId': processedPath[2],'ConditionDescription': processedPath[3]}
    dict['ProcessingPass']=procpass
    
    
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Available Production(s):")
    return dict
  
  ############################################################################# 
  def elevelHeader_6(self, path, levels, processedPath,procpass):
    r = procpass.split('/')[1:]
    for i in r:
      processedPath.remove(i)
    
    if self.advancedQuery_:
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1],'EventTypeId': processedPath[2],'ConditionDescription': processedPath[3],'Production':processedPath[4],'FileType':processedPath[5]}
    else:
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1],'EventTypeId': processedPath[2],'ConditionDescription': processedPath[3],'FileType':processedPath[4]}
    dict['ProcessingPass']=procpass
    
            
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Available file types:")
    return dict
      
  ############################################################################# 
  def _listProduction(self, path, SelectionDict, SortDict, StartItem, Maxitems):    
    
    entityList = list()
    levels, processedPath,procpass = self.getLevelAndPath(path)
    
    if levels == 0:
       self.plevelHeader_0(path, levels, processedPath)
       entityList += self.plevelBody_0(path, levels, None)
       
    if levels == 1: 
      dict = self.plevelHeader_2(path, levels, processedPath) 
      entityList += self.plevelBody_2(path, levels, dict)
    
    if levels == 2:
      dict = self.plevelHeader_3(path, levels, processedPath) 
      entityList += self.plevelBody_3(path, levels, dict)
    
    if levels == 3:
      dict = self.plevelHeader_4(path, levels, processedPath) 
      entityList += self.plevelBody_4(path, levels, dict)
    
    return entityList
  
  ############################################################################# 
  def _listRuns(self, path, SelectionDict, SortDict, StartItem, Maxitems):
    entityList = list()
    
    levels, processedPath,procpass = self.getLevelAndPath(path)
    
    if levels == 0:
       self.rlevelHeader_0(path, levels, processedPath)
       entityList += self.rlevelBody_0(path, levels, None)
       
    if levels == 1: 
      dict = self.rlevelHeader_2(path, levels, processedPath) 
      entityList += self.rlevelBody_2(path, levels, dict, procpass)
    
    if levels == 2:
      dict = self.rlevelHeader_3(path, levels, processedPath, procpass) 
      entityList += self.rlevelBody_3(path, levels, dict)
    
    if levels == 3:
      dict = self.rlevelHeader_4(path, levels, processedPath,procpass) 
      entityList += self.clevelBody_6(path, levels, dict)
        
    return entityList

   ############################################################################# 
  def plevelHeader_0(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug ("productions:")
    gLogger.debug ("-----------------------------------------------------------")
  
    # list root
    gLogger.debug("listing productions")

  ############################################################################# 
  def plevelBody_0(self, path, levels, processedPath):
    entityList = list()
    result = self.db_.getAvailableProductions()
  
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        prod = str(record[0])
        entityList += [self._getEntityFromPath(path, str(prod), levels, 'Production(s)/Run(s)')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def plevelHeader_2(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("listing eventtype")
    
    dict = {'Production':processedPath[0]}
     
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    
    gLogger.debug("Available event types:")
    return dict
  
  ############################################################################# 
  def plevelBody_2(self, path, levels, dict):
    entityList = list()
    
    result = self.db_.getStandardEventTypes(dict)
    if result['OK']:
      dbResult = result['Value']
      if len(dbResult) > 1:
        add = self.__addAll(path, levels, 'Event types')
        if add:
          entityList += [add]        
      for record in dbResult['Records']:  
        value = {'Event Type':record[0],'Description':record[1]} 
        entityList += [self._getSpecificEntityFromPath(path, value, str(record[0]), levels, None, 'Event types',dict,'getEventTypes')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def plevelHeader_3(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("listing file types")
    dict = {'Production':processedPath[0],'EventTypeId': processedPath[1]}
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    
    gLogger.debug("Available file types:")
    return dict
  
  ############################################################################# 
  def plevelBody_3(self, path, levels, dict):
    entityList = list()
    result = self.db_.getFileTypes(dict)
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult['Records']:
         entityList += [self._getEntityFromPath(path, record[0], levels, 'FileTypes',dict,'getFileTypes')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def plevelHeader_4(self, path, levels, processedPath): 
    entityList = list()
    gLogger.debug("listing file types")
    dict = {'Production':processedPath[0],'EventTypeId': processedPath[1], 'FileType':processedPath[2]}
     
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    
    gLogger.debug("Available files:")
    return dict
  
  ############################################################################# 
  def plevelBody_4(self, path, levels, dict):
    entityList = list()
    dict['Quality']=self.__getSelectedQualities()
    result = self.db_.getFiles(dict)
    if result['OK']:
      for record in result['Value']['Records']:
        value = {'name':record[0],'EventStat':record[1], 'FileSize':record[2], 'CreationDate':record[3], 'JobStart':record[4], 'JobEnd':record[5], 'WorkerNode':record[6],
                 'FileType':dict['FileType'],'RunNumber':record[8], 'FillNumber':record[9], 'FullStat':record[10], 'DataqualityFlag':record[11], 'EventTypeId':dict['EventTypeId'],
                 'EventInputStat':record[12], 'TotalLuminosity':record[13], 'Luminosity':record[14], 'InstLuminosity':record[15]}
        self.files_ += [record[0]]
        entityList += [self._getEntityFromPath(path, value, levels,'List of files',dict,'getFiles')]
      self._cacheIt(entityList)
    else:
      return result
    
    return entityList
  
  def rlevelHeader_0(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug ("Runs:")
    gLogger.debug ("-----------------------------------------------------------")
  
    # list root
    gLogger.debug("listing runs")
 
  ############################################################################# 
  def rlevelBody_0(self, path, levels, processedPath):
    entityList = list()
    result = self.db_.getAvailableRunNumbers()
  
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        run = str(record[0])
        entityList += [self._getEntityFromPath(path, str(run), levels, 'Run(s)')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def rlevelHeader_2(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("listing processing pass")
    dict  = {'RunNumber':processedPath[0]}  
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    
    gLogger.debug("Available processing pass:")
    return dict
  
  ############################################################################# 
  def rlevelBody_2(self, path, levels, dict, procpass):
    entityList = list()
    dict['ProcessingPass']=procpass
    result = self.db_.getProcessingPass(dict, procpass)
    if result['OK']:
      dbResult = result['Value']
      if dbResult[0]['TotalRecords'] > 0: # it is a processing pass
        add = self.__addAll(path, levels, 'Processing Pass')
        if add:
          entityList += [add]
      for record in dbResult[0]['Records']:  
        entityList += [self._getEntityFromPath(path, record[0], levels, 'Processing Pass',dict,'getProcessingPass')]
      self._cacheIt(entityList)
      if dbResult[1]['TotalRecords'] > 0:
        value = {}
        for record in dbResult[1]['Records']:  
          value = {'Event Type':record[0],'Description':record[1]} 
          entityList += [self._getSpecificEntityFromPath(path, value, str(record[0]), levels, None, 'Event types',dict,'getProcessingPass')]
        self._cacheIt(entityList) 
    else:
      gLogger.error(result['Message'])
    return entityList
  
  #############################################################################
  def rlevelHeader_3(self, path, levels, processedPath, procpass):
    entityList = list()
    gLogger.debug("listing eventtypes")
    r = procpass.split('/')[1:]
    for i in r:
      processedPath.remove(i)
    
    dict = { 'RunNumber':processedPath[0],'EventTypeId': processedPath[1]}
    dict['ProcessingPass']=procpass
    
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    
    gLogger.debug("Available eventtypes types:")
    return dict
  
  #############################################################################
  def rlevelBody_3(self, path, levels, dict):
    entityList = list()
    result = self.db_.getFileTypes(dict)
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult['Records']:
         entityList += [self._getEntityFromPath(path, record[0], levels, 'FileTypes',dict,'getFileTypes')]
      self._cacheIt(entityList)
    else:
        gLogger.error(result['Message'])
    return entityList    
  
  
   ############################################################################# 
  def rlevelHeader_4(self, path, levels, processedPath, procpass):
    entityList = list()
    gLogger.debug("listing file types")
    r = procpass.split('/')[1:]
    for i in r:
      processedPath.remove(i)
    
    dict = { 'RunNumber':processedPath[0],'EventTypeId': processedPath[1],'FileType':processedPath[2]}
    dict['ProcessingPass']=procpass
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug(dict)
    
    gLogger.debug("Available file types:")
    return dict
  
  ############################################################################# 
  def rlevelBody_4(self, path, levels, dict):
    entityList = list()
    result = self.db_.getFileTypesWithAgivenRun(runnumber, processing, evt)
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        ftype = str(record[0])
        entityList += [self._getEntityFromPath(path, ftype, levels, 'File types')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
    
  ############################################################################# 
  def _getEntityFromPath(self, presentPath, newPathElement, level, leveldescription=None, selection = None, method = None):
     
    if isinstance(newPathElement, types.DictType):
      # this must be a file
      entity = objects.Entity(newPathElement)
      newPathElement = str(entity['name']).rsplit("/", 1)[1]
      entity.update({'FileName':entity['name']})
      expandable = False
      entity.update({'expandable':expandable})
      type = self.LHCB_BKDB_FILE_TYPE
      if selection!=None:
        entity.update({'selection':selection})
      
      if method!=None:
        entity.update({'method':method})
                                  
    else:
      # this must be a folder
      entity = objects.Entity()
      name = newPathElement
           
      expandable = True
      type = self.LHCB_BKDB_FOLDER_TYPE                            
                            
      fullPath  = presentPath.rstrip(INTERNAL_PATH_SEPARATOR)
      fullPath += INTERNAL_PATH_SEPARATOR + \
      name
      
      
      entity.update({'name':name, 'fullpath':fullPath,'expandable':expandable})
      
      if leveldescription <> None:
        entity.update({'level':leveldescription})
      
      if leveldescription == 'FileTypes':
        entity.update({'showFiles':0})
        
      if selection!=None:
        entity.update({'selection':selection})
      
      if method!=None:
        entity.update({'method':method})
   
      elif level == 5:
        entity.update({'showFiles':0})
      '''
      if not self.advancedQuery_ and level==4:
        entity.update({'showFiles':0})
      elif  self.advancedQuery_ and level==7:
        entity.update({'showFiles':0})
      elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[2] and level == 2:
        entity.update({'showFiles':0})
      elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[3] and level == 3:
        entity.update({'showFiles':0})
      '''
    return entity
  
  ############################################################################# 
  def _getSpecificEntityFromPath(self, presentPath, value, newPathElement, level, description=None, leveldescription=None, selection=None, method=None):
    if isinstance(value, types.DictType):
      entity = objects.Entity(value)
      type = self.LHCB_BKDB_FILE_TYPE
      name = newPathElement
                        
      expandable = True
      
      fullPath  = presentPath.rstrip(INTERNAL_PATH_SEPARATOR)
      fullPath += INTERNAL_PATH_SEPARATOR + \
      name
      
      if description<>None:
        entity.update({'name':description, 'fullpath':fullPath, 'expandable':expandable})  
      else:
        entity.update({'name':name, 'fullpath':fullPath, 'expandable':expandable})  
      if leveldescription <> None:
        entity.update({'level':leveldescription})
    
      if selection!=None:
        entity.update({'selection':selection})
      
      if method != None:
        entity.update({'method':method})
       
      if level == 5:
        entity.update({'showFiles':0})
      
    return entity
    

#    takes an absolute path and returns of tuples with prefixes and posfixes
#    of path elements. If invalid path returns null
  ############################################################################# 
  def _processPath(self, path):
    path = path.encode('ascii')
    path = path.strip(INTERNAL_PATH_SEPARATOR + " ")
    paths = path.split(self.getPathSeparator())    
    return paths

#   it caches an entity or a list of entities
  ############################################################################# 
  def _cacheIt(self, entityList):
    if isinstance(entityList, objects.Entity):
      # convert it into a list
      entityList = [entityList]
    elif not isinstance(entityList, types.ListType):
      # neither entity nor list
      gLogger.warn("couldn't cache invalid entity(list) of type " + str(entityList.__class))
      return
        
    for entity in entityList:
      # TO IMPLEMENT!! time of the caching
      try:
        self.__entityCache.update({entity['fullpath']: (entity, 0)})
      except:
        gLogger.warn("couldn't cache entity(?) " + str(entity))
        return S_ERROR('couldnt cache entity!')
        
  #############################################################################       
  def getAbsolutePath(self, path):
    # get current working directory if empty
    if path in [ "", ".", None] :
      path = INTERNAL_PATH_SEPARATOR # root        
      # convert it into absolute path    
    path = os.path.normpath(path)
    if os.sep != INTERNAL_PATH_SEPARATOR:
      path = path.replace(os.sep, INTERNAL_PATH_SEPARATOR)
            
    # for this special case of anomaly when double // may appear
    path = path.replace(2*INTERNAL_PATH_SEPARATOR, INTERNAL_PATH_SEPARATOR)
    
    return S_OK(path)
  
  #############################################################################       
  def get(self, path = "/"):
    path = self.getAbsolutePath(path)['Value']
    entity = self._getEntity(path)
    if entity.__class__ == types.NoneType:
      gLogger.error(path + " doesn't exist!");
      #raise ValueError, "Invalid path %s" % path
    return S_OK(entity)
    
  #############################################################################       
  def _getEntity(self, path):
    #
    # This is not doing anything at the moment
    #
    #
        
    # First try
    try:
      entity = self.__entityCache[path][0]
      gLogger.debug("getting " + str(path) + " from the cache")
      return entity
    except:
      # not cached so far
      gLogger.debug(str(path) + " not in cache. Fetching...")
      entityList = self.list(self.mergePaths(path, "..")['Value'])

    # Second try
    
    try:
      gLogger.debug("getting " + str(path) + " eventually from the cache")
      entity = self.__entityCache[path][0]
      return entity
    except:
      # still not in the cache... wrong path
      gLogger.warn(str(path) + " seems to be a wrong path");
      return None
        
    return entity
  
  #############################################################################       
  def getNumberOfEvents(self, files):
    sum = 0
    for file in files:
      sum += int(file['EventStat'])
    return sum
  
    
  def getJobInfo(self, lfn):
    result = self.db_.getJobInfo(lfn)
    value = None
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        prod = str(record[0])
        value = {'DiracJobID':record[0], 'DiracVersion':record[1], 'EventInputStat':record[2], 'ExecTime':record[3], 'FirstEventNumber':record[4], \
                 'Location':record[5], 'Name':record[6], 'NumberofEvents':record[7], \
                  'StatisticsRequested':record[8], 'WNCPUPOWER':record[9], 'CPUTime':record[10], 'WNCACHE':record[11], 'WNMEMORY':record[12], 'WNMODEL':record[13], 'WORKERNODE':record[14],'WNCPUHS06':record[15],'TotalLuminosity':record[17]}  
    else:
      gLogger.error(result['Message'])
    return value
  
  #############################################################################       
  def getLimitedFiles(self, SelectionDict, SortDict, StartItem, Maxitems):
    
    if self.parameter_ == self.LHCB_BKDB_PARAMETERS[0]:
      return self._getLimitedFilesConfigParams(SelectionDict, SortDict, StartItem, Maxitems) 
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[1]:
      return self._getLimitedFilesEventTypeParams(SelectionDict, SortDict, StartItem, Maxitems)
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[2]:
      return self._getLimitedFilesProductions(SelectionDict, SortDict, StartItem, Maxitems)
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[3]:
      return self._getLimitedFilesRuns(SelectionDict, SortDict, StartItem, Maxitems)
  
  #############################################################################       
  def _getLimitedFilesConfigParams(self, SelectionDict, SortDict, StartItem, Maxitems):  
    path = SelectionDict['fullpath']
    levels, processedPath,procpass = self.getLevelAndPath(path)
    r = procpass.split('/')[1:]
    for i in r:
      processedPath.remove(i)
    
    if self.advancedQuery_:
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1], 'ConditionDescription': processedPath[2],'EventTypeId': processedPath[3],'Production':processedPath[4],'FileType':processedPath[5]}
    else:
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1], 'ConditionDescription': processedPath[2],'EventTypeId': processedPath[3],'FileType':processedPath[4]}
    dict['ProcessingPass']=procpass
    dict['fullpath']=path
    
    return self.__getFiles(dict, SortDict, StartItem, Maxitems)
    
    
  
  #############################################################################       
  def _getLimitedFilesEventTypeParams(self, SelectionDict, SortDict, StartItem, Maxitems):
    path = SelectionDict['fullpath']
    levels, processedPath,procpass = self.getLevelAndPath(path)
    r = procpass.split('/')[1:]
    for i in r:
      processedPath.remove(i)
    
    if self.advancedQuery_:
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1],'EventTypeId': processedPath[2], 'ConditionDescription': processedPath[3],'Production':processedPath[4],'FileType':processedPath[5]}
    else:
      dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1], 'EventTypeId': processedPath[2],'ConditionDescription': processedPath[3],'FileType':processedPath[4]}
    dict['ProcessingPass']=procpass
    dict['fullpath']=path
    
    return self.__getFiles(dict, SortDict, StartItem, Maxitems)
    
  def _getLimitedFilesProductions(self, SelectionDict, SortDict, StartItem, Maxitems):
    path = SelectionDict['fullpath']
    levels, processedPath,procpass = self.getLevelAndPath(path)

    dict = { 'Production': processedPath[0], 'EventTypeId': processedPath[1],'FileType':processedPath[2]}
    dict['fullpath']=path
    
    return self.__getFiles(dict, SortDict, StartItem, Maxitems)

  #############################################################################
  def _getLimitedFilesRuns(self, SelectionDict, SortDict, StartItem, Maxitems):
    path = SelectionDict['fullpath']
    levels, processedPath,procpass = self.getLevelAndPath(path)
    r = procpass.split('/')[1:]
    for i in r:
      processedPath.remove(i)
      
    dict = {'RunNumber':processedPath[0],'EventTypeId': processedPath[1],'FileType':processedPath[2],'ProcessingPass':procpass}
    dict['fullpath']=path
    
    return self.__getFiles(dict, SortDict, StartItem, Maxitems)

  
  #############################################################################       
  def __getFiles(self, dict, SortDict, StartItem, Maxitems):
    totalrecords = 0
    nbOfEvents = 0
    filesSize = 0
    lumi = 0
    selection = dict
    if len(SortDict) > 0:
      res = self.db_.getFilesSumary(dict)
      if not res['OK']:
        gLogger.error(res['Message'])
      else:
        records = res['Value']['Records']
        params = res['Value']['ParameterNames']
        totalrecords = records[0][0]
        nbOfEvents = records[0][1]
        filesSize =  records[0][2]
        lumi = records[0][3]
    records = []
    parametersNames=[]
    if StartItem > -1 and Maxitems != 0:
      dict['StartItem']=StartItem
      dict['MaxItem']=Maxitems
      dict['Quality']=self.__getSelectedQualities()
      result = self.db_.getLimitedFiles(dict)
      
      if result['OK']:      
        parametersNames = result['Value']['ParameterNames'] 
        records = result['Value']['Records']
        totalrecords = result['Value']['TotalRecords']
      else:
        gLogger.error(result['Message'])
        return result
    
    return {'TotalRecords':totalrecords,'ParameterNames':parametersNames,'Records':records,'Extras': {'Selection':selection, 'GlobalStatistics':{'Number of Events':nbOfEvents, 'Files Size':filesSize,'Luminosity': lumi}} } 
  
  #############################################################################       
  def getAncestors(self, files, depth):
    return self.db_.getAncestors(files, depth)
  
  #############################################################################       
  def getLogfile(self, filename):
    return self.db_.getLogfile(filename)
  
  #############################################################################       
  def writePythonOrJobOptions(self, StartItem, Maxitems, path, savetype ): 
    path = self.getAbsolutePath(path)['Value'] # shall we do this here or in the _processedPath()?
    valid, processedPath = self._processPath(path)
    selection = {}
    if not valid:
      gLogger.error(path + " is not valid!");
      raise ValueError, "Invalid path '%s'" % path
        # get directory content
    levels = len(processedPath)
    gLogger.debug("listing files")
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    simid = processedPath[2][1]
    processing = processedPath[3][1]
    evtType = processedPath[4][1]
    prod = processedPath[5][1]
    ftype = processedPath[6][1]
    if len(processedPath) < 8:
      pname = 'ALL'
      pversion = 'ALL'
    else: 
      if processedPath[7][1] != 'ALL':
        pname = processedPath[7][1].split(' ')[0]
        pversion = processedPath[7][1].split(' ')[1]
      else:
        pname = processedPath[7][1]
        pversion = processedPath[7][1]
    retVal = self.__getFiles(configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion, {'sas':1}, StartItem, Maxitems, selection)
    values = retVal['Records']
    files = {}
    # The list has to be convert to dictionary
    for i in values:
      files[i[0]] = {'FileName':i[0],'EventStat':i[1], 'FileSize':i[2], 'FileType':i[7],'EventTypeId':i[8]}
      
    return self.writeJobOptions(files,optionsFile = '', savedType = savetype, catalog = None, savePfn=None)
  
  #############################################################################         
  def getLimitedInformations(self,StartItem, Maxitems, path):
    path = self.getAbsolutePath(path)['Value'] # shall we do this here or in the _processedPath()?
    valid, processedPath = self._processPath(path)
    selection = {}
    if not valid:
      gLogger.error(path + " is not valid!");
      raise ValueError, "Invalid path '%s'" % path
        # get directory content
    levels = len(processedPath)
    gLogger.debug("listing files")
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    simid = processedPath[2][1]
    processing = processedPath[3][1]
    evtType = processedPath[4][1]
    prod = processedPath[5][1]
    ftype = processedPath[6][1]
    if len(processedPath) < 8:
      pname = 'ALL'
      pversion = 'ALL'
    else: 
      if processedPath[7][1] != 'ALL':
        pname = processedPath[7][1].split(' ')[0]
        pversion = processedPath[7][1].split(' ')[1]
      else:
        pname = processedPath[7][1]
        pversion = processedPath[7][1]
    files = self.__getFiles(configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion, {'sas':1}, StartItem, Maxitems, selection)
    
    nbe = 0
    fsize=0
    nbfiles = 0
    
    for file in files['Records']:
      nbfiles += 1
      if file[2] != None:
        nbe += int(file[1])
      if file[2] != None:
        fsize += int(file[2])
    return S_OK({'Number of Events':nbe, 'Files Size':fsize,'Number of files':nbfiles} )  
    
  #############################################################################       
  def writeJobOptions(self, files, optionsFile = '', savedType = None, catalog = None, savePfn = None):
    fd = ''
    if optionsFile == '':
      fd = ''
      if savedType == 'txt':
        for lfn in files:
          fd += str(lfn)+'\n'
        return fd
    
    # get lst of event types
    import time
    evtTypes = {}
    nbEvts = 0
    fileType = None
    for i in files:
        file = files[i]
        type = int(file['EventTypeId'])
        stat = 0
        if file['EventStat'] != None:
          stat = int(file['EventStat'])
  
        if not evtTypes.has_key(type):
            evtTypes[type] = [0, 0, 0.]
        evtTypes[type][0] += 1
        evtTypes[type][1] += stat
        if files[i]['FileSize'] == None:
          evtTypes[type][2] += 0
        else:
          evtTypes[type][2] += int(file['FileSize'])/1000000000.
          
          
        nbEvts += stat
        if not fileType:
            fileType = file['FileType']
        if file['FileType'] != fileType:
            print "All files don't have the same type, impossible to write jobOptions"
            return 1
    
    pythonOpts = None
    if savedType != None:
      pythonOpts = savedType == 'py'
    else:
      fd = open(optionsFile, 'w')
      n,ext = os.path.splitext(optionsFile)
      pythonOpts = ext == '.py'  
      
    s = ''
    if pythonOpts:
        comment = "#-- "
    else:
        comment = "//-- "
    s += comment + "GAUDI jobOptions generated on " + time.asctime() + "\n"
    s += comment + "Contains event types : \n"
    types = evtTypes.keys()
    types.sort()
    for type in types:
        s += comment + "  %8d - %d files - %d events - %.2f GBytes\n"%(type, evtTypes[type][0], evtTypes[type][1], evtTypes[type][2])

    # Now write the event selector option
    if pythonOpts:
        s += "\nfrom Gaudi.Configuration import * \n"
        s += "\nEventSelector().Input   = [\n"
    else:
        s += "\nEventSelector.Input   = {\n"
    fileType = fileType.split()[0]
    # Allow fileType to be of the form XXX.<fileType>
    try:
      fileType = fileType.split(".")[1]
    except:
      pass
    poolTypes = ["DST", "RDST", "DIGI", "SIM", "XDST"]
    mdfTypes = ["RAW", "MDF"]
    etcTypes = ["SETC", "FETC", "ETC"]
    #lfns = [file['FileName'] for file in files]
    #lfns.sort()
    keys = files.keys()
    keys.sort()
    first = True
    for lfn in keys:
        file = files[lfn]
        if not first:
            s +=",\n"
        first = False
        if savePfn:
          if fileType in poolTypes:    
              s += "\"   DATAFILE=\'" + savePfn[lfn]['turl'] + "' TYP='POOL_ROOTTREE' OPT='READ'\""
          elif fileType in mdfTypes:
              s += "\"   DATAFILE=\'" + savePfn[lfn]['turl'] + "' SVC='LHCb::MDFSelector'\"" 
          elif fileType in etcTypes:
              s += "\"   COLLECTION='TagCreator/1' DATAFILE=\'" + savePfn[lfn]['turl'] + "' TYP='POOL_ROOT'\"" 
        else:
          if fileType in poolTypes:    
              s += "\"   DATAFILE='LFN:" + file['FileName'] + "' TYP='POOL_ROOTTREE' OPT='READ'\"" 
          elif fileType in mdfTypes:
              s += "\"   DATAFILE='LFN:" + file['FileName'] + "' SVC='LHCb::MDFSelector'\"" 
          elif fileType in etcTypes:
              s += "\"   COLLECTION='TagCreator/1' DATAFILE='LFN:" + file['FileName'] + "' TYP='POOL_ROOT'\""
          else:
            s += "\"   DATAFILE='LFN:" + file['FileName'] + "' TYP='POOL_ROOTTREE' OPT='READ'\""
    if pythonOpts:
        s += "]\n"
    else:
        s += "\n};\n"
    if catalog != None:
        s += "FileCatalog().Catalogs = [ 'xmlcatalog_file:"+catalog+"' ]\n"
    if fd:
      fd.write(s)
      fd.close()
    else:
      return s
    
  #############################################################################
  def getProcessingPassSteps(self, dict):
    return self.db_.getProcessingPassSteps(dict)
  
  #############################################################################       
  def getMoreProductionInformations(self, prodid):
    return self.db_.getMoreProductionInformations(prodid)
  
  #############################################################################
  def getAvailableProductions(self):
    return self.db_.getAvailableProductions()
  
  #############################################################################
  def getFileHistory(self, lfn):
    return self.db_.getFileHistory(lfn)
  
  #############################################################################
  def getProductionProcessingPassSteps(self, dict):
    return self.db_.getProductionProcessingPassSteps(dict)
  
  #############################################################################
  def getAvailableDataQuality(self):
    return self.db_.getAvailableDataQuality()
  
  #############################################################################
  def setDataQualities(self, values):
    self.dataQualities_ = values
  
  def __getSelectedQualities(self):
    res = []
    for i in self.dataQualities_:
      if self.dataQualities_[i]==True:
        res += [i]
    return res