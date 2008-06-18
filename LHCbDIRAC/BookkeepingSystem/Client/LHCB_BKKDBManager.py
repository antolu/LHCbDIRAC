########################################################################
# $Id: LHCB_BKKDBManager.py,v 1.43 2008/06/18 10:23:27 zmathe Exp $
########################################################################

"""
LHCb Bookkeeping database manager
"""

from DIRAC                                                               import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Client.BaseESManager                        import BaseESManager
from DIRAC.BookkeepingSystem.Client.BookkeepingClient                    import BookkeepingClient
from DIRAC.BookkeepingSystem.Client.objects                              import Entity
from DIRAC.BookkeepingSystem.Client.Help                                 import Help
#from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
import os
import types
import sys

__RCSID__ = "$Id: LHCB_BKKDBManager.py,v 1.43 2008/06/18 10:23:27 zmathe Exp $"

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
  LHCB_BKDB_PREFIXES_CONFIG =     ['CFG',    # configurations
                                   'EVT',    # event type
                                   'PROD',   #production 
                                   'FTY',    #file type 
                                   'PRO',    #program name and version
                                    '',    # filename                                 
                                   ]
  
  LHCB_BKDB_PREFIXES_PROCESSING = ['PPA',
                                   'PRO',
                                   'EVT',
                                   'FTY',
                                   ''
                                  ]
  
  LHCB_BKDB_PREFIXES_EVENTTYPE = ['EVT',
                                  'CFG',
                                  'PAS',
                                  'PROD',
                                  'FTY',
                                   ''
                                  ]
  LHCB_BKDB_PREFIXES=[]

  LHCB_BKDB_PARAMETERS = ['Configuration', 'Event type' ,'Processing Pass' ]
    
  LHCB_BKDB_PREFIX_SEPARATOR = "_"
  
  ############################################################################# 
  def __init__(self):
    super(LHCB_BKKDBManager, self).__init__()
    self._BaseESManager___fileSeparator = INTERNAL_PATH_SEPARATOR    
    #self.__pathSeparator = INTERNAL_PATH_SEPARATOR
    self.db_ = BookkeepingClient()
    self.lfc_ = None #LcgFileCatalogCombinedClient()
    
    self.helper_ = Help()
    
    self.entityCache_ = {'/':(Entity({'name':'/', 'fullpath':'/'}), 0)} 
    self.parameter_ = self.LHCB_BKDB_PARAMETERS[0]
    self.LHCB_BKDB_PREFIXES = self.LHCB_BKDB_PREFIXES_CONFIG
    self.files_ = []

    self.treeLevels_ = -1
    print "First please choise which kind of queries you want to use!"
    print "The default value is Configuration!"
    print "The possible parameters:"
    print self.getPossibleParameters()
    print "For Example:"
    print "client.setParameter('Processing Pass')"
    print "If you need help, you will use client.help() commnad."

  ############################################################################# 
  def _updateTreeLevels(self, level):
    self.treeLevels_ = level
  
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
  def setParameter(self, name):
    if self.LHCB_BKDB_PARAMETERS.__contains__(name):
      self.parameter_ = name
      self.treeLevels_ = -1
      if name == 'Configuration':
      	self.LHCB_BKDB_PREFIXES = self.LHCB_BKDB_PREFIXES_CONFIG
      elif name == 'Processing Pass':
        self.LHCB_BKDB_PREFIXES = self.LHCB_BKDB_PREFIXES_PROCESSING
      elif name == 'Event type':
        self.LHCB_BKDB_PREFIXES = self.LHCB_BKDB_PREFIXES_EVENTTYPE
   
    else:
      print "Wrong Parameter!"
  
  #############################################################################
  def getLogicalFiles(self):
    return self.files_ 
  
  #############################################################################
  def getFilesPFN(self):
    lfns = self.files_
    res = []#self.lfc_.getPfnsByLfnList(lfns)
    return res
  
  ############################################################################# 
  def list(self, path=""):
    
    if self.parameter_ == self.LHCB_BKDB_PARAMETERS[0]:
      return self._listConfigs(path) 
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[1]:
      return self._listEventTypes(path)
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[2]:
      return self._listProcessing(path)
  
  
  ############################################################################# 
  def _listConfigs(self, path):
    
    entityList = list()
    path = self.getAbsolutePath(path)['Value'] # shall we do this here or in the _processedPath()?
    valid, processedPath = self._processPath(path)
   
    if not valid:
      gLogger.error(path + " is not valid!");
      raise ValueError, "Invalid path '%s'" % path
        # get directory content
    levels = len(processedPath)
    self._updateTreeLevels(levels)
  
    if levels == 0:    
      print "-----------------------------------------------------------"
      print "Configurations name and version:\n"
      print "-----------------------------------------------------------"

      # list root
      gLogger.debug("listing configurations")
      dbResult = self.db_.getAviableConfiguration()
      for record in dbResult:
        configs = record[0]+' '+record[1]
        entityList += [self._getEntityFromPath(path, configs, levels)]
        self._cacheIt(entityList)
    
    if levels == 1:
      gLogger.debug("listing Event Types")
      config = processedPath[0][1]
      configName = config.split(' ')[0]
      configVersion = config.split(' ')[1]

      print "-----------------------------------------------------------"
      print "Selected parameters:"
      print "-----------------------------------------------------------"
      print "Configuration Name      | "+configName
      print "Configuration Version   | "+configVersion
      print "-----------------------------------------------------------"

      print "Available Event types:\n"

      dbResult = self.db_.getEventTypes(configName, configVersion) 
      for record in dbResult:
        eventtypes = str(record[0])
        value = {'EventTypeID':eventtypes, 'Description':record[1]}
        entityList += [self._getSpecificEntityFromPath(path, value, eventtypes, levels)]
        self._cacheIt(entityList)
    
    if levels == 2: 
      gLogger.debug("listing productions")
      value = processedPath[0][1]
      configName = value.split(' ')[0]
      configVersion = value.split(' ')[1]
      eventType = int(processedPath[1][1])

      print "-----------------------------------------------------------"
      print "Selected parameters:"
      print "-----------------------------------------------------------"
      print "Configuration Name     | "+configName
      print "Configuration Version  | "+configVersion
      print "Event type             | "+str(eventType)
      print "-----------------------------------------------------------"

      print "Available productions:\n"

      dbResult = self.db_.getProductions(configName, configVersion, eventType)
      for record in dbResult:
        prod = str(record[0])
        entityList += [self._getEntityFromPath(path, prod, levels)]
        self._cacheIt(entityList)
        
      entityList += [self._getEntityFromPath(path, "ALL", levels)]
      self._cacheIt(entityList)
          

    if levels == 3:
      gLogger.debug("listing filetypes")
      value = processedPath[0][1]
      configName = value.split(' ')[0]
      configVersion = value.split(' ')[1]
      eventType = int(processedPath[1][1])
      if processedPath[2][1]!= 'ALL':
      	prod = int(processedPath[2][1])
      else:
        prod = processedPath[2][1]
 
      print "-----------------------------------------------------------"
      print "Selected parameters: "
      print "-----------------------------------------------------------"

      print "Configuration Name      | "+configName
      print "Configuration Version   | "+configVersion
      print "Event type              | "+str(eventType)
      print "Production              | "+str(prod)
      print "-----------------------------------------------------------"

      print "Available file types:"
      dbResult = None
      if prod != 'ALL':
        dbResult = self.db_.getFileTypes(configName, configVersion, eventType, prod)
      else:
        S_ERROR("NOT implemented!")
      for record in dbResult:
        fileType = record[0]
        entityList += [self._getEntityFromPath(path, fileType, levels)]
        self._cacheIt(entityList)
    
    if levels == 4:
      value = processedPath[0][1]
      configName = value.split(' ')[0]
      configVersion = value.split(' ')[1]
      eventType = int(processedPath[1][1])
      if processedPath[2][1] != 'ALL':
        prod = int(processedPath[2][1])
      filetype = processedPath[3][1]
      print "-----------------------------------------------------------"
      print "Selected parameters:   "
      print "-----------------------------------------------------------"
      print "Configuration Name     | "+configName
      print "Configuration Version  | "+configVersion
      print "Event type             | "+str(eventType)
      print "Production             | "+str(prod)
      print "File type              | "+filetype
      print "-----------------------------------------------------------"
      print "Available Program Name and Program Version:"
      dbResult = self.db_.getProgramNameAndVersion(configName, configVersion, eventType, prod, filetype)
      for record in dbResult:
        programName = record[0]
        programVersion = record[1]
        nb = record[2]
        program = programName+' '+programVersion
        value = { 'Program Name':programName,'Program Version':programVersion,'Number Of Events':nb }
        entityList += [self._getSpecificEntityFromPath(path, value, program, levels)]
        self._cacheIt(entityList)
    
    if levels == 5:
      self.files_ = []
      gLogger.debug("listing files")
      value = processedPath[0][1]
      configName = value.split(' ')[0]
      configVersion = value.split(' ')[1]
      eventType = int(processedPath[1][1])
      prod = processedPath[2][1]
      if prod != 'ALL':
        prod = int(processedPath[2][1])
      
      filetype = processedPath[3][1]

      pname = processedPath[4][1].split(' ')[0]
      pversion = processedPath[4][1].split(' ')[1]

      print "-----------------------------------------------------------"
      print "Selected parameters:   "
      print "-----------------------------------------------------------"
      print "Configuration Name     | "+configName
      print "Configuration Version  | "+configVersion
      print "Event type             | "+str(eventType)
      print "Production             | "+str(prod)
      print "File type              | "+filetype
      print "Program name           | "+pname
      print "Program version        | "+pversion
      print "-----------------------------------------------------------"
      print "File list:\n"
      
      dbResult = None
      if prod != 'ALL':
        dbResult = self.db_.getSpecificFiles(configName,configVersion,pname,pversion,filetype,eventType,prod)
      else:
        dbResult = self.db_.getSpecificFilesWithoutProd(configName,configVersion,pname,pversion,filetype,eventType)
      
      for record in dbResult:
        value = {'name':record[0],'EventStat':record[1], 'FileSize':record[2],'CreationDate':record[3],'Generator':record[4],'GeometryVersion':record[5],       'JobStart':record[6], 'JobEnd':record[7],'WorkerNode':record[8]}
        self.files_ += [record[0]]
        entityList += [self._getEntityFromPath(path, value, levels)]
        self._cacheIt(entityList)    

    
    return entityList
  
  ############################################################################# 
  def _listEventTypes(self, path):
    entityList = list()
    path = self.getAbsolutePath(path)['Value'] # shall we do this here or in the _processedPath()?
    valid, processedPath = self._processPath(path)
    if not valid:
      gLogger.error(path + " is not valid!");
      raise ValueError, "Invalid path '%s'" % path
        # get directory content
    levels = len(processedPath)
    self._updateTreeLevels(levels)
  
    if levels == 0:    
      print "-----------------------------------------------------------"
      print "Available Event types:                                     "              
      print "-----------------------------------------------------------"

      gLogger.debug("listing event types")
      dbResult = self.db_.getAvailableEventTypes()
      for record in dbResult:
        event = str(record[0])
        value = {'Event Type':record[0],'Description':record[1]}
        entityList += [self._getSpecificEntityFromPath(path, value, event, levels)]
        self._cacheIt(entityList)                  
   
    if levels == 1:
      eventType = processedPath[0][1]
      print "-----------------------------------------------------------"
      print "Selected parameters:   "
      print "-----------------------------------------------------------"
      print "Event type              | "+str(eventType)
      print "-----------------------------------------------------------"
      
      print "Available configuration name and version:" 
      
      dbResult = self.db_.getConfigNameAndVersion(int(eventType))
      for record in dbResult:
        configs = record[0]+' '+record[1]
        entityList += [self._getEntityFromPath(path, configs, levels)]
        self._cacheIt(entityList)
    
    if levels == 2:
      gLogger.debug("listing processing pass")
      eventType = processedPath[0][1]
      config = processedPath[1][1]
      configName = config.split(' ')[0]
      configVersion = config.split(' ')[1]

      print "-----------------------------------------------------------"
      print "Selected parameters:"
      print "-----------------------------------------------------------"
      print "Event Type              | "+ str(eventType)
      print "Configuration Name      | "+configName
      print "Configuration Version   | "+configVersion
      print "-----------------------------------------------------------"
      
      print "Available processing pass:"
      dbResult = self.db_.getAvailableProcessingPass(configName, configVersion, int(eventType))

      for record in dbResult:
        processing = record[0]
        value = {'Total processing pass':record[0]}
        entityList += [self._getSpecificEntityFromPath(path, value, processing, levels)]
        self._cacheIt(entityList)  
      
    if levels == 3:
      gLogger.debug("listing production!")
      eventType = processedPath[0][1]
      config = processedPath[1][1]
      configName = config.split(' ')[0]
      configVersion = config.split(' ')[1]
      processingPass = processedPath[2][1]

      print "-----------------------------------------------------------"
      print "Selected parameters:"
      print "-----------------------------------------------------------"
      print "Event Type              | "+ str(eventType)
      print "Configuration Name      | "+configName
      print "Configuration Version   | "+configVersion
      print "Processing pass         | "+processingPass
      print "-----------------------------------------------------------"
      
      print "Aviable productions:"
      dbResult = self.db_.getProductionsWithEventTypes(int(eventType), configName,  configVersion, processingPass)
      for record in dbResult:
        prod = str(record[0])
        entityList += [self._getEntityFromPath(path, prod, levels)]
        self._cacheIt(entityList)
        
      entityList += [self._getEntityFromPath(path, "ALL", levels)]
      self._cacheIt(entityList)
      
    if levels == 4:
      gLogger.debug("listing production!")
      eventType = processedPath[0][1]
      config = processedPath[1][1]
      configName = config.split(' ')[0]
      configVersion = config.split(' ')[1]
      processingPass = processedPath[2][1]
      production = processedPath[3][1]

      print "-----------------------------------------------------------"
      print "Selected parameters:"
      print "-----------------------------------------------------------"
      print "Event Type              | "+ str(eventType)
      print "Configuration Name      | "+configName
      print "Configuration Version   | "+configVersion
      print "Processing pass         | "+processingPass
      print "Production              | "+production
      print "-----------------------------------------------------------"
      
      print "Available file type:"
      dbResult = None
      if production != 'ALL':
        dbResult = self.db_.getFileTypesWithEventType(configName, configVersion, int(eventType), int(production))
      else:
        dbResult = self.db_.getFileTypesWithEventTypeALL(configName, configVersion, int(eventType))

      for record in dbResult:
        fileType = str(record[0])
        entityList += [self._getEntityFromPath(path, fileType, levels)]
        self._cacheIt(entityList)

    if levels == 5:
      gLogger.debug("listing files!")
      eventType = processedPath[0][1]
      config = processedPath[1][1]
      configName = config.split(' ')[0]
      configVersion = config.split(' ')[1]
      processingPass = processedPath[2][1]
      production = processedPath[3][1]
      fileType = processedPath[4][1]
  
      print "-----------------------------------------------------------"
      print "Selected parameters:"
      print "-----------------------------------------------------------"
      print "Event Type              | "+ str(eventType)
      print "Configuration Name      | "+configName
      print "Configuration Version   | "+configVersion
      print "Production              | "+processingPass
      print "File type               | "+fileType
      print "-----------------------------------------------------------"
      
      print "File list:"
      if production != 'ALL':    
        dbResult = self.db_.getFilesByEventType(configName, configVersion, fileType, int(eventType), int(production))     
      else:
        dbResult = self.db_.getFilesByEventTypeALL(configName, configVersion, fileType, int(eventType))
      for record in dbResult:
        value = {'name':record[0],'EventStat':record[1], 'FileSize':record[2],'CreationDate':record[3],'Generator':record[4],'GeometryVersion':record[5],    'JobStart':record[6], 'JobEnd':record[7],'WorkerNode':record[8]}
        self.files_ += [record[0]]
        entityList += [self._getEntityFromPath(path, value, levels)]
        self._cacheIt(entityList)
    return entityList
  
  ############################################################################# 
  def _listProcessing(self, path):
    entityList = list()
    path = self.getAbsolutePath(path)['Value'] # shall we do this here or in the _processedPath()?
    valid, processedPath = self._processPath(path)
   
    if not valid:
      gLogger.error(path + " is not valid!");
      raise ValueError, "Invalid path '%s'" % path
        # get directory content
    levels = len(processedPath)
    self._updateTreeLevels(levels)
  
    if levels == 0:    
      print "-----------------------------------------------------------"
      print "Processing Pass:"
      print "-----------------------------------------------------------"

      gLogger.debug("listing processing pass")
      dbResult = self.db_.getProcessingPass()
      for record in dbResult:
        processing = record[0]
        value = {'Step 0':record[1],'Step 1':record[2],'Step 2':record[3],'Step 3':record[4],'Step 4':record[5]}
        entityList += [self._getSpecificEntityFromPath(path, value, processing, levels)]
        self._cacheIt(entityList)                  
   
    if levels == 1:
      gLogger.debug("listing productions")
      processingPass = processedPath[0][1]

      print "-----------------------------------------------------------"
      print "Selected parameters:    "
      print "-----------------------------------------------------------"
      print "Processing Pass:      | "+str(processingPass)
      print "-----------------------------------------------------------"
     
      print "Available productions:"
      dbResult = self.db_.getProductionsWithPocessingPass(processingPass)
      for record in dbResult:
        prod = str(record[0])
        entityList += [self._getEntityFromPath(path, prod, levels)]
        self._cacheIt(entityList)
        
      entityList += [self._getEntityFromPath(path, "ALL", levels)]
      self._cacheIt(entityList)

    if levels == 2:
      processingPass = processedPath[0][1]
      prod =  int(processedPath[1][1])
      print "-----------------------------------------------------------"
      print "Selected parameters:   "
      print "-----------------------------------------------------------"
      print "Processing Pass:       |"+str(processingPass)
      print "Production:            | "+str(prod)
      print "-----------------------------------------------------------"
      
      print "Available Event types:"
      
      dbResult = self.db_.getEventTyesWithProduction(prod)
      for record in dbResult:
        eventtype = str(record[0])
        value = {'EventTypeID':eventtype, 'Description':record[1]}
        entityList += [self._getSpecificEntityFromPath(path, value, eventtype, levels)]
        self._cacheIt(entityList)
   
    if levels == 3:
      processingPass = processedPath[0][1]
      prod =  int(processedPath[1][1])
      evt = int(processedPath[2][1])
      print "-----------------------------------------------------------"
      print "Selected parameters:   "
      print "-----------------------------------------------------------"
      print "Processing Pass:       |"+str(processingPass)
      print "Production:            |"+str(prod)
      print "Event type:            |"+str(evt)
      print "-----------------------------------------------------------"

      print "Available file types:"

      dbResult = self.db_.getFileTypesWithProduction(prod, evt)
      for record in dbResult:
        fileType = str(record[0])
        entityList += [self._getEntityFromPath(path, fileType, levels)]
        self._cacheIt(entityList)

    if levels == 4:
      self.files_ = []
      processingPass = processedPath[0][1]
      prod =  int(processedPath[1][1])
      evt = int(processedPath[2][1])
      fileType = str(processedPath[3][1])
      print "-----------------------------------------------------------"
      print "Selected parameters:   "
      print "-----------------------------------------------------------"
      print "Processing Pass:       |"+str(processingPass)
      print "Production:            |"+str(prod)
      print "Event type:            |"+str(evt)
      print "File type              |"+fileType
      print "-----------------------------------------------------------"

      print "Available files:"

      dbResult = self.db_.getFilesByProduction(prod, evt, fileType)
      for record in dbResult:
        value = {'name':record[0],'EventStat':record[1], 'FileSize':record[2],'CreationDate':record[3],'Generator':record[4],'GeometryVersion':record[5],    'JobStart':record[6], 'JobEnd':record[7],'WorkerNode':record[8]}
        self.files_ += [record[0]]
        entityList += [self._getEntityFromPath(path, value, levels)]
        self._cacheIt(entityList)

 

    return entityList
 
  ############################################################################# 
  def _getEntityFromPath(self, presentPath, newPathElement, level):
     
    if isinstance(newPathElement, types.DictType):
      # this must be a file
      entity = Entity(newPathElement)
      newPathElement = str(entity['name']).rsplit("/", 1)[1]
      entity.update({'Logical file name':entity['name']})
      expandable = False
      type = self.LHCB_BKDB_FILE_TYPE                            
    else:
      # this must be a folder
      entity = Entity()
      newPathElement = self.LHCB_BKDB_PREFIXES[level]+ \
      self.LHCB_BKDB_PREFIX_SEPARATOR + \
      newPathElement
      expandable = True
      type = self.LHCB_BKDB_FOLDER_TYPE                            
                            
      fullPath  = presentPath.rstrip(INTERNAL_PATH_SEPARATOR)
      fullPath += INTERNAL_PATH_SEPARATOR + \
      newPathElement
      entity.update({'name':newPathElement, 'fullpath':fullPath})
    
    return entity
  
  def _getSpecificEntityFromPath(self, presentPath, value, newPathElement, level):
    if isinstance(value, types.DictType):
      entity = Entity(value)
      expandable = False
      type = self.LHCB_BKDB_FILE_TYPE
      newPathElement = self.LHCB_BKDB_PREFIXES[level]+ \
      self.LHCB_BKDB_PREFIX_SEPARATOR + \
      newPathElement

      fullPath  = presentPath.rstrip(INTERNAL_PATH_SEPARATOR)
      fullPath += INTERNAL_PATH_SEPARATOR + \
      newPathElement
      entity.update({'name':newPathElement, 'fullpath':fullPath})  
    return entity
    

#    takes an absolute path and returns of tuples with prefixes and posfixes
#    of path elements. If invalid path returns null
  ############################################################################# 
  def _processPath(self, path):
    path = path.encode('ascii')
    correctPath = True
    path = path.strip(INTERNAL_PATH_SEPARATOR + " ")
    tokens = path.split(self.getPathSeparator())
    if tokens == ['']:
      # path is root i.e. '\'
      return True, []
    result = []
    counter = 0;
    correctPath = True        
    for token in tokens:
      prefix, suffix = self._splitPathElement(token)
      if self.LHCB_BKDB_PREFIXES[counter] != prefix:
        # the prefix is not at the right location in the path or it is coming too late
        correctPath = False
        break                        
      result += [(prefix, suffix)]
      counter += 1
      gLogger.debug("processPath=" + str(result))    
    
    return (correctPath, result)

  ############################################################################# 
  def _splitPathElement(self, pathElement):
    t = pathElement.split(self.LHCB_BKDB_PREFIX_SEPARATOR, 1)
    if (pathElement[0] == self.LHCB_BKDB_PREFIX_SEPARATOR or  # starts with separator
        len(t) == 1 or                                         # contains no separator
        t[0] not in self.LHCB_BKDB_PREFIXES                    # no declared prefix
        ):
      t = ['', pathElement]    # then it will be a filename
    return t 

#   it caches an entity or a list of entities
  ############################################################################# 
  def _cacheIt(self, entityList):
    if isinstance(entityList, Entity):
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
      return 
        
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
  def get(self, path = ""):
    path = self.getAbsolutePath(path)['Value']
    entity = self._getEntity(path)
    if entity.__class__ == types.NoneType:
      gLogger.error(path + " doesn't exist!");
      raise ValueError, "Invalid path %s" % path
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
      entityList = self.list(self.mergePaths(path, ".."))
      self._cacheIt(entityList)
            

    # Second try
    try:
      gLogger.debug("getting " + str(path) + " eventually from the cache")
      entity = self.__entityCache[path][0]
      return entity
    except:
      # still not in the cache... wrong path
      gLogger.warning(str(path) + " seems to be a wrong path");
      return None
        
    return entity
  
  #############################################################################       
  def getNumberOfEvents(self, files):
    sum = 0
    for file in files:
      sum += int(file['EventStat'])
    return sum
