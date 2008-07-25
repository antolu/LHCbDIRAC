########################################################################
# $Id: LHCB_BKKDBManager.py,v 1.51 2008/07/25 19:15:47 zmathe Exp $
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

__RCSID__ = "$Id: LHCB_BKKDBManager.py,v 1.51 2008/07/25 19:15:47 zmathe Exp $"

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
                                   'SCON',
                                   'PAS',
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
      result = self.db_.getAvailableConfigurations()
      
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          configs = record[0]+' '+record[1]
          entityList += [self._getEntityFromPath(path, configs, levels)]
          self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
    
    if levels == 1:
      gLogger.debug("listing Simulation Conditions!")
      config = processedPath[0][1]
      configName = config.split(' ')[0]
      configVersion = config.split(' ')[1]

      print "-----------------------------------------------------------"
      print "Selected parameters:"
      print "-----------------------------------------------------------"
      print "Configuration Name      | "+configName
      print "Configuration Version   | "+configVersion
      print "-----------------------------------------------------------"

      print "Available Simulation Conditions:\n"

      result = self.db_.getSimulationConditions(configName, configVersion) 
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          simid = str(record[0])
          value = {'SimulationCondition':simid, 'BeamCond':record[1],'BeamEnergy':record[2],'Generator':record[3],'MagneticFileld':record[4],'DetectorCond':record[5],'Luminosity':record[6]}
          entityList += [self._getSpecificEntityFromPath(path, value, simid, levels)]
          self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
    
    if levels == 2: 
      gLogger.debug("listing processing pass")
      value = processedPath[0][1]
      configName = value.split(' ')[0]
      configVersion = value.split(' ')[1]
      simid = int(processedPath[1][1])

      print "-----------------------------------------------------------"
      print "Selected parameters:"
      print "-----------------------------------------------------------"
      print "Configuration Name     | "+configName
      print "Configuration Version  | "+configVersion
      print "Simulation condition   | "+str(simid)
      print "-----------------------------------------------------------"

      print "Available processing pass:\n"

      result = self.db_.getProPassWithSimCond(configName, configVersion, simid)
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          prod = str(record[0])
          entityList += [self._getEntityFromPath(path, prod, levels)]
          self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
          

    if levels == 3:
      gLogger.debug("listing event types")
      value = processedPath[0][1]
      configName = value.split(' ')[0]
      configVersion = value.split(' ')[1]
      simid = int(processedPath[1][1])
      processing = processedPath[2][1]
 
      print "-----------------------------------------------------------"
      print "Selected parameters: "
      print "-----------------------------------------------------------"

      print "Configuration Name      | "+configName
      print "Configuration Version   | "+configVersion
      print "Simulation Condition    | "+str(simid)
      print "Processing Pass         | "+str(processing)
      print "-----------------------------------------------------------"

      print "Available event types types:"
      result = self.db_.getEventTypeWithSimcond(configName, configVersion, simid, processing)
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          evtType = str(record[0])
          value = {'Event Type':evtType,'Description':record[1]}
          entityList += [self._getSpecificEntityFromPath(path, value, evtType, levels)]
          self._cacheIt(entityList)
      else:
          gLogger.error(result['Message'])
      
        
      
    if levels == 4:
      value = processedPath[0][1]
      configName = value.split(' ')[0]
      configVersion = value.split(' ')[1]
      simid = int(processedPath[1][1])
      processing = processedPath[2][1]
      evtType = processedPath[3][1]
      
      print "-----------------------------------------------------------"
      print "Selected parameters:   "
      print "-----------------------------------------------------------"
      print "Configuration Name     | "+configName
      print "Configuration Version  | "+configVersion
      print "Simulation Condition   | "+str(simid)
      print "Processing Pass        | "+str(processing)
      print "Event type             | "+str(evtType)
      print "-----------------------------------------------------------"
      print "Available Production(s):"
      
      result = self.db_.getProductionsWithSimcond(configName, configVersion, simid, processing, evtType)
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          prod = str(record[0])
          entityList += [self._getEntityFromPath(path, prod, levels)]
          self._cacheIt(entityList)
        
        entityList += [self._getEntityFromPath(path, "ALL", levels)]
        self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
    
    if levels ==5:
      value = processedPath[0][1]
      configName = value.split(' ')[0]
      configVersion = value.split(' ')[1]
      simid = int(processedPath[1][1])
      processing = processedPath[2][1]
      evtType = processedPath[3][1]
      prod = processedPath[4][1]
      if prod=='ALL':
        prod=0
        
      print "-----------------------------------------------------------"
      print "Selected parameters:   "
      print "-----------------------------------------------------------"
      print "Configuration Name     | "+configName
      print "Configuration Version  | "+configVersion
      print "Simulation Condition   | "+str(simid)
      print "Processing Pass        | "+str(processing)
      print "Event type             | "+str(evtType)
      print "Production             | "+str(processedPath[4][1])
      print "-----------------------------------------------------------"
      print "Available file types:"
      
      result = self.db_.getFileTypesWithSimcond(configName, configVersion, simid, processing, evtType, prod)
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          ftype = str(record[0])
          entityList += [self._getEntityFromPath(path, ftype, levels)]
          self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])

        
    if levels ==6:       
      value = processedPath[0][1]
      configName = value.split(' ')[0]
      configVersion = value.split(' ')[1]
      simid = int(processedPath[1][1])
      processing = processedPath[2][1]
      evtType = processedPath[3][1]
      prod = processedPath[4][1]
      if prod=='ALL':
        prod=0
      ftype = processedPath[5][1]  
      
      print "-----------------------------------------------------------"
      print "Selected parameters:   "
      print "-----------------------------------------------------------"
      print "Configuration Name     | "+configName
      print "Configuration Version  | "+configVersion
      print "Simulation Condition   | "+str(simid)
      print "Processing Pass        | "+str(processing)
      print "Event type             | "+str(evtType)
      print "Production             | "+str(processedPath[4][1])
      print "File Type              | "+str(ftype)
      print "-----------------------------------------------------------"
      print "Available program name and version:"
      
      result = self.db_.getProgramNameWithSimcond(configName, configVersion, simid, processing, evtType, prod, ftype)
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          programName = record[0]
          programVersion = record[1]
          nb = record[2]
          program = programName+' '+programVersion
          value = { 'Program Name':programName,'Program Version':programVersion,'Number Of Events':nb }
          entityList += [self._getSpecificEntityFromPath(path, value, program, levels)]
          self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])

    
    if levels == 7:
      self.files_ = []
      gLogger.debug("listing files")
      value = processedPath[0][1]
      configName = value.split(' ')[0]
      configVersion = value.split(' ')[1]
      simid = int(processedPath[1][1])
      processing = processedPath[2][1]
      evtType = processedPath[3][1]
      prod = processedPath[4][1]
      if prod=='ALL':
        prod=0
      ftype = processedPath[5][1]
      pname = processedPath[6][1].split(' ')[0]
      pversion = processedPath[6][1].split(' ')[1]

      print "-----------------------------------------------------------"
      print "Selected parameters:   "
      print "-----------------------------------------------------------"
      print "Configuration Name     | "+configName
      print "Configuration Version  | "+configVersion
      print "Simulation Condition   | "+str(simid)
      print "Processing Pass        | "+str(processing)
      print "Event type             | "+str(evtType)
      print "Production             | "+str(processedPath[4][1])
      print "File Type              | "+str(ftype)
      print "Program name           | "+pname
      print "Program version        | "+pversion
      print "-----------------------------------------------------------"
      print "File list:\n"
      
      result = self.db_.getFilesWithSimcond(configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion)
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          value = {'name':record[0],'EventStat':record[1], 'FileSize':record[2],'CreationDate':record[3],'Generator':record[4],'GeometryVersion':record[5],       'JobStart':record[6], 'JobEnd':record[7],'WorkerNode':record[8],'FileType':ftype, 'EvtTypeId':int(evtType)}
          self.files_ += [record[0]]
          entityList += [self._getEntityFromPath(path, value, levels)]
          self._cacheIt(entityList)    
      else:
        gLogger.error(result['Message'])

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
      result = self.db_.getAvailableEventTypes()
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          event = str(record[0])
          value = {'Event Type':record[0],'Description':record[1]}
          entityList += [self._getSpecificEntityFromPath(path, value, event, levels)]
          self._cacheIt(entityList)                  
      else:
        gLogger.error(result['Message'])
   
    if levels == 1:
      eventType = processedPath[0][1]
      print "-----------------------------------------------------------"
      print "Selected parameters:   "
      print "-----------------------------------------------------------"
      print "Event type              | "+str(eventType)
      print "-----------------------------------------------------------"
      
      print "Available configuration name and version:" 
      
      result = self.db_.getConfigNameAndVersion(int(eventType))
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          configs = record[0]+' '+record[1]
          entityList += [self._getEntityFromPath(path, configs, levels)]
          self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
    
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
      result = self.db_.getAvailableProcessingPass(configName, configVersion, int(eventType))
      if result['OK']:
        dbResult = result['Value']

        for record in dbResult:
          processing = record[0]
          value = {'Total processing pass':record[0]}
          entityList += [self._getSpecificEntityFromPath(path, value, processing, levels)]
          self._cacheIt(entityList)  
      else:
        gLogger.error(result['Message'])
      
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
      result = self.db_.getProductionsWithEventTypes(int(eventType), configName,  configVersion, processingPass)
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          prod = str(record[0])
          entityList += [self._getEntityFromPath(path, prod, levels)]
          self._cacheIt(entityList)
        
        entityList += [self._getEntityFromPath(path, "ALL", levels)]
        self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
      
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
        result = self.db_.getFileTypesWithEventType(configName, configVersion, int(eventType), int(production))
        if result['OK']:
          dbResult = result['Value']
        else:
          gLogger.error(result['Message'])
      else:
        result = self.db_.getFileTypesWithEventTypeALL(configName, configVersion, int(eventType))
        if result['OK']:
          dbResult = result['Value']
        else:
          gLogger.error(result['Message'])

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
        result = self.db_.getFilesByEventType(configName, configVersion, fileType, int(eventType), int(production))     
        if result['OK']:
          dbResult = result['Value']
        else:
          gLogger.error(result['Message'])
      else:
        result = self.db_.getFilesByEventTypeALL(configName, configVersion, fileType, int(eventType))
        if result['OK']:
          dbResult = result['Value']
        else:
          gLogger.error(result['Message'])
          
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
      result = self.db_.getProcessingPass()
      if result['OK']:
        dbResult = result['Value']
  
        for record in dbResult:
          processing = record[0]
          value = {'Step 0':record[1],'Step 1':record[2],'Step 2':record[3],'Step 3':record[4],'Step 4':record[5]}
          entityList += [self._getSpecificEntityFromPath(path, value, processing, levels)]
          self._cacheIt(entityList)                  
      else:
        gLogger.error(result['Message'])
   
    if levels == 1:
      gLogger.debug("listing productions")
      processingPass = processedPath[0][1]

      print "-----------------------------------------------------------"
      print "Selected parameters:    "
      print "-----------------------------------------------------------"
      print "Processing Pass:      | "+str(processingPass)
      print "-----------------------------------------------------------"
     
      print "Available productions:"
      result = self.db_.getProductionsWithPocessingPass(processingPass)
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          prod = str(record[0])
          entityList += [self._getEntityFromPath(path, prod, levels)]
          self._cacheIt(entityList)
        
        entityList += [self._getEntityFromPath(path, "ALL", levels)]
        self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])

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
      
      result = self.db_.getEventTyesWithProduction(prod)
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          eventtype = str(record[0])
          value = {'EventTypeID':eventtype, 'Description':record[1]}
          entityList += [self._getSpecificEntityFromPath(path, value, eventtype, levels)]
          self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
   
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

      result = self.db_.getFileTypesWithProduction(prod, evt)
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          fileType = str(record[0])
          entityList += [self._getEntityFromPath(path, fileType, levels)]
          self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])

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

      result = self.db_.getFilesByProduction(prod, evt, fileType)
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          value = {'name':record[0],'EventStat':record[1], 'FileSize':record[2],'CreationDate':record[3],'Generator':record[4],'GeometryVersion':record[5],    'JobStart':record[6], 'JobEnd':record[7],'WorkerNode':record[8]}
          self.files_ += [record[0]]
          entityList += [self._getEntityFromPath(path, value, levels)]
          self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
 

    return entityList
 
  ############################################################################# 
  def _getEntityFromPath(self, presentPath, newPathElement, level):
     
    if isinstance(newPathElement, types.DictType):
      # this must be a file
      entity = Entity(newPathElement)
      newPathElement = str(entity['name']).rsplit("/", 1)[1]
      entity.update({'FileName':entity['name']})
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
  
  #############################################################################       
  def writeJobOptions(self, files, optionsFile = "jobOptions.opts"):
       # get lst of event types
    import time
    evtTypes = {}
    nbEvts = 0
    fileType = None
    for file in files:
        type = int(file['EvtTypeId'])
        stat = int(file['EventStat'])
        if not evtTypes.has_key(type):
            evtTypes[type] = [0, 0, 0.]
        evtTypes[type][0] += 1
        evtTypes[type][1] += stat
        evtTypes[type][2] += int(file['FileSize'])/1000000000.
        nbEvts += stat
        if not fileType:
            fileType = file['FileType']
        if file['FileType'] != fileType:
            print "All files don't have the same type, impossible to write jobOptions"
            return 1

    fd = open(optionsFile, 'w')
    n,ext = os.path.splitext(optionsFile)
    pythonOpts = ext == '.py'
    if pythonOpts:
        comment = "#-- "
    else:
        comment = "//-- "
    fd.write(comment + "GAUDI jobOptions generated on " + time.asctime() + "\n")
    fd.write(comment + "Contains event types : \n")
    types = evtTypes.keys()
    types.sort()
    for type in types:
        fd.write(comment + "  %8d - %d files - %d events - %.2f GBytes\n"%(type, evtTypes[type][0], evtTypes[type][1], evtTypes[type][2]))

    # Now write the event selector option
    if pythonOpts:
        fd.write("\nEventSelector.Input()   = [\n")
    else:
        fd.write("\nEventSelector.Input   = {\n")
    fileType = fileType.split()[0]
    poolTypes = ["DST", "RDST", "DIGI", "SIM"]
    mdfTypes = ["RAW"]
    etcTypes = ["SETC", "FETC", "ETC"]
    lfns = [file['FileName'] for file in files]
    lfns.sort()
    first = True
    for lfn in lfns:
        if not first:
            fd.write(",\n")
        first = False
        if fileType in poolTypes:
            fd.write("\"   DATAFILE='LFN:" + file['FileName'] + "' TYP='POOL_ROOTTREE' OPT='READ'\"")
        elif fileType in mdfTypes:
            fd.write("\"   DATAFILE='LFN:" + file['FileName'] + "' SVC='LHCb::MDFSelector'\"")
        elif fileType in etcTypes:
            fd.write("\"   COLLECTION='TagCreator/1' DATAFILE='LFN:" + file['FileName'] + "' TYP='POOL_ROOT'\"")
    if pythonOpts:
        fd.write("]\n")
    else:
        fd.write("\n};\n")
    fd.close

