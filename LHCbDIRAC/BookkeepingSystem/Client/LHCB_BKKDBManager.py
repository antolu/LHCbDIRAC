########################################################################
# $Id: LHCB_BKKDBManager.py,v 1.91 2009/05/04 20:17:33 zmathe Exp $
########################################################################

"""
LHCb Bookkeeping database manager
"""

from DIRAC                                                               import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Client.BaseESManager                        import BaseESManager
from DIRAC.BookkeepingSystem.Client.BookkeepingClient                    import BookkeepingClient
from DIRAC.BookkeepingSystem.Client                                      import objects
from DIRAC.BookkeepingSystem.Client.Help                                 import Help
from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
import os
import types
import sys

__RCSID__ = "$Id: LHCB_BKKDBManager.py,v 1.91 2009/05/04 20:17:33 zmathe Exp $"

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
  LHCB_BKDB_PREFIXES_CONFIG =     ['CFGN',  # configname
                                   'CFGV',  #configversion
                                   'SCON',
                                   'PAS',
                                   'EVT',    # event type
                                   'PROD',   #production 
                                   'FTY',    #file type 
                                   'PRO',    #program name and version
                                    '',    # filename                                 
                                   ]
  
  LHCB_BKDB_PREFIXES_PRODUCTION = ['PROD',
                                   'EVT',
                                   'FTY',
                                   ''
                                  ]
  
  LHCB_BKDB_PREFIXES_EVENTTYPE = ['CFGN',
                                  'CFGV',
                                  'EVT',
                                  'SCON',
                                  'PAS',
                                  'PROD',
                                  'FTY',
                                  'PRO',
                                   '',
                                  ]
  LHCB_BKDB_PREFIXES=[]

  LHCB_BKDB_PARAMETERS = ['Configuration', 'Event type' ,'Productions' ]
    
  LHCB_BKDB_PREFIX_SEPARATOR = "_"
  
  ############################################################################# 
  def __init__(self, rpcClinet = None):
    super(LHCB_BKKDBManager, self).__init__()
    self._BaseESManager___fileSeparator = INTERNAL_PATH_SEPARATOR    
    #self.__pathSeparator = INTERNAL_PATH_SEPARATOR
    self.db_ = BookkeepingClient(rpcClinet)
    self.lfc_ = LcgFileCatalogCombinedClient()
    
    self.helper_ = Help()
    
    self.__entityCache = {'/':(objects.Entity({'name':'/', 'fullpath':'/','expandable':True}), 0)} 
    self.parameter_ = self.LHCB_BKDB_PARAMETERS[0]
    self.LHCB_BKDB_PREFIXES = self.LHCB_BKDB_PREFIXES_CONFIG
    self.files_ = []

    self.treeLevels_ = -1
    self.advancedQuery_ = False
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
   
    else:
      gLogger.error("Wrong Parameter!")
  
  #############################################################################
  def getLogicalFiles(self):
    return self.files_ 
  
  #############################################################################
  def getFilesPFN(self):
    lfns = self.files_
    res = self.lfc_.getReplicas(lfns)
    return res
  
  ############################################################################# 
  def list(self, path="/"):
    if self.parameter_ == self.LHCB_BKDB_PARAMETERS[0]:
      return self._listConfigs(path) 
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[1]:
      return self._listEventTypes(path)
    elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[2]:
      return self._listProduction(path)
  
  ############################################################################# 
  def getLevel(self, path):
    path = self.getAbsolutePath(path)['Value'] # shall we do this here or in the _processedPath()?
    valid, processedPath = self._processPath(path)
   
    if not valid:
      gLogger.error(path + " is not valid!");
      raise ValueError, "Invalid path '%s'" % path
        # get directory content
    levels = len(processedPath)
    self._updateTreeLevels(levels)
    return levels, processedPath
  
    
  ############################################################################# 
  def _listConfigs(self, path):
    entityList = list()
    levels, processedPath = self.getLevel(path)
  
    if levels == 0:
       self.clevelHeader_0(path, levels, processedPath)
       entityList += self.clevelBody_0(path, levels,)
    if levels == 1: 
      configName = self.clevelHeader_1(path, levels, processedPath) 
      entityList += self.clevelBody_1(path, levels, configName)  
    if levels == 2: 
      configName, configVersion = self.clevelHeader_2(path, levels, processedPath) 
      entityList += self.clevelBody_2(path, levels, configName, configVersion)
    if levels == 3: 
      configName, configVersion, simid = self.clevelHeader_3(path, levels, processedPath) 
      entityList += self.clevelBody_3(path, levels, configName, configVersion, simid)
    if levels == 4: 
      configName, configVersion, simid, processing = self.clevelHeader_4(path, levels, processedPath) 
      entityList += self.clevelBody_4(path, levels, configName, configVersion, simid, processing)
    
    if levels == 5 and self.advancedQuery_: 
      configName, configVersion, simid, processing, evtType = self.clevelHeader_5(path, levels, processedPath) 
      entityList += self.clevelBody_5(path, levels, configName, configVersion, simid, processing, evtType)
    elif levels == 5 and not self.advancedQuery_:
      newPath = self.__createPath(processedPath,(self.LHCB_BKDB_PREFIXES_CONFIG[5],'ALL'))
      path = newPath
      v,processedPath = self._processPath(path)
      levels = 6

    if levels == 6: 
      configName, configVersion, simid, processing, evtType, prod = self.clevelHeader_6(path, levels, processedPath) 
      entityList += self.clevelBody_6(path, levels, configName, configVersion, simid, processing, evtType, prod)

    if levels == 7 and self.advancedQuery_: 
      configName, configVersion, simid, processing, evtType, prod, ftype = self.clevelHeader_7(path, levels, processedPath) 
      entityList += self.clevelBody_7(path, levels, configName, configVersion, simid, processing, evtType, prod, ftype)
    elif levels == 7 and not self.advancedQuery_:
      newPath = self.__createPath(processedPath,(self.LHCB_BKDB_PREFIXES_CONFIG[7],'ALL'))
      path = newPath
      v,processedPath = self._processPath(path)
      levels = 8

    if levels == 8: 
      configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion = self.clevelHeader_8(path, levels, processedPath) 
      entityList += self.clevelBody_8(path, levels, configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion)

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
      for record in dbResult:
        configs = record[0]
        entityList += [self._getEntityFromPath(path, configs, levels)]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def clevelHeader_1(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("listing configversions")
    configName = processedPath[0][1]  
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name      | %s " % (configName))
    
    gLogger.debug("Available Config Versions:")
    return configName
  
  def clevelBody_1(self, path, levels, configName):
    entityList = list()
    result = self.db_.getConfigVersions(configName)
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        configs = record[0]
        entityList += [self._getEntityFromPath(path, configs, levels,'Configuration versions')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
  
  
  ############################################################################# 
  def clevelHeader_2(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("listing Simulation Conditions!")
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
          
    gLogger.debug( "-----------------------------------------------------------")
    gLogger.debug("Selected parameters:" )                             
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name      | %s " % (configName) )
    gLogger.debug("Configuration Version   | %s " % (configVersion))
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Available Simulation Conditions:")
    return configName, configVersion
  
  ############################################################################# 
  def clevelBody_2(self, path, levels, configName, configVersion):
    entityList = list()
    if configName=='MC':
      result = self.db_.getSimulationConditions(configName, configVersion) 
      if result['OK']:
        dbResult = result['Value']
        if len(dbResult) > 1:
          add = self.__addAll(path, levels, 'Simulation Conditions/DataTaking')
          if add:
            entityList += [add]
        for record in dbResult:
          simid = str(record[0])
          description = record[1]
          value = {'SimulationCondition':simid, 'Simulation Description':description, 'BeamCond':record[2],'BeamEnergy':record[3],'Generator':record[4],'MagneticFileld':record[5],'DetectorCond':record[6],'Luminosity':record[7]}
          entityList += [self._getSpecificEntityFromPath(path, value, simid, levels, description, 'Simulation Conditions/DataTaking')]
        self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
    else:
      result = self.db_.getSimulationConditions(configName, configVersion, 1) 
      if result['OK']:
        dbResult = result['Value']
        if len(dbResult) > 1:
          add = self.__addAll(path, levels, 'Simulation Conditions/DataTaking')
          if add:
            entityList += [add]
        for record in dbResult:
          simid = str(record[0])
          description = record[1]
          value = {'BEAMCOND':record[2],'BEAMENERGY':record[3],'MAGNETICFIELD':record[4],'VELO':record[5],'IT':record[6],'TT':record[7],'OT':record[8],'RICH1':record[9],'RICH2':record[10],'SPD_PRS':record[11],'ECAL':record[12],'HCAL':record[13],'MUON':record[14],'L0':record[15],'HLT':record[16]}
          entityList += [self._getSpecificEntityFromPath(path, value, simid, levels, description, 'Simulation Conditions/DataTaking')]
        self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
    return entityList
  

  ############################################################################# 
  def clevelHeader_3(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("listing processing pass")
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    simid = processedPath[2][1]

    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name     | %s " % (configName))
    gLogger.debug("Configuration Version  | %s " % (configVersion))
    gLogger.debug("Simulation condition   | %s " % (str(simid)) )
    gLogger.debug( "-----------------------------------------------------------")

    gLogger.debug("Available processing pass:\n")
    return configName, configVersion, simid
  
  ############################################################################# 
  def clevelBody_3(self, path, levels, configName, configVersion, simid):
    entityList = list()
    result = self.db_.getProPassWithSimCondNew(configName, configVersion, simid)
    if result['OK']:
      dbResult = result['Value']
      if len(dbResult) > 1:
        add = self.__addAll(path, levels, 'Processing Pass')
        if add:
          entityList += [add]
      for record in dbResult:
        prod = str(record[1])
        value = {'passid':record[0],'Total ProcessingPass':prod}
  
        entityList += [self._getSpecificEntityFromPath(path, value, prod, levels, None,  'Processing Pass')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def clevelHeader_4(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("listing event types")
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    simid = processedPath[2][1]
    processing = processedPath[3][1]
 
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters: ")
    gLogger.debug("-----------------------------------------------------------")

    gLogger.debug("Configuration Name      | %s " % (configName))
    gLogger.debug("Configuration Version   | %s " % (configVersion))
    gLogger.debug("Simulation Condition    | %s " % (str(simid)) )
    gLogger.debug("Processing Pass         | %s " % (str(processing)) )
    gLogger.debug("-----------------------------------------------------------")

    gLogger.debug("Available event types types:")
    return configName, configVersion, simid, processing
  
  
  ############################################################################# 
  def clevelBody_4(self, path, levels, configName, configVersion, simid, processing):
    entityList = list()
    result = self.db_.getEventTypeWithSimcond(configName, configVersion, simid, processing)
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        evtType = str(record[0])
        value = {'Event Type':evtType,'Description':record[1]}
        entityList += [self._getSpecificEntityFromPath(path, value, evtType, levels,None, 'Event types')]
      self._cacheIt(entityList)
    else:
        gLogger.error(result['Message'])
    return entityList    
    
  ############################################################################# 
  def clevelHeader_5(self, path, levels, processedPath):
    entityList = list()
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    simid = processedPath[2][1]
    processing = processedPath[3][1]
    evtType = processedPath[4][1]
    
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name     | %s " % (configName) )
    gLogger.debug("Configuration Version  | %s " % (configVersion) )
    gLogger.debug("Simulation Condition   | %s " % (str(simid)) )
    gLogger.debug("Processing Pass        | %s " % (str(processing)) )
    gLogger.debug("Event type             | %s " % (str(evtType)))
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Available Production(s):")
    return configName, configVersion, simid, processing, evtType
  
  ############################################################################# 
  def clevelBody_5(self, path, levels, configName, configVersion, simid, processing, evtType):
    entityList = list()
    result = self.db_.getProductionsWithSimcond(configName, configVersion, simid, processing, evtType)
    if result['OK']:
      dbResult = result['Value']
      if len(dbResult) > 1:
        add = self.__addAll(path, levels, 'Production(s)/Run(s)')
        if add:
          entityList += [add] 
      for record in dbResult:
        prod = str(record[0])
        entityList += [self._getEntityFromPath(path, prod, levels, 'Production(s)/Run(s)')]
      self._cacheIt(entityList)        
    else:
      gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def clevelHeader_6(self, path, levels, processedPath):
    entityList = list()
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    simid = processedPath[2][1]
    processing = processedPath[3][1]
    evtType = processedPath[4][1]
    prod = processedPath[5][1]
  
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name     | %s " % (configName))
    gLogger.debug("Configuration Version  | %s " % (configVersion))
    gLogger.debug("Simulation Condition   | %s " % (str(simid)))
    gLogger.debug("Processing Pass        | %s " % (str(processing)))
    gLogger.debug("Event type             | %s " % (str(evtType)))
    gLogger.debug("Production             | %s"  % (str(processedPath[4][1])))
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Available file types:")
    return configName, configVersion, simid, processing, evtType, prod
  
  ############################################################################# 
  def clevelBody_6(self, path, levels, configName, configVersion, simid, processing, evtType, prod):
    entityList = list()
    result = self.db_.getFileTypesWithSimcond(configName, configVersion, simid, processing, evtType, prod)
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
  def clevelHeader_7(self, path, levels, processedPath):
    entityList = list()
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    simid = processedPath[2][1]
    processing = processedPath[3][1]
    evtType = processedPath[4][1]
    prod = processedPath[5][1]
    ftype = processedPath[6][1]  
    
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name     | %s " %(configName))
    gLogger.debug("Configuration Version  | %s " %(configVersion))
    gLogger.debug("Simulation Condition   | %s " %(str(simid)))
    gLogger.debug("Processing Pass        | %s " % (str(processing)))
    gLogger.debug("Event type             | %s " % (str(evtType)))
    gLogger.debug("Production             | %s " % (str(processedPath[4][1])))
    gLogger.debug("File Type              | %s " % (str(ftype)))
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Available program name and version:")
    return configName, configVersion, simid, processing, evtType, prod, ftype
  
  ############################################################################# 
  def clevelBody_7(self, path, levels, configName, configVersion, simid, processing, evtType, prod, ftype):
    entityList = list()
    result = self.db_.getProgramNameWithSimcond(configName, configVersion, simid, processing, evtType, prod, ftype)
    if result['OK']:
      dbResult = result['Value']
      if len(dbResult) > 1:
        add = self.__addAll(path, levels, 'Program name and version')
        if add:
          entityList += [add] 
      
      for record in dbResult:
        programName = record[0]
        programVersion = record[1]
        program = programName+' '+programVersion
        entityList += [self._getEntityFromPath(path, program, levels, 'Program name and version')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
 
  ############################################################################# 
  def clevelHeader_8(self, path, levels, processedPath):
    entityList = list()
    self.files_ = []
    gLogger.debug("listing files")
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    simid = processedPath[2][1]
    processing = processedPath[3][1]
    evtType = processedPath[4][1]
    prod = processedPath[5][1]
    ftype = processedPath[6][1]
    if processedPath[7][1] != 'ALL':
      pname = processedPath[7][1].split(' ')[0]
      pversion = processedPath[7][1].split(' ')[1]
    else:
      pname = processedPath[7][1]
      pversion = processedPath[7][1]

    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name     | %s " %(configName))
    gLogger.debug("Configuration Version  | %s " %(configVersion))
    gLogger.debug("Simulation Condition   | %s " %(str(simid)))
    gLogger.debug("Processing Pass        | %s " % (str(processing)))
    gLogger.debug("Event type             | %s " %(str(evtType)))
    gLogger.debug("Production             | %s " %(str(processedPath[4][1])))
    gLogger.debug("File Type              | %s " %(str(ftype)))
    gLogger.debug("Program name           | %s " %(pname))
    gLogger.debug("Program version        | %s " %(pversion))
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("File list:\n")
    return configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion
  
  def clevelBody_8(self, path, levels, configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion):
    entityList = list()
    result = self.db_.getFilesWithSimcond(configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion)
    selection = {"Configuration Name":configName, \
                 "Configuration Version":configVersion, \
                 "Simulation Condition":str(simid), \
                 "Processing Pass":str(processing), \
                 "Event type":str(evtType), \
                 "Production":str(prod), \
                 "File Type":str(ftype), \
                 "Program name":pname, \
                 "Program version":pversion}
    
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        value = {'name':record[0],'EventStat':record[1], 'FileSize':str(record[2]),'CreationDate':record[3],'Generator':record[4],'GeometryVersion':record[5],       'JobStart':record[6], 'JobEnd':record[7],'WorkerNode':record[8],'FileType':record[9],'RunNumber':record[10],'FillNumber':record[11],'PhysicStat':record[12], 'DataQuality':record[13],'EvtTypeId':evtType,'Selection':selection}
        self.files_ += [record[0]]
        entityList += [self._getEntityFromPath(path, value, levels,'List of files')]
      self._cacheIt(entityList)    
    else:
      gLogger.error(result['Message'])
    
    return entityList



  
  

  ############################################################################# 
  def _listEventTypes(self, path):
    entityList = list()
    levels, processedPath = self.getLevel(path)
  
    if levels == 0:
       self.clevelHeader_0(path, levels, processedPath)
       entityList += self.clevelBody_0(path, levels,)
    if levels == 1: 
      configName = self.clevelHeader_1(path, levels, processedPath) 
      entityList += self.clevelBody_1(path, levels, configName)  

    if levels == 2: 
      configName, configVersion = self.clevelHeader_2(path, levels, processedPath) 
      entityList += self.elevelBody_2(path, levels, configName, configVersion)
   
    if levels == 3: 
      configName, configVersion, eventType = self.elevelHeader_3(path, levels, processedPath) 
      entityList += self.elevelBody_3(path, levels, configName, configVersion, eventType)
    
    if levels == 4: 
      configName, configVersion, eventtype, simcond = self.elevelHeader_4(path, levels, processedPath) 
      entityList += self.elevelBody_4(path, levels, configName, configVersion, eventtype, simcond)
   
    if levels == 5 and self.advancedQuery_: 
      configName, configVersion, simid, processing, evtType = self.elevelHedaer_5(path, levels, processedPath) 
      entityList += self.clevelBody_5(path, levels, configName, configVersion, simid, processing, evtType)
    elif levels == 5 and not self.advancedQuery_:
      newPath = self.__createPath(processedPath,(self.LHCB_BKDB_PREFIXES_EVENTTYPE[5],'ALL'))
      path = newPath
      v,processedPath = self._processPath(path)
      levels = 6
    

    if levels == 6: 
      configName, configVersion, simid, processing, evtType, prod = self.elevelHeader_6(path, levels, processedPath) 
      entityList += self.clevelBody_6(path, levels, configName, configVersion, simid, processing, evtType, prod)
   
    if levels == 7 and self.advancedQuery_: 
      configName, configVersion, simid, processing, evtType, prod, ftype = self.elevelHeader_7(path, levels, processedPath) 
      entityList += self.clevelBody_7(path, levels, configName, configVersion, simid, processing, evtType, prod, ftype)
    elif levels == 7 and not self.advancedQuery_:
      newPath = self.__createPath(processedPath,(self.LHCB_BKDB_PREFIXES_EVENTTYPE[7],'ALL'))
      path = newPath
      v,processedPath = self._processPath(path)
      levels = 8
      
    if levels == 8: 
      configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion = self.elevelHeader_8(path, levels, processedPath) 
      entityList += self.clevelBody_8(path, levels, configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion)
    return entityList
  
   ############################################################################# 
  def elevelBody_2(self, path, levels, configName, configVersion):      
    entityList = list()
    result = self.db_.getEventTypes(configName, configVersion)
    if result['OK']:
      dbResult = result['Value']
      if len(dbResult) > 1:
        add = self.__addAll(path, levels, 'Event types')
        if add:
          entityList += [add]        
      for record in dbResult:
        event = str(record[0])
        value = {'Event Type':record[0],'Description':record[1]}
        entityList += [self._getSpecificEntityFromPath(path, value, event, levels, None, 'Event types')]
      self._cacheIt(entityList)      
    else:
      gLogger.error(result['Message'])
    return entityList

  ############################################################################# 
  def elevelHeader_3(self, path, levels, processedPath):
    gLogger.debug("listing simulation conditions")
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    eventType = processedPath[2][1]
    
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name      | %s " %(configName))
    gLogger.debug("Configuration Version   | %s " %(configVersion))
    gLogger.debug("Event Type              | %s " %(str(eventType)))
    gLogger.debug("-----------------------------------------------------------")
    
    gLogger.debug("Available simulation conditions:")
    return configName, configVersion, eventType
  
  ############################################################################# 
  def elevelBody_3(self,path, levels, configName, configVersion, eventType):
    entityList = list()
    if configName=='MC':
      result = self.db_.getSimCondWithEventType(configName, configVersion, eventType)
      if result['OK']:
        dbResult = result['Value']
        if len(dbResult) > 1:
          add = self.__addAll(path, levels, 'Simulation Conditions/DataTaking')
          if add:
            entityList += [add]       
        for record in dbResult:
          simid = str(record[0])
          description = record[1]
          value = {'SimulationCondition':simid, 'Simulation Description':description, 'BeamCond':record[2],'BeamEnergy':record[3],'Generator':record[4],'MagneticFileld':record[5],'DetectorCond':record[6],'Luminosity':record[7]}
          entityList += [self._getSpecificEntityFromPath(path, value, simid, levels, description, 'Simulation Conditions/DataTaking')]
        self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
    else:
      result = self.db_.getSimulationConditions(configName, configVersion, 1) 
      if result['OK']:
        dbResult = result['Value']
        if len(dbResult) > 1:
          add = self.__addAll(path, levels, 'Simulation Conditions/DataTaking')
          if add:
            entityList += [add]       
        for record in dbResult:
          simid = str(record[0])
          description = record[1]
          value = {'BEAMCOND':record[2],'BEAMENERGY':record[3],'MAGNETICFIELD':record[4],'VELO':record[5],'IT':record[6],'TT':record[7],'OT':record[8],'RICH1':record[9],'RICH2':record[10],'SPD_PRS':record[11],'ECAL':record[12]}
          entityList += [self._getSpecificEntityFromPath(path, value, simid, levels, description, 'Simulation Conditions/DataTaking')]
        self._cacheIt(entityList)
      else:
        gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def elevelHeader_4(self, path, levels, processedPath):
    gLogger.debug("listing processing pass")
    configName = processedPath[0][1] 
    configVersion = processedPath[1][1]
    eventtype = processedPath[2][1]
    simcond = processedPath[3][1]
 
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters: ")
    gLogger.debug("-----------------------------------------------------------")

    gLogger.debug("Configuration Name      | %s " %(configName))
    gLogger.debug("Configuration Version   | %s " %(configVersion))
    gLogger.debug("Event type              | %s " %(str(eventtype)))
    gLogger.debug("Simulation Condition    | %s " %(str(simcond)))
    gLogger.debug("-----------------------------------------------------------")

    gLogger.debug("Available processing pass types:")
    return configName, configVersion, eventtype, simcond
  
  ############################################################################# 
  def elevelBody_4(self, path, levels, configName, configVersion, eventtype, simcond):
    entityList = list()
    result = self.db_.getProPassWithEventTypeNew(configName, configVersion, eventtype, simcond)
    if result['OK']:
      dbResult = result['Value']
      if len(dbResult) > 1:
        add = self.__addAll(path, levels, 'Processing Pass')
        if add:
          entityList += [add]        
      for record in dbResult:
        proc = str(record[1])
        value = {'passid':record[0],'Total ProcessingPass':proc}
        entityList += [self._getSpecificEntityFromPath(path, value, proc, levels, None, 'Processing Pass')]
      self._cacheIt(entityList)
    else:
        gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def elevelHedaer_5(self, path, levels, processedPath):
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    evtType = processedPath[2][1]
    simid = processedPath[3][1]
    processing = processedPath[4][1]
    
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name     | %s " %(configName))
    gLogger.debug("Configuration Version  | %s " %(configVersion))
    gLogger.debug("Simulation Condition   | %s " %(str(simid)))
    gLogger.debug("Processing Pass        | %s " %(str(processing)))
    gLogger.debug("Event type             | %s " %(str(evtType)))
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Available Production(s):")
    return configName, configVersion, simid, processing, evtType
  
  ############################################################################# 
  def elevelHeader_6(self, path, levels, processedPath):
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    evtType = processedPath[2][1]
    simid = processedPath[3][1]
    processing = processedPath[4][1] 
    prod = processedPath[5][1]
            
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name     | %s " %(configName))
    gLogger.debug("Configuration Version  | %s " %(configVersion))
    gLogger.debug("Simulation Condition   | %s " %(str(simid)))
    gLogger.debug("Processing Pass        | %s " %(str(processing)))
    gLogger.debug("Event type             | %s " %(str(evtType)))
    gLogger.debug("Production             | %s " %(str(prod)))
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Available file types:")
    return configName, configVersion, simid, processing, evtType, prod
  
  #############################################################################       
  def elevelHeader_7(self, path, levels, processedPath):
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    evtType = processedPath[2][1]
    simid = processedPath[3][1]
    processing = processedPath[4][1] 
    prod = processedPath[5][1]
    ftype = processedPath[6][1]  
    
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name     | %s " %(configName))
    gLogger.debug("Configuration Version  | %s " %(configVersion))
    gLogger.debug("Simulation Condition   | %s " %(str(simid)))
    gLogger.debug("Processing Pass        | %s " %(str(processing)))
    gLogger.debug("Event type             | %s " %(str(evtType)))
    gLogger.debug("Production             | %s " %(str(prod)))
    gLogger.debug("File Type              | %s " %(str(ftype)))
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Available program name and version:")
    return configName, configVersion, simid, processing, evtType, prod, ftype
      
  
  ############################################################################# 
  def elevelHeader_8(self, path, levels, processedPath):
  
    self.files_ = []
    gLogger.debug("listing files")
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    evtType = processedPath[2][1]
    simid = processedPath[3][1]
    processing = processedPath[4][1] 
    prod = processedPath[5][1]
    ftype = processedPath[6][1]
    if len(processedPath[7][1]) < 8:
      pname = 'ALL'
      pversion = 'ALL'
    else:
      pname = processedPath[7][1].split(' ')[0]
      pversion = processedPath[7][1].split(' ')[1]

    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name     | %s " %(configName))
    gLogger.debug("Configuration Version  | %s " %(configVersion))
    gLogger.debug("Simulation Condition   | %s " %(str(simid)))
    gLogger.debug("Processing Pass        | %s " %(str(processing)))
    gLogger.debug("Event type             | %s " %(str(evtType)))
    gLogger.debug("Production             | %s " %(str(prod)))
    gLogger.debug("File Type              | %s " %(str(ftype)))
    gLogger.debug("Program name           | %s " %(pname))
    gLogger.debug("Program version        | %s " %(pversion))
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("File list:\n")
    
    return configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion
  
  ############################################################################# 
  def _listProduction(self, path):    
  
    entityList = list()
    levels, processedPath = self.getLevel(path)
    
    if levels == 0:
       self.plevelHeader_0(path, levels, processedPath)
       entityList += self.plevelBody_0(path, levels, None)
       
    if levels == 1: 
      production = self.plevelHeader_2(path, levels, processedPath) 
      entityList += self.plevelBody_2(path, levels, production)
    
    if levels == 2:
      production, evt = self.plevelHeader_3(path, levels, processedPath) 
      entityList += self.plevelBody_3(path, levels, production, evt)
    
    if levels == 3:
      production, evt, ftype = self.plevelHeader_4(path, levels, processedPath) 
      entityList += self.plevelBody_4(path, levels, production, evt, ftype)
    
    return entityList
  
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
    production = processedPath[0][1]  
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Production        | %s " % (production))
    
    gLogger.debug("Available event types:")
    return production
  
  ############################################################################# 
  def plevelBody_2(self, path, levels, production):
    entityList = list()
    result = self.db_.getEventTyesWithProduction(production)
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        event = str(record[0])
        value = {'Event Type':record[0],'Description':record[1]}
        entityList += [self._getSpecificEntityFromPath(path, value, event, levels, None, 'Event types')]
      self._cacheIt(entityList)
    else:
      gLogger.error(result['Message'])
    return entityList
  
  ############################################################################# 
  def plevelHeader_3(self, path, levels, processedPath):
    entityList = list()
    gLogger.debug("listing file types")
    production = processedPath[0][1] 
    eventtype =  processedPath[1][1] 
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Production        | %s " % (production))
    gLogger.debug("Event type        | %s " % (eventtype))
    
    gLogger.debug("Available file types:")
    return production, eventtype
  
  ############################################################################# 
  def plevelBody_3(self, path, levels, production, evt):
    entityList = list()
    result = self.db_.getFileTypesWithSimcond('ALL', 'ALL', 'ALL', 'ALL', evt, production)
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
  def plevelHeader_4(self, path, levels, processedPath): 
    entityList = list()
    gLogger.debug("listing file types")
    production = processedPath[0][1] 
    eventtype =  processedPath[1][1] 
    ftype = processedPath[2][1] 
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Production        | %s " % (production))
    gLogger.debug("Event type        | %s " % (eventtype))
    gLogger.debug("File type         | %s " % (ftype))
    
    gLogger.debug("Available files:")
    return production, eventtype, ftype
  
  ############################################################################# 
  def plevelBody_4(self, path, levels, production, evt, ftype):
    entityList = list()
    result = self.db_.getFilesWithSimcond('ALL', 'ALL', 'ALL', 'ALL', evt, production, ftype, 'ALL', 'ALL')
    selection = {"Configuration Name":'ALL', \
                 "Configuration Version":'ALL', \
                 "Simulation Condition":'ALL', \
                 "Processing Pass":'ALL', \
                 "Event type":str(evt), \
                 "Production":str(production), \
                 "File Type":str(ftype), \
                 "Program name":'ALL', \
                 "Program version":'ALL'}
    
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        value = {'name':record[0],'EventStat':record[1], 'FileSize':str(record[2]),'CreationDate':record[3],'Generator':record[4],'GeometryVersion':record[5],       'JobStart':record[6], 'JobEnd':record[7],'WorkerNode':record[8],'FileType':record[9],'RunNumber':record[10],'FillNumber':record[11],'PhysicStat':record[12], 'DataQuality':record[13],'EvtTypeId':evt,'Selection':selection}
        self.files_ += [record[0]]
        entityList += [self._getEntityFromPath(path, value, levels,'List of files')]
      self._cacheIt(entityList)    
    else:
      gLogger.error(result['Message'])
    
    return entityList
  
  ############################################################################# 
  def _getEntityFromPath(self, presentPath, newPathElement, level, leveldescription=None):
     
    if isinstance(newPathElement, types.DictType):
      # this must be a file
      entity = objects.Entity(newPathElement)
      newPathElement = str(entity['name']).rsplit("/", 1)[1]
      entity.update({'FileName':entity['name']})
      expandable = False
      entity.update({'expandable':expandable})
      type = self.LHCB_BKDB_FILE_TYPE                            
    else:
      # this must be a folder
      entity = objects.Entity()
      name = newPathElement
      newPathElement = self.LHCB_BKDB_PREFIXES[level]+ \
      self.LHCB_BKDB_PREFIX_SEPARATOR + \
      newPathElement
      expandable = True
      type = self.LHCB_BKDB_FOLDER_TYPE                            
                            
      fullPath  = presentPath.rstrip(INTERNAL_PATH_SEPARATOR)
      fullPath += INTERNAL_PATH_SEPARATOR + \
      newPathElement

      entity.update({'name':name, 'fullpath':fullPath,'expandable':expandable})
      
      if leveldescription <> None:
        entity.update({'level':leveldescription})
      
      if not self.advancedQuery_ and level==6:
        entity.update({'showFiles':0})
      elif  self.advancedQuery_ and level==7:
        entity.update({'showFiles':0})
      elif self.parameter_ == self.LHCB_BKDB_PARAMETERS[2] and level == 2:
        entity.update({'showFiles':0})
    return entity
  
  ############################################################################# 
  def _getSpecificEntityFromPath(self, presentPath, value, newPathElement, level, description=None, leveldescription=None):
    if isinstance(value, types.DictType):
      entity = objects.Entity(value)
      type = self.LHCB_BKDB_FILE_TYPE
      name = newPathElement
      newPathElement = self.LHCB_BKDB_PREFIXES[level]+ \
      self.LHCB_BKDB_PREFIX_SEPARATOR + \
      newPathElement
      
      expandable = True
      
      fullPath  = presentPath.rstrip(INTERNAL_PATH_SEPARATOR)
      fullPath += INTERNAL_PATH_SEPARATOR + \
      newPathElement
      if description<>None:
        entity.update({'name':description, 'fullpath':fullPath, 'expandable':expandable})  
      else:
        entity.update({'name':name, 'fullpath':fullPath, 'expandable':expandable})  
      if leveldescription <> None:
        entity.update({'level':leveldescription})
    
      if not self.advancedQuery_ and level==6:
        entity.update({'showFiles':0})
      elif  self.advancedQuery_ and level==7:
        entity.update({'showFiles':0})  
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
        value = {'DiracJobID':record[0], 'DiracVersion':record[1], 'EventInputStat':record[2], 'ExecTime':record[3], 'FirstEventNumber':record[4], 'Generator':record[5], \
                 'GeometryVersion':record[6], 'GridJobID':record[7], 'LocalJobID':record[8], 'Location':record[9], 'Name':record[10], 'NumberofEvents':record[11], \
                  'StatistivsRequested':record[12], 'WNCPUPOWER':record[13], 'WNCPUTIME':record[14], 'WNCACHE':record[15], 'WNMEMORY':record[16], 'WNMODEL':record[17], 'WORKERNODE':record[18]}  
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
  #############################################################################       
  def _getLimitedFilesConfigParams(self, SelectionDict, SortDict, StartItem, Maxitems):
    entityList = list()
    path = SelectionDict['fullpath'] 
    path = self.getAbsolutePath(path)['Value'] # shall we do this here or in the _processedPath()?
    valid, processedPath = self._processPath(path)
   
    if not valid:
      gLogger.error(path + " is not valid!");
      raise ValueError, "Invalid path '%s'" % path
        # get directory content
    levels = len(processedPath)
    self._updateTreeLevels(levels)
    #if levels == 7:
    self.files_ = []
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

    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name     | %s " %(configName))
    gLogger.debug("Configuration Version  | %s " %(configVersion))
    gLogger.debug("Simulation Condition   | %s " %(str(simid)))
    gLogger.debug("Processing Pass        | %s " %(str(processing)))
    gLogger.debug("Event type             | %s " %(str(evtType)))
    gLogger.debug("Production             | %s " %(str(prod)))
    gLogger.debug("File Type              | %s " %(str(ftype)))
    gLogger.debug("Program name           | %s " %(pname))
    gLogger.debug("Program version        | %s " %(pversion))
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("File list:\n")
    
    selection = {"Configuration Name":configName, \
                 "Configuration Version":configVersion, \
                 "Simulation Condition":str(simid), \
                 "Processing Pass":str(processing), \
                 "Event type":str(evtType), \
                 "Production":str(prod), \
                 "File Type":str(ftype), \
                 "Program name":pname, \
                 "Program version":pversion}
  
    return self.__getFiles(configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion, SortDict, StartItem, Maxitems, selection)
    
    
  
  #############################################################################       
  def _getLimitedFilesEventTypeParams(self, SelectionDict, SortDict, StartItem, Maxitems):
    entityList = list()
    path = SelectionDict['fullpath'] 
    path = self.getAbsolutePath(path)['Value'] # shall we do this here or in the _processedPath()?
    valid, processedPath = self._processPath(path)
   
    if not valid:
      gLogger.error(path + " is not valid!");
      raise ValueError, "Invalid path '%s'" % path
        # get directory content
    levels = len(processedPath)
    self._updateTreeLevels(levels)
      
    #if levels == 7:    
    self.files_ = []
    gLogger.debug("listing files")
    configName = processedPath[0][1]
    configVersion = processedPath[1][1]
    evtType = processedPath[2][1]
    simid = processedPath[3][1]
    processing = processedPath[4][1] 
    prod = processedPath[5][1]
    ftype = processedPath[6][1]
    if len(processedPath) < 8:
      pname = 'ALL'
      pversion = 'ALL'
    else:
      pname = processedPath[7][1].split(' ')[0]
      pversion = processedPath[7][1].split(' ')[1]

    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Selected parameters:   ")
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("Configuration Name     | %s " %(configName))
    gLogger.debug("Configuration Version  | %s " %(configVersion))
    gLogger.debug("Simulation Condition   | %s " %(str(simid)))
    gLogger.debug("Processing Pass        | %s " %(str(processing)))
    gLogger.debug("Event type             | %s " %(str(evtType)))
    gLogger.debug("Production             | %s " %(str(prod)))
    gLogger.debug("File Type              | %s " %(str(ftype)))
    gLogger.debug("Program name           | %s " %(pname))
    gLogger.debug("Program version        | %s " %(pversion))
    gLogger.debug("-----------------------------------------------------------")
    gLogger.debug("File list:\n")
    
    selection = {"Configuration Name":configName, \
                 "Configuration Version":configVersion, \
                 "Simulation Condition":str(simid), \
                 "Processing Pass":str(processing), \
                 "Event type":str(evtType), \
                 "Production":str(prod), \
                 "File Type":str(ftype), \
                 "Program name":pname, \
                 "Program version":pversion}
          
    return self.__getFiles(configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion, SortDict, StartItem, Maxitems, selection)
  
  def _getLimitedFilesProductions(self, SelectionDict, SortDict, StartItem, Maxitems):
    entityList = list()
    path = SelectionDict['fullpath'] 
    path = self.getAbsolutePath(path)['Value'] # shall we do this here or in the _processedPath()?
    valid, processedPath = self._processPath(path)
   
    if not valid:
      gLogger.error(path + " is not valid!");
      raise ValueError, "Invalid path '%s'" % path
        # get directory content
    levels = len(processedPath)
    self._updateTreeLevels(levels)
      
    #if levels == 7:    
    self.files_ = []
    gLogger.debug("listing files")
    prod = processedPath[0][1]
    evtType = processedPath[1][1]
    ftype = processedPath[2][1]
        
    selection = {"Configuration Name":'ALL', \
                 "Configuration Version":'ALL', \
                 "Simulation Condition":'ALL', \
                 "Processing Pass":'ALL', \
                 "Event type":str(evtType), \
                 "Production":str(prod), \
                 "File Type":str(ftype), \
                 "Program name":'ALL', \
                 "Program version":'ALL'}
          
    return self.__getFiles('ALL', 'ALL', 'ALL', 'ALL', evtType, prod, ftype, 'ALL', 'ALL', SortDict, StartItem, Maxitems, selection)
  
  #############################################################################       
  def __getFiles(self,configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion, SortDict, StartItem, Maxitems, selection):
    totalrecords = 0
    nbOfEvents = 0
    filesSize = 0
    if len(SortDict) > 0:
      res = self.db_.getLimitedNbOfFiles(configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion)
      if not res['OK']:
        gLogger.error(res['Message'])
      else:
        totalrecords = res['Value'][0][0]
        nbOfEvents = res['Value'][0][1]
        filesSize = res['Value'][0][2]
    records = []
    parametersNames=[]
    if StartItem > -1 and Maxitems != 0:
      result = self.db_.getLimitedFilesWithSimcond(configName, configVersion, simid, processing, evtType, prod, ftype, pname, pversion, StartItem, Maxitems)
    
      
      parametersNames = ['Name','EventStat', 'FileSize','CreationDate','Generator','GeometryVersion','JobStart', 'JobEnd','WorkerNode','FileType', 'EvtTypeId','RunNumber','FillNumber','PhysicStat']
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult:
          value = [record[1],record[2],record[3],str(record[4]),record[5],record[6],str(record[7]),str(record[8]),record[9],record[10], evtType, record[11],record[12],record[13]]
          records += [value]
      else:
        gLogger.error(result['Message'])
    
    return {'TotalRecords':totalrecords,'ParameterNames':parametersNames,'Records':records,'Extras': {'Selection':selection, 'GlobalStatistics':{'Number of Events':nbOfEvents, 'Files Size':filesSize }} } 
  
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
    return self.writeJobOptions(retVal['Records'],optionsFile = '', savedType = savetype, catalog = None, savePfn=None)
  
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
      if savetype == 'txt':
        for lfn in files:
          fd += str(lfn[0])+'\n'
        return fd
    
    # get lst of event types
    import time
    evtTypes = {}
    nbEvts = 0
    fileType = None
    for i in files:
        file = files[i]
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
    poolTypes = ["DST", "RDST", "DIGI", "SIM"]
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
  def getProcessingPassDesc(self, desc, passid, simid='ALL'):
    return self.db_.getProcessingPassDesc(desc, passid, simid)
  
  #############################################################################       
  def getMoreProductionInformations(self, prodid):
    return self.db_.getMoreProductionInformations(prodid)
