"""
LHCb Bookkeeping database manager
"""
########################################################################
# $Id$
########################################################################



from DIRAC                                                                      import gLogger, S_OK, S_ERROR
from LHCbDIRAC.BookkeepingSystem.Client.BaseESManager                        import BaseESManager
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient                    import BookkeepingClient
from LHCbDIRAC.BookkeepingSystem.Client                                      import objects
from LHCbDIRAC.BookkeepingSystem.Client.Help                                 import Help
from DIRAC.Resources.Catalog.FileCatalog                                     import FileCatalog
import os
import types
import time

__RCSID__ = "$Id$"

INTERNAL_PATH_SEPARATOR = "/"

#############################################################################
class LHCbBookkeepingManager( BaseESManager ):
  """creates the virtual file system"""

  __bookkeepingFolderProperties = ['name',
                                'fullpath',
                                        ]
    # watch out for this ad hoc solution
    # if any changes made check all functions
    #
  __bookkeepingConfigurationPrefixes = ['ConfigName',  # configname
                                   'ConfigVersion',  # configversion
                                   'Simulation/DataTaking',
                                   'ProcessingPass',
                                   'EventType',  # event type
                                   'Production',  # production
                                   'FileType',  # file type
                                   ''
                                   ]

  __bookkeepingProductionPrefixes = ['PROD',
                                   'EVT',
                                   'FTY',
                                   ''
                                  ]

  __bookkeepingRunPrefixes = ['RUN',
                                'PAS',
                                'EVT',
                                'FTY',
                                 ''
                                  ]

  __bookkeepingEventtypePrefixes = ['ConfigName',
                                  'ConfigVersion',
                                  'EventType',
                                  'Simulation/DataTaking',
                                  'ProcessingPass',
                                  'Production',
                                  'FileType',
                                   '',
                                  ]
  __bookkeepingDatabasePrefixes = []

  __bookkeepingParameters = ['Configuration', 'Event type' , 'Productions', 'Runlookup' ]

  __bookkeepingShortparameternames = {__bookkeepingParameters[0]:'sim', __bookkeepingParameters[1]:'evt' ,
                                    __bookkeepingParameters[2]:'prod', __bookkeepingParameters[3]:'run' }

  __bookkeepingQueryTypes = ['adv', 'std']

  #############################################################################
  def __init__( self, rpcClinet = None, web = False ):
    """initialize the values"""
    BaseESManager.__init__( self )
    self._BaseESManager___fileSeparator = INTERNAL_PATH_SEPARATOR
    # self.__pathSeparator = INTERNAL_PATH_SEPARATOR
    self.db_ = BookkeepingClient( rpcClinet )
    if not web:
      self.fc = FileCatalog()
    self.helper_ = Help()

    self.__entityCache = {'/':( objects.Entity( {'name':'/', 'fullpath':'/', 'expandable':True} ), 0 )}
    self.parameter_ = self.__bookkeepingParameters[0]
    self.files_ = []
    self.__filetypes = []
    
    self.treeLevels_ = -1
    self.advancedQuery_ = False
    print 'WELCOME'
    print "For more information use the 'help' command! "
    self.dataQualities_ = {}
    if not web:
      retVal = self.db_.getAvailableFileTypes()
      if not retVal['OK']:
        gLogger.error( retVal )
      else:
        self.__filetypes = [ i[0] for i in retVal['Value']['Records']]

  #############################################################################
  def setFileTypes( self, fileTypeList = list() ):
    """it sets the file types. The parameter is a list of file type"""
    if fileTypeList and len(fileTypeList)>0:
      self.__filetypes = fileTypeList
    else:
      retVal = self.db_.getAvailableFileTypes()
      if not retVal['OK']:
        gLogger.error( retVal )
      else:
        self.__filetypes = [ i[0] for i in retVal['Value']['Records']]
    
  #############################################################################
  def _updateTreeLevels( self, level ):
    """tree level update"""
    self.treeLevels_ = level

  #############################################################################
  @staticmethod
  def setVerbose( value ):
    """information printed"""
    objects.VERBOSE = value

  #############################################################################
  def setAdvancedQueries( self, value ):
    """advanced queries"""
    self.advancedQuery_ = value

  #############################################################################
  def _getTreeLevels( self ):
    """level of the current tree"""
    return self.treeLevels_

  #############################################################################
  def help( self ):
    """help information"""
    if self.parameter_ == self.__bookkeepingParameters[0]:
      self.helper_.helpConfig( self._getTreeLevels() )
    elif self.parameter_ == self.__bookkeepingParameters[1]:
      self.helper_.helpEventType( self._getTreeLevels() )
    elif self.parameter_ == self.__bookkeepingParameters[2]:
      self.helper_.helpProcessing( self._getTreeLevels() )

  #############################################################################
  def getPossibleParameters( self ):
    """available parameters"""
    return self.__bookkeepingParameters

  #############################################################################
  def getCurrentParameter( self ):
    """current parameters"""
    return self.__bookkeepingShortparameternames[self.parameter_]

  #############################################################################
  def getQueriesTypes( self ):
    """types of queries"""
    if self.advancedQuery_:
      return self.__bookkeepingQueryTypes[0]
    else:
      return self.__bookkeepingQueryTypes[1]

  #############################################################################
  def setParameter( self, name ):
    """query types"""
    if self.__bookkeepingParameters.__contains__( name ):
      self.parameter_ = name
      self.treeLevels_ = -1
      if name == 'Configuration':
        self.__bookkeepingDatabasePrefixes = self.__bookkeepingConfigurationPrefixes
      elif name == 'Productions':
        self.__bookkeepingDatabasePrefixes = self.__bookkeepingProductionPrefixes
      elif name == 'Event type':
        self.__bookkeepingDatabasePrefixes = self.__bookkeepingEventtypePrefixes
      elif name == 'Runlookup':
        self.__bookkeepingDatabasePrefixes = self.__bookkeepingRunPrefixes
    else:
      gLogger.error( "Wrong Parameter!" )

  #############################################################################
  def getLogicalFiles( self ):
    """lfn"""
    return self.files_

  #############################################################################
  def getFilesPFN( self ):
    """pfn"""
    lfns = self.files_
    res = self.fc.getReplicas( lfns )
    return res

  #############################################################################
  def list( self, path = "/", selectionDict = None, sortDict = None, startItem = 0, maxitems = 0 ):
    """list a path"""
    gLogger.debug( path )
    selectionDict = selectionDict if selectionDict is not None else {}
    sortDict = sortDict if sortDict is not None else {}

    if self.parameter_ == self.__bookkeepingParameters[0]:
      return self._listConfigs( path, sortDict, startItem, maxitems )
    elif self.parameter_ == self.__bookkeepingParameters[1]:
      return self._listEventTypes( path, sortDict, startItem, maxitems )
    elif self.parameter_ == self.__bookkeepingParameters[2]:
      return self._listProduction( path )
    elif self.parameter_ == self.__bookkeepingParameters[3]:
      return self._listRuns( path )

  #############################################################################
  def getLevelAndPath( self, path ):
    """level and path"""
    if path == '/':
      return 0, [], ''  # it is the first level
    path = self.getAbsolutePath( path )['Value']  # shall we do this here or in the _processedPath()?
    processedPath = self._processPath( path )
    tmpPath = list( processedPath )
    if self.parameter_ == self.__bookkeepingParameters[1]:
      level, procpass = self.__getEvtLevel( tmpPath, [], level = 0, start = False, end = False,
                                           processingpath = '', startlevel = 4 )
    elif self.parameter_ == self.__bookkeepingParameters[3]:
      level, procpass = self.__getRunLevel( tmpPath, [], level = 0, start = False, end = False,
                                           processingpath = '', startlevel = 1 )
    else:
      level, procpass = self.__getLevel( path = tmpPath, visited = [],
                                        level = 0, start = False,
                                        end = False, processingpath = '',
                                        startlevel = 3 )
    self._updateTreeLevels( level )
    return level, processedPath, procpass

  #############################################################################
  # This method recursive visite all the tree nodes and found the processing pass
  def __getLevel( self, path, visited, level, start, end, processingpath, startlevel ):
    """level"""
    for i in path:
      if level == startlevel and start == False:
        for j in visited:
          path.remove( j )
        level += 1
        return self.__getLevel( path, visited,
                               level, start = True,
                               end = False, processingpath = '',
                               startlevel = 3 )
      else:
        level += 1
        try:
          result = type( long( i ) ) == types.LongType
          if start and result:
            end = True
        except ValueError, ex:
          gLogger.warn( str( self.__class__ ) + "__getLevel" + str( ex ) )
        if start and not end:
          level = startlevel
          processingpath += '/' + i
        elif end and level <= startlevel + 1:
          level = startlevel + 1
      visited += [i]
    return level, processingpath

  #############################################################################
  def __getRunLevel( self, path, visited, level, start, end, processingpath, startlevel ):
    """run level"""
    for i in path:
      if level == startlevel and start == False:
        for j in visited:
          path.remove( j )
        level += 1
        return self.__getRunLevel( path, visited,
                                  level, start = True,
                                  end = False,
                                  processingpath = '',
                                  startlevel = 1 )
      else:
        level += 1
        try:
          result = type( long( i ) ) == types.LongType
          if start and result:
            end = True
        except ValueError, ex:
          gLogger.warn( str( self.__class__ ) + "__getRunLevel" + str( ex ) )
        if start and not end:
          level = startlevel
          processingpath += '/' + i
        elif end and level <= startlevel + 1:
          level = startlevel + 1
      visited += [i]
    return level, processingpath

  #############################################################################
  # This method recursive visite all the tree nodes and found the processing pass
  def __getEvtLevel( self, path, visited, level, start, end, processingpath, startlevel ):
    """evt level"""
    for i in path:
      if level == startlevel and start == False:
        for j in visited:
          path.remove( j )
        level += 1
        return self.__getEvtLevel( path, visited,
                                  level, start = True,
                                  end = False, processingpath = '',
                                  startlevel = 4 )
      else:
        level += 1
        try:
          result = ( type( long( i ) ) == types.LongType )
        except ValueError, ex:
          gLogger.warn( str( self.__class__ ) + "__getEvtLevel" + str( ex ) )
          result = i in self.__filetypes
        if start and result:
          end = True
        if start and not end:
          level = startlevel
          processingpath += '/' + i
        elif end and level <= startlevel + 1:
          level = startlevel + 1
      visited += [i]
    return level, processingpath

  #############################################################################
  def _listConfigs( self, path, sortDict, startItem, maxitems ):
    """list 1th tree"""
    entityList = list()
    levels, processedPath, procpass = self.getLevelAndPath( path )

    if levels == 0:  # configname
      self.clevelHeader_0()
      entityList += self.clevelBody_0( path, levels, )

    if levels == 1:  # config version
      in_dict = self.clevelHeader_1( processedPath )
      entityList += self.clevelBody_1( path, levels, in_dict )

    if levels == 2:  # sim or data desc
      in_dict = self.clevelHeader_2( processedPath )
      entityList += self.clevelBody_2( path, levels, in_dict )

    if levels == 3:  # processing
      in_dict = self.clevelHeader_3( processedPath )
      entityList += self.clevelBody_3( path, levels, in_dict, procpass )

    if levels == 4 and self.advancedQuery_:  # prod
      in_dict = self.clevelHeader_4( processedPath, procpass )
      entityList += self.clevelBody_4( path, levels, in_dict )
    elif levels == 4 and not self.advancedQuery_:
      processedPath += ['ALL']
      in_dict = self.clevelHeader_5( processedPath, procpass )
      entityList += self.clevelBody_5( path, 5, in_dict )

    if levels == 5 and self.advancedQuery_:  # file type
      in_dict = self.clevelHeader_5( processedPath, procpass )
      entityList += self.clevelBody_5( path, levels, in_dict )
    elif levels == 5 and not self.advancedQuery_:
      levels = 6

    if levels == 6 and startItem == 0 and maxitems == 0:  # files
      in_dict = self.clevelHeader_6( processedPath, procpass )
      entityList += self.clevelBody_6( path, levels, in_dict )
    elif levels == 6 and ( startItem != 0 or maxitems != 0 ):  # files
      in_dict = self.clevelHeader_6( processedPath, procpass )
      entityList += self.clevelBodyLimited_6( path, levels, in_dict, sortDict, startItem, maxitems )
    return entityList

  #############################################################################
  def __addAll( self, path, levels, description ):
    """ level all"""
    if self.advancedQuery_:
      return self._getEntityFromPath( path, "ALL", levels, description )
    else:
      return None

  @staticmethod
  def __createPath( processedPath, name ):
    """ create a path"""
    path = ''
    for i in processedPath:
      string = '/' + i[0] + '_' + i[1]
      path += string

    path += '/' + name[0] + '_' + name[1]
    return path


  #############################################################################
  @staticmethod
  def clevelHeader_0():
    """configuration tree (first tree)"""
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug ( "Configurations names:" )
    gLogger.debug ( "-----------------------------------------------------------" )

    # list root
    gLogger.debug( "listing Configuration Names" )

  #############################################################################
  def clevelBody_0( self, path, levels ):
    """1t level of the configuration tree"""
    entityList = list()
    result = self.db_.getAvailableConfigNames()

    if result['OK']:
      dbResult = result['Value']
      for record in dbResult['Records']:
        entityList += [self._getEntityFromPath( path, record[0], levels, None, {}, 'getAvailableConfigNames' )]
      self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
    return entityList

  #############################################################################
  @staticmethod
  def clevelHeader_1( processedPath ):
    """ second level"""
    gLogger.debug( "listing configversions" )
    in_dict = {'ConfigName': processedPath[0]}
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:" )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Configuration Name      | %s " % ( processedPath[0] ) )

    gLogger.debug( "Available Config Versions:" )
    return in_dict

  def clevelBody_1( self, path, levels, in_dict ):
    """second """
    entityList = list()
    result = self.db_.getConfigVersions( in_dict )
    if result['OK']:
      dbResult = result['Value']
      description = dbResult["ParameterNames"][0]
      for record in dbResult['Records']:
        entityList += [self._getEntityFromPath( path, record[0], levels, description, in_dict, 'getConfigVersions' )]
      self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
    return entityList


  #############################################################################
  @staticmethod
  def clevelHeader_2( processedPath ):
    """third"""
    gLogger.debug( "listing Simulation Conditions!" )
    in_dict = {'ConfigName': processedPath[0],
            'ConfigVersion':processedPath[1]}

    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:" )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Available Simulation Conditions:" )
    return in_dict

  #############################################################################
  def clevelBody_2( self, path, levels, in_dict ):
    """build the tree node"""
    entityList = list()
    result = self.db_.getConditions( in_dict )
    if result['OK']:
      dbResult = result['Value']
      if dbResult[0]["TotalRecords"] > 0:
        add = self.__addAll( path, levels, 'Simulation Conditions/DataTaking' )
        if add:
          entityList += [add]
        for record in dbResult[0]['Records']:
          value = {}
          j = 0
          for i in dbResult[0]['ParameterNames']:
            value[i] = record[j]
            j += 1
          entityList += [self._getSpecificEntityFromPath( path, value,
                                                         record[1], levels,
                                                         None, 'Simulation Conditions/DataTaking',
                                                         in_dict, 'getConditions' )]
        self._cacheIt( entityList )
      if dbResult[1]["TotalRecords"] > 0:
        for record in dbResult[1]['Records']:
          value = {}
          j = 0
          for i in dbResult[1]['ParameterNames']:
            value[i] = record[j]
            j += 1
          entityList += [self._getSpecificEntityFromPath( path, value, record[1], levels,
                                                         None, 'Simulation Conditions/DataTaking',
                                                         in_dict, 'getConditions' )]
        self._cacheIt( entityList )

    else:
      gLogger.error( result['Message'] )

    return entityList


  #############################################################################
  @staticmethod
  def clevelHeader_3( processedPath ):
    """fourth level"""
    gLogger.debug( "listing processing pass" )
    in_dict = {'ConfigName': processedPath[0],
            'ConfigVersion':processedPath[1],
            'ConditionDescription':processedPath[2]}

    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:" )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )
    gLogger.debug( "-----------------------------------------------------------" )

    gLogger.debug( "Available processing pass:\n" )
    return in_dict

  #############################################################################
  def clevelBody_3( self, path, levels, in_dict, procpass ):
    """ fourth tree node"""
    entityList = list()
    in_dict['ProcessingPass'] = procpass
    result = self.db_.getProcessingPass( in_dict, procpass )
    if result['OK']:
      dbResult = result['Value']
      if dbResult[0]['TotalRecords'] > 0:  # it is a processing pass
        add = self.__addAll( path, levels, 'Processing Pass' )
        if add:
          entityList += [add]
        for record in dbResult[0]['Records']:
          entityList += [self._getEntityFromPath( path, record[0],
                                                 levels, 'Processing Pass',
                                                 in_dict, 'getProcessingPass' )]
        self._cacheIt( entityList )
      if dbResult[1]['TotalRecords'] > 0:
        value = {}
        for record in dbResult[1]['Records']:
          value = {'Event Type':record[0], 'Description':record[1]}
          entityList += [self._getSpecificEntityFromPath( path, value, str( record[0] ),
                                                         levels, None, 'Event types',
                                                         in_dict, 'getProcessingPass' )]
        self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
    return entityList

  #############################################################################
  @staticmethod
  def clevelHeader_4( processedPath, procpass ):
    """ 5th level"""
    gLogger.debug( "listing event types" )
    retVal = procpass.split( '/' )[1:]
    for i in retVal:
      processedPath.remove( i )

    in_dict = { 'ConfigName': processedPath[0],
               'ConfigVersion':processedPath[1],
               'ConditionDescription': processedPath[2],
               'EventType': processedPath[3]}
    in_dict['ProcessingPass'] = procpass

    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters: " )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )
    gLogger.debug( "-----------------------------------------------------------" )

    gLogger.debug( "Available event types types:" )
    return in_dict


  #############################################################################
  def clevelBody_4( self, path, levels, in_dict ):
    """5th tree node"""
    entityList = list()
    result = self.db_.getProductions( in_dict )
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult['Records']:
        entityList += [self._getEntityFromPath( path, str( record[0] ), levels,
                                               'Production(s)/Run(s)', in_dict, 'getProductions' )]
      self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
    return entityList

  #############################################################################
  def clevelHeader_5( self, processedPath, procpass ):
    """6th tree"""
    gLogger.debug( "listing event types" )
    retVal = procpass.split( '/' )[1:]
    for i in retVal:
      processedPath.remove( i )

    if self.advancedQuery_:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'ConditionDescription': processedPath[2],
                 'EventType': processedPath[3],
                 'Production':processedPath[4]}
    else:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'ConditionDescription': processedPath[2],
                 'EventType': processedPath[3]}
    in_dict['ProcessingPass'] = procpass

    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters: " )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )
    gLogger.debug( "-----------------------------------------------------------" )

    gLogger.debug( "Available event types types:" )
    return in_dict


  #############################################################################
  def clevelBody_5( self, path, levels, in_dict ):
    """build the 6th tree node"""
    entityList = list()
    in_dict['Visible'] = 'Y'
    result = self.db_.getFileTypes( in_dict )
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult['Records']:
        entityList += [self._getEntityFromPath( path, record[0], levels, 'FileTypes', in_dict, 'getFileTypes' )]
      self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
    return entityList

  #############################################################################
  def clevelHeader_6( self, processedPath, procpass ):
    """7th tree prepare"""
    gLogger.debug( "listing event types" )

    retVal = procpass.split( '/' )[1:]
    for i in retVal:
      processedPath.remove( i )

    if self.advancedQuery_:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'ConditionDescription': processedPath[2],
                 'EventType': processedPath[3],
                 'Production':processedPath[4],
                 'FileType':processedPath[5]}
    else:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'ConditionDescription': processedPath[2],
                 'EventType': processedPath[3],
                 'FileType':processedPath[4]}
    in_dict['ProcessingPass'] = procpass

    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters: " )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )
    gLogger.debug( "-----------------------------------------------------------" )

    return in_dict

  #############################################################################
  def clevelBody_6( self, path, levels, in_dict ):
    """build the 7th tree"""
    entityList = list()
    in_dict['Quality'] = self.__getSelectedQualities()
    in_dict['Visible'] = 'Y'
    result = self.db_.getFilesWithMetadata( in_dict )
    if result['OK']:
      for record in result['Value']['Records']:
        value = {'name':record[0], 'EventStat':record[1], 'FileSize':record[2], 'CreationDate':record[3],
                 'JobStart':record[4], 'JobEnd':record[5], 'WorkerNode':record[6],
                 'FileType':in_dict['FileType'], 'RunNumber':record[8],
                 'FillNumber':record[9], 'FullStat':record[10], 'DataqualityFlag':record[11],
                 'EventType':in_dict['EventType'],
                 'EventInputStat':record[12], 'TotalLuminosity':record[13], 'Luminosity':record[14],
                 'InstLuminosity':record[15], 'TCK':record[16]}
        self.files_ += [record[0]]
        entityList += [self._getEntityFromPath( path, value, levels, 'List of files', in_dict, 'getFilesWithMetadata' )]
      self._cacheIt( entityList )
    else:
      return result
    return entityList

  #############################################################################
  def clevelBodyLimited_6( self, path, levels, in_dict, sortDict, startItem, maxitems ):
    """7th tree node for web"""
    entityList = list()
    in_dict['Quality'] = self.__getSelectedQualities()
    result = self.__getFiles( in_dict, sortDict, startItem, maxitems )
    for record in result['Records']:
      value = {'name':record[0], 'EventStat':record[1], 'FileSize':record[2], 'CreationDate':record[3],
               'JobStart':record[4], 'JobEnd':record[5], 'WorkerNode':record[6],
               'FileType':in_dict['FileType'], 'EventType':in_dict['EventType'],
               'RunNumber':record[9], 'FillNumber':record[10], 'FullStat':record[11],
               'DataqualityFlag':record[12], 'EventInputStat':record[13],
               'TotalLuminosity':record[14], 'Luminosity':record[15], 'InstLuminosity':record[16], 'TCK':record[17]}

      self.files_ += [record[0]]
      entityList += [self._getEntityFromPath( path, value, levels, 'List of files', in_dict, 'getFilesWithMetadata' )]
    self._cacheIt( entityList )
    return entityList


  #############################################################################
  def _listEventTypes( self, path, sortDict, startItem, maxitems ):
    """second tree based on event type"""
    entityList = list()
    levels, processedPath, procpass = self.getLevelAndPath( path )

    if levels == 0:  # configname
      self.clevelHeader_0()
      entityList += self.clevelBody_0( path, levels, )

    if levels == 1:  # config version
      in_dict = self.clevelHeader_1( processedPath )
      entityList += self.clevelBody_1( path, levels, in_dict )

    if levels == 2:  # event type
      in_dict = self.clevelHeader_2( processedPath )
      entityList += self.elevelBody_2( path, levels, in_dict )

    if levels == 3:  # sim ot daq desq
      in_dict = self.elevelHeader_3( processedPath )
      entityList += self.elevelBody_3( path, levels, in_dict )

    if levels == 4:  # processing pass
      in_dict = self.elevelHeader_4( processedPath )
      entityList += self.elevelBody_4( path, levels, in_dict, procpass )

    if self.advancedQuery_ and levels == 5:
      in_dict = self.elevelHedaer_5( processedPath, procpass )
      entityList += self.clevelBody_5( path, levels, in_dict )
    elif levels == 5:
      levels = 6

    if levels == 6 and startItem == 0 and maxitems == 0:  # files
      in_dict = self.elevelHeader_6( processedPath, procpass )
      entityList += self.clevelBody_6( path, levels, in_dict )
    elif levels == 6 and ( startItem != 0 or maxitems != 0 ):  # files
      in_dict = self.elevelHeader_6( processedPath, procpass )
      entityList += self.clevelBodyLimited_6( path, levels, in_dict, sortDict, startItem, maxitems )

    return entityList

  #############################################################################
  def elevelBody_2( self, path, levels, in_dict ):
    """event type based queries"""
    entityList = list()
    result = self.db_.getEventTypes( in_dict )
    if result['OK']:
      dbResult = result['Value']
      if len( dbResult ) > 1:
        add = self.__addAll( path, levels, 'Event types' )
        if add:
          entityList += [add]
      for record in dbResult['Records']:
        value = {'Event Type':record[0], 'Description':record[1]}
        entityList += [self._getSpecificEntityFromPath( path, value, str( record[0] ),
                                                       levels, None, 'Event types',
                                                       in_dict, 'getEventTypes' )]
      self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
      return result
    return entityList

  #############################################################################
  @staticmethod
  def elevelHeader_3( processedPath ):
    """event type based tree node"""
    gLogger.debug( "listing simulation conditions" )

    in_dict = { 'ConfigName': processedPath[0], 'ConfigVersion':processedPath[1], 'EventType': processedPath[2]}


    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:" )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )
    gLogger.debug( "-----------------------------------------------------------" )

    gLogger.debug( "Available conditions:" )
    return in_dict

  #############################################################################
  def elevelBody_3( self, path, levels, in_dict ):
    """building the tree node"""

    entityList = list()

    result = self.db_.getConditions( in_dict )
    if result['OK']:
      dbResult = result['Value']
      if dbResult[0]["TotalRecords"] > 0:
        add = self.__addAll( path, levels, 'Simulation Conditions/DataTaking' )
        if add:
          entityList += [add]
        for record in dbResult[0]['Records']:
          value = {}
          j = 0
          for i in dbResult[0]['ParameterNames']:
            value[i] = record[j]
            j += 1
          entityList += [self._getSpecificEntityFromPath( path, value, record[1],
                                                         levels, None, 'Simulation Conditions/DataTaking',
                                                         in_dict, 'getConditions' )]
        self._cacheIt( entityList )
      if dbResult[1]["TotalRecords"] > 0:
        for record in dbResult[1]['Records']:
          value = {}
          j = 0
          for i in dbResult[1]['ParameterNames']:
            value[i] = record[j]
            j += 1
          entityList += [self._getSpecificEntityFromPath( path, value, record[1], levels,
                                                         None, 'Simulation Conditions/DataTaking',
                                                         in_dict, 'getConditions' )]
        self._cacheIt( entityList )

    else:
      gLogger.error( result['Message'] )

    return entityList

  #############################################################################
  @staticmethod
  def elevelHeader_4( processedPath ):
    """prepare the 5th query"""
    gLogger.debug( "listing processing pass" )

    in_dict = {'ConfigName': processedPath[0],
            'ConfigVersion':processedPath[1],
            'EventType':processedPath[2],
            'ConditionDescription':processedPath[3]
             }


    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters: " )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )
    gLogger.debug( "-----------------------------------------------------------" )

    gLogger.debug( "Available processing pass types:" )
    return in_dict

  #############################################################################
  def elevelBody_4( self, path, levels, in_dict, procpass ):
    """make the tree node"""
    entityList = list()

    result = self.db_.getProcessingPass( in_dict, procpass )
    if result['OK']:
      dbResult = result['Value']
      if dbResult[0]['TotalRecords'] > 0:  # it is a processing pass
        add = self.__addAll( path, levels, 'Processing Pass' )
        if add:
          entityList += [add]
        for record in dbResult[0]['Records']:
          entityList += [self._getEntityFromPath( path, record[0],
                                                 levels, 'Processing Pass',
                                                 in_dict, 'getProcessingPass' )]
        self._cacheIt( entityList )
      elif self.advancedQuery_:
        in_dict['ProcessingPass'] = procpass
        result = self.db_.getProductions( in_dict )
        if result['OK']:
          dbResult = result['Value']
          for record in dbResult['Records']:
            entityList += [self._getEntityFromPath( path, str( record[0] ),
                                                   levels, 'Production(s)/Run(s)',
                                                   in_dict, 'getProductions' )]
          self._cacheIt( entityList )

    if len( procpass ) > 0:
      in_dict['ProcessingPass'] = procpass
      in_dict['Visible'] = 'Y'
      result = self.db_.getFileTypes( in_dict )
      if result['OK']:
        dbResult = result['Value']
        for record in dbResult['Records']:
          entityList += [self._getEntityFromPath( path, record[0], levels, 'FileTypes', in_dict, 'getFileTypes' )]
        self._cacheIt( entityList )
      else:
        gLogger.error( result['Message'] )
    return entityList

  #############################################################################
  def elevelHedaer_5( self, processedPath, procpass ):
    """prepare tree node"""
    retVal = procpass.split( '/' )[1:]
    for i in retVal:
      processedPath.remove( i )

    if self.advancedQuery_:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'EventType': processedPath[2],
                 'ConditionDescription': processedPath[3],
                 'Production':processedPath[4]}
    else:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'EventType': processedPath[2],
                 'ConditionDescription': processedPath[3]}
    in_dict['ProcessingPass'] = procpass


    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:   " )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Available Production(s):" )
    return in_dict

  #############################################################################
  def elevelHeader_6( self, processedPath, procpass ):
    """ prepare tree mode"""
    retVal = procpass.split( '/' )[1:]
    for i in retVal:
      processedPath.remove( i )

    if self.advancedQuery_:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'EventType': processedPath[2],
                 'ConditionDescription': processedPath[3],
                 'Production':processedPath[4],
                 'FileType':processedPath[5]}
    else:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'EventType': processedPath[2],
                 'ConditionDescription': processedPath[3],
                 'FileType':processedPath[4]}
    in_dict['ProcessingPass'] = procpass


    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:   " )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Available file types:" )
    return in_dict

  #############################################################################
  def _listProduction( self, path ):
    """production lookup"""
    entityList = list()
    levels, processedPath, procpass = self.getLevelAndPath( path )
    gLogger.debug( str( procpass ) )

    if levels == 0:
      self.plevelHeader_0()
      entityList += self.plevelBody_0( path, levels )

    if levels == 1:
      in_dict = self.plevelHeader_2( processedPath )
      entityList += self.plevelBody_2( path, levels, in_dict )

    if levels == 2:
      in_dict = self.plevelHeader_3( processedPath )
      entityList += self.plevelBody_3( path, levels, in_dict )

    if levels == 3:
      in_dict = self.plevelHeader_4( processedPath )
      entityList += self.plevelBody_4( path, levels, in_dict )

    return entityList

  #############################################################################
  def _listRuns( self, path ):
    """run lookup"""
    entityList = list()

    levels, processedPath, procpass = self.getLevelAndPath( path )

    if levels == 0:
      self.rlevelHeader_0()
      entityList += self.rlevelBody_0( path, levels )

    if levels == 1:
      in_dict = self.rlevelHeader_2( processedPath )
      entityList += self.rlevelBody_2( path, levels, in_dict, procpass )

    if levels == 2:
      in_dict = self.rlevelHeader_3( processedPath, procpass )
      entityList += self.rlevelBody_3( path, levels, in_dict )

    if levels == 3:
      in_dict = self.rlevelHeader_4( processedPath, procpass )
      entityList += self.clevelBody_6( path, levels, in_dict )

    return entityList

  #############################################################################
  @staticmethod
  def plevelHeader_0():
    """prepare production lookup tree node"""
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug ( "productions:" )
    gLogger.debug ( "-----------------------------------------------------------" )

    # list root
    gLogger.debug( "listing productions" )

  #############################################################################
  def plevelBody_0( self, path, levels ):
    """make the node of the production lookup tree"""
    entityList = list()
    result = self.db_.getAvailableProductions()

    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        prod = str( record[0] )
        entityList += [self._getEntityFromPath( path, str( prod ), levels, 'Production(s)/Run(s)' )]
      self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
    return entityList

  #############################################################################
  @staticmethod
  def plevelHeader_2( processedPath ):
    """prepare the tree node"""
    gLogger.debug( "listing eventtype" )

    in_dict = {'Production':processedPath[0]}

    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:" )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )

    gLogger.debug( "Available event types:" )
    return in_dict

  #############################################################################
  def plevelBody_2( self, path, levels, in_dict ):
    """make the tree node"""
    entityList = list()

    result = self.db_.getEventTypes( in_dict )
    if result['OK']:
      dbResult = result['Value']
      if len( dbResult ) > 1:
        add = self.__addAll( path, levels, 'Event types' )
        if add:
          entityList += [add]
      for record in dbResult['Records']:
        value = {'Event Type':record[0], 'Description':record[1]}
        entityList += [self._getSpecificEntityFromPath( path, value,
                                                       str( record[0] ),
                                                       levels, None,
                                                       'Event types',
                                                       in_dict, 'getEventTypes' )]
      self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
    return entityList

  #############################################################################
  @staticmethod
  def plevelHeader_3( processedPath ):
    """prepare tree node"""
    gLogger.debug( "listing file types" )
    in_dict = {'Production':processedPath[0], 'EventType': processedPath[1]}
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:" )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )

    gLogger.debug( "Available file types:" )
    return in_dict

  #############################################################################
  def plevelBody_3( self, path, levels, in_dict ):
    """make tree node"""
    entityList = list()
    in_dict['Visible'] = 'Y'
    result = self.db_.getFileTypes( in_dict )
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult['Records']:
        entityList += [self._getEntityFromPath( path, record[0], levels, 'FileTypes', in_dict, 'getFileTypes' )]
      self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
    return entityList

  #############################################################################
  @staticmethod
  def plevelHeader_4( processedPath ):
    """prepare the tree node"""
    gLogger.debug( "listing file types" )
    in_dict = {'Production':processedPath[0], 'EventType': processedPath[1], 'FileType':processedPath[2]}

    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:" )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )

    gLogger.debug( "Available files:" )
    return in_dict

  #############################################################################
  def plevelBody_4( self, path, levels, in_dict ):
    """make the tree node"""
    entityList = list()
    in_dict['Quality'] = self.__getSelectedQualities()
    in_dict['Visible'] = 'Y'
    result = self.db_.getFilesWithMetadata( in_dict )
    if result['OK']:
      for record in result['Value']['Records']:
        value = {'name':record[0], 'EventStat':record[1], 'FileSize':record[2],
                 'CreationDate':record[3], 'JobStart':record[4], 'JobEnd':record[5],
                 'WorkerNode':record[6], 'FileType':in_dict['FileType'], 'RunNumber':record[8],
                 'FillNumber':record[9], 'FullStat':record[10], 'DataqualityFlag':record[11],
                 'EventType':in_dict['EventType'], 'EventInputStat':record[12],
                 'TotalLuminosity':record[13], 'Luminosity':record[14],
                 'InstLuminosity':record[15], 'TCK':record[16]}
        self.files_ += [record[0]]
        entityList += [self._getEntityFromPath( path, value,
                                               levels, 'List of files',
                                               in_dict, 'getFilesWithMetadata' )]
      self._cacheIt( entityList )
    else:
      return result

    return entityList

  @staticmethod
  def rlevelHeader_0():
    """prepare run lookup node"""
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug ( "Runs:" )
    gLogger.debug ( "-----------------------------------------------------------" )

    # list root
    gLogger.debug( "listing runs" )

  #############################################################################
  def rlevelBody_0( self, path, levels ):
    """make tree node"""
    entityList = list()
    result = self.db_.getAvailableRuns()

    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        run = str( record[0] )
        entityList += [self._getEntityFromPath( path, str( run ), levels, 'Run(s)' )]
      self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
    return entityList

  #############################################################################
  @staticmethod
  def rlevelHeader_2( processedPath ):
    """prepare tree node"""
    gLogger.debug( "listing processing pass" )
    in_dict = {'RunNumber':processedPath[0]}
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:" )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )

    gLogger.debug( "Available processing pass:" )
    return in_dict

  #############################################################################
  def rlevelBody_2( self, path, levels, in_dict, procpass ):
    """make tree node"""
    entityList = list()
    in_dict['ProcessingPass'] = procpass
    result = self.db_.getProcessingPass( in_dict, procpass )
    if result['OK']:
      dbResult = result['Value']
      if dbResult[0]['TotalRecords'] > 0:  # it is a processing pass
        add = self.__addAll( path, levels, 'Processing Pass' )
        if add:
          entityList += [add]
      for record in dbResult[0]['Records']:
        entityList += [self._getEntityFromPath( path, record[0],
                                               levels, 'Processing Pass',
                                               in_dict, 'getProcessingPass' )]
      self._cacheIt( entityList )
      if dbResult[1]['TotalRecords'] > 0:
        value = {}
        for record in dbResult[1]['Records']:
          value = {'Event Type':record[0], 'Description':record[1]}
          entityList += [self._getSpecificEntityFromPath( path, value, str( record[0] ),
                                                         levels, None, 'Event types',
                                                         in_dict, 'getProcessingPass' )]
        self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
    return entityList

  #############################################################################
  @staticmethod
  def rlevelHeader_3( processedPath, procpass ):
    """prepare node of the run lookup tree"""
    gLogger.debug( "listing eventtypes" )
    retVal = procpass.split( '/' )[1:]
    for i in retVal:
      processedPath.remove( i )

    in_dict = { 'RunNumber':processedPath[0], 'EventType': processedPath[1]}
    in_dict['ProcessingPass'] = procpass

    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:" )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )

    gLogger.debug( "Available eventtypes types:" )
    return in_dict

  #############################################################################
  def rlevelBody_3( self, path, levels, in_dict ):
    """make tree node"""
    entityList = list()
    in_dict['Visible'] = 'Y'
    result = self.db_.getFileTypes( in_dict )
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult['Records']:
        entityList += [self._getEntityFromPath( path, record[0], levels, 'FileTypes', in_dict, 'getFileTypes' )]
      self._cacheIt( entityList )
    else:
      gLogger.error( result['Message'] )
    return entityList


  #############################################################################
  @staticmethod
  def rlevelHeader_4( processedPath, procpass ):
    """prepare tree"""
    gLogger.debug( "listing file types" )
    retVal = procpass.split( '/' )[1:]
    for i in retVal:
      processedPath.remove( i )

    in_dict = { 'RunNumber':processedPath[0], 'EventType': processedPath[1], 'FileType':processedPath[2]}
    in_dict['ProcessingPass'] = procpass
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( "Selected parameters:" )
    gLogger.debug( "-----------------------------------------------------------" )
    gLogger.debug( in_dict )

    gLogger.debug( "Available file types:" )
    return in_dict


  #############################################################################
  @staticmethod
  def _getEntityFromPath( presentPath, newPathElement, level, leveldescription = None, selection = None, method = None ):
    """create a entity"""
    if isinstance( newPathElement, types.DictType ):
      # this must be a file
      entity = objects.Entity( newPathElement )
      newPathElement = str( entity['name'] ).rsplit( "/", 1 )[1]
      entity.update( {'FileName':entity['name']} )
      expandable = False
      entity.update( {'expandable':expandable} )
      if selection != None:
        entity.update( {'selection':selection} )

      if method != None:
        entity.update( {'method':method} )

    else:
      # this must be a folder
      entity = objects.Entity()
      name = newPathElement

      expandable = True

      fullPath = presentPath.rstrip( INTERNAL_PATH_SEPARATOR )
      fullPath += INTERNAL_PATH_SEPARATOR + \
      name


      entity.update( {'name':name, 'fullpath':fullPath, 'expandable':expandable} )

      if leveldescription != None:
        entity.update( {'level':leveldescription} )

      if leveldescription == 'FileTypes':
        entity.update( {'showFiles':0} )

      if selection != None:
        entity.update( {'selection':selection} )

      if method != None:
        entity.update( {'method':method} )

      elif level == 5:
        entity.update( {'showFiles':0} )
    return entity

  #############################################################################
  @staticmethod
  def _getSpecificEntityFromPath( presentPath, value,
                                 newPathElement, level,
                                 description = None,
                                 leveldescription = None,
                                 selection = None, method = None ):
    """crate a specific entity"""
    if isinstance( value, types.DictType ):
      entity = objects.Entity( value )
      name = newPathElement

      expandable = True

      fullPath = presentPath.rstrip( INTERNAL_PATH_SEPARATOR )
      fullPath += INTERNAL_PATH_SEPARATOR + \
      name

      if description != None:
        entity.update( {'name':description, 'fullpath':fullPath, 'expandable':expandable} )
      else:
        entity.update( {'name':name, 'fullpath':fullPath, 'expandable':expandable} )
      if leveldescription != None:
        entity.update( {'level':leveldescription} )

      if selection != None:
        entity.update( {'selection':selection} )

      if method != None:
        entity.update( {'method':method} )

      if level == 5:
        entity.update( {'showFiles':0} )

    return entity



  #############################################################################
  def _processPath( self, path ):
    """takes an absolute path and returns of tuples with prefixes and posfixes
     of path elements. If invalid path returns null"""
    path = path.encode( 'ascii' )
    path = path.strip( INTERNAL_PATH_SEPARATOR + " " )
    paths = path.split( self.getPathSeparator() )
    return paths

  #############################################################################
  def _cacheIt( self, entityList ):
    """it caches an entity or a list of entities"""
    if isinstance( entityList, objects.Entity ):
      # convert it into a list
      entityList = [entityList]
    elif not isinstance( entityList, types.ListType ):
      # neither entity nor list
      gLogger.warn( "couldn't cache invalid entity(list) of type " + str( entityList.__class__ ) )
      return

    for entity in entityList:
      # TO IMPLEMENT!! time of the caching
      try:
        if 'fullpath' in entity:
          self.__entityCache.update( {entity['fullpath']: ( entity, 0 )} )
      except ValueError, ex:
        gLogger.warn( "couldn't cache entity(?) " + str( entity ) + "  " + str( ex ) )
        return S_ERROR( 'couldnt cache entity!' )

  #############################################################################
  @staticmethod
  def getAbsolutePath( path ):
    """ get current working directory if empty"""
    if path in [ "", ".", None] :
      path = INTERNAL_PATH_SEPARATOR  # root
      # convert it into absolute path
    path = os.path.normpath( path )
    if os.sep != INTERNAL_PATH_SEPARATOR:
      path = path.replace( os.sep, INTERNAL_PATH_SEPARATOR )

    # for this special case of anomaly when double // may appear
    path = path.replace( 2 * INTERNAL_PATH_SEPARATOR, INTERNAL_PATH_SEPARATOR )

    return S_OK( path )

  #############################################################################
  def get( self, path = "/" ):
    """get a node for a given path"""
    path = self.getAbsolutePath( path )['Value']
    entity = self._getEntity( path )
    if entity.__class__ == types.NoneType:
      gLogger.error( path + " doesn't exist!" )
      # raise ValueError, "Invalid path %s" % path
    return S_OK( entity )

  #############################################################################
  def _getEntity( self, path ):
    """This is not doing anything at the moment"""
    try:
      entity = self.__entityCache[path][0]
      gLogger.debug( "getting " + str( path ) + " from the cache" )
      return entity
    except ValueError, ex:
      # not cached so far
      gLogger.debug( str( path ) + " not in cache. Fetching..." + ex )

    # Second try

    try:
      gLogger.debug( "getting " + str( path ) + " eventually from the cache" )
      entity = self.__entityCache[path][0]
      return entity
    except ValueError, ex:
      # still not in the cache... wrong path
      gLogger.warn( str( path ) + " seems to be a wrong path" + ex )
      return None

    return entity

  #############################################################################
  @staticmethod
  def getNumberOfEvents( files ):
    """statistics"""
    esum = 0
    for lfn in files:
      esum += int( lfn['EventStat'] )
    return esum

  #############################################################################
  def getJobInfo( self, lfn ):
    """job info"""
    result = self.db_.getJobInfo( lfn )
    value = None
    if result['OK']:
      dbResult = result['Value']
      for record in dbResult:
        value = {'DiracJobID':record[0], 'DiracVersion':record[1], 'EventInputStat':record[2],
                 'ExecTime':record[3], 'FirstEventNumber':record[4], \
                 'Location':record[5], 'Name':record[6], 'NumberofEvents':record[7], \
                  'StatisticsRequested':record[8], 'WNCPUPOWER':record[9],
                  'CPUTime':record[10], 'WNCACHE':record[11],
                  'WNMEMORY':record[12], 'WNMODEL':record[13],
                  'WORKERNODE':record[14], 'WNCPUHS06':record[15], 'TotalLuminosity':record[17]}
    else:
      gLogger.error( result['Message'] )
    return value

  #############################################################################
  def getLimitedFiles( self, selectionDict, sortDict, startItem, maxitems ):
    """web """
    if self.parameter_ == self.__bookkeepingParameters[0]:
      return self._getLimitedFilesConfigParams( selectionDict, sortDict, startItem, maxitems )
    elif self.parameter_ == self.__bookkeepingParameters[1]:
      return self._getLimitedFilesEventTypeParams( selectionDict, sortDict, startItem, maxitems )
    elif self.parameter_ == self.__bookkeepingParameters[2]:
      return self._getLimitedFilesProductions( selectionDict, sortDict, startItem, maxitems )
    elif self.parameter_ == self.__bookkeepingParameters[3]:
      return self._getLimitedFilesRuns( selectionDict, sortDict, startItem, maxitems )

  #############################################################################
  def _getDataSetTree1( self, selectionDict ):
    # it is the configname configversion(default) query.
    # The input parameter is a path and it constructs the dictionary.
    """input dictionary"""
    path = selectionDict['fullpath']
    levels, processedPath, procpass = self.getLevelAndPath( path )
    gLogger.debug( str( levels ) )
    retVal = procpass.split( '/' )[1:]
    for i in retVal:
      processedPath.remove( i )

    if self.advancedQuery_:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'ConditionDescription': processedPath[2],
                 'EventType': processedPath[3],
                 'Production':processedPath[4],
                 'FileType':processedPath[5]}
    else:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'ConditionDescription': processedPath[2],
                 'EventType': processedPath[3],
                 'FileType':processedPath[4]}
    in_dict['ProcessingPass'] = procpass
    in_dict['fullpath'] = path
    return in_dict


  #############################################################################
  def _getLimitedFilesConfigParams( self, selectionDict, sortDict, startItem, maxitems ):
    """input dictionary"""
    selection = self._getDataSetTree1( selectionDict )
    return self.__getFiles( selection, sortDict, startItem, maxitems )

  #############################################################################
  def _getDataSetTree2( self, selectionDict ):  # it is the event type based query
    """input dictionary of the event type tree"""
    path = selectionDict['fullpath']
    levels, processedPath, procpass = self.getLevelAndPath( path )
    gLogger.debug( str( levels ) )
    retVal = procpass.split( '/' )[1:]
    for i in retVal:
      processedPath.remove( i )

    if self.advancedQuery_:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'EventType': processedPath[2],
                 'ConditionDescription': processedPath[3],
                 'Production':processedPath[4],
                 'FileType':processedPath[5]}
    else:
      in_dict = { 'ConfigName': processedPath[0],
                 'ConfigVersion':processedPath[1],
                 'EventType': processedPath[2],
                 'ConditionDescription': processedPath[3],
                 'FileType':processedPath[4]}
    in_dict['ProcessingPass'] = procpass
    in_dict['fullpath'] = path
    return in_dict

  #############################################################################
  def _getLimitedFilesEventTypeParams( self, selectionDict, sortDict, startItem, maxitems ):
    """input dictionary"""
    selection = self._getDataSetTree2( selectionDict )
    return self.__getFiles( selection, sortDict, startItem, maxitems )

  #############################################################################
  def _getDataSetTree3( self, selectionDict ):  # production based query
    """input dictionary"""
    path = selectionDict['fullpath']
    levels, processedPath, procpass = self.getLevelAndPath( path )
    gLogger.debug( str( levels ) )
    gLogger.debug( procpass )

    in_dict = { 'Production': processedPath[0], 'EventType': processedPath[1], 'FileType':processedPath[2]}
    in_dict['fullpath'] = path
    return in_dict

  #############################################################################
  def _getLimitedFilesProductions( self, selectionDict, sortDict, startItem, maxitems ):
    """input dictionary"""
    selection = self._getDataSetTree3( selectionDict )
    return self.__getFiles( selection, sortDict, startItem, maxitems )

  #############################################################################
  def _getDataSetTree4( self, selectionDict ):  # run based query
    """input dictionary"""
    path = selectionDict['fullpath']
    levels, processedPath, procpass = self.getLevelAndPath( path )
    gLogger.debug( str( levels ) )
    retVal = procpass.split( '/' )[1:]
    for i in retVal:
      processedPath.remove( i )

    in_dict = {'RunNumber':processedPath[0],
               'EventType': processedPath[1],
               'FileType':processedPath[2],
               'ProcessingPass':procpass}
    in_dict['fullpath'] = path
    return in_dict

  #############################################################################
  def _getLimitedFilesRuns( self, selectionDict, sortDict, startItem, maxitems ):
    """input dictionary"""
    selection = self._getDataSetTree4( selectionDict )
    return self.__getFiles( selection, sortDict, startItem, maxitems )


  #############################################################################
  def __getFiles( self, in_dict, sortDict, startItem, maxitems ):
    """returns the files"""
    totalrecords = 0
    nbOfEvents = 0
    filesSize = 0
    lumi = 0
    selection = in_dict
    in_dict['Visible'] = 'Y'
    if len( sortDict ) > 0:
      in_dict['Quality'] = self.__getSelectedQualities()
      res = self.db_.getFilesSummary( in_dict )
      if not res['OK']:
        gLogger.error( res['Message'] )
      else:
        records = res['Value']['Records']
        totalrecords = records[0][0]
        nbOfEvents = records[0][1]
        filesSize = records[0][2]
        lumi = records[0][3]
    records = []
    parametersNames = []
    if startItem > -1 and maxitems != 0:
      in_dict['StartItem'] = startItem
      in_dict['MaxItem'] = maxitems
      in_dict['Quality'] = self.__getSelectedQualities()
      result = self.db_.getLimitedFiles( in_dict )

      if result['OK']:
        parametersNames = result['Value']['ParameterNames']
        records = result['Value']['Records']
        if totalrecords == 0:
          totalrecords = result['Value']['TotalRecords']
      else:
        gLogger.error( result['Message'] )
        return result

    return {'TotalRecords':totalrecords,
            'ParameterNames':parametersNames,
            'Records':records,
            'Extras': {'Selection':selection,
                       'GlobalStatistics':{'Number of Events':nbOfEvents, 'Files Size':filesSize, 'Luminosity': lumi}} }

  #############################################################################
  def getAncestors( self, files, depth ):
    """ancestors of given files"""
    return self.db_.getFileAncestors( files, depth )

  #############################################################################
  def getLogfile( self, filename ):
    """log file"""
    return self.db_.getFileCreationLog( filename )

  #############################################################################
  def writePythonOrJobOptions( self, startItem, maxitems, path, savetype ):
    """create Gaudi Card"""
    result = None
    dataset = None
    if self.parameter_ == self.__bookkeepingParameters[0]:
      result = self._getLimitedFilesConfigParams( {'fullpath':path}, {}, startItem, maxitems )
      dataset = self._getDataSetTree1( {'fullpath':path} )
    elif self.parameter_ == self.__bookkeepingParameters[1]:
      result = self._getLimitedFilesEventTypeParams( {'fullpath':path}, {}, startItem, maxitems )
      dataset = self._getDataSetTree2( {'fullpath':path} )
    elif self.parameter_ == self.__bookkeepingParameters[2]:
      result = self._getLimitedFilesProductions( {'fullpath':path}, {}, startItem, maxitems )
      dataset = self._getDataSetTree3( {'fullpath':path} )
    elif self.parameter_ == self.__bookkeepingParameters[3]:
      result = self._getLimitedFilesRuns( {'fullpath':path}, {}, startItem, maxitems )
      dataset = self._getDataSetTree4( {'fullpath':path} )

    if result.has_key( 'TotalRecords' ) and result['TotalRecords'] > 0:
      values = result['Records']
      params = result['ParameterNames']
      files = {}
      # The list has to be convert to dictionary
      for i in values:
        files[i[0]] = {'FileName':i[params.index( 'Name' )],
                       'EventStat':i[params.index( 'EventStat' )],
                       'FileSize':i[params.index( 'FileSize' )],
                       'FileType':i[params.index( 'FileType' )],
                       'EventType':i[params.index( 'EventType' )]}
      return self.writeJobOptions( files, optionsFile = '',
                                  savedType = savetype, catalog = None,
                                  savePfn = None, dataset = dataset )
    else:
      return S_ERROR( "Error discoverd during the option file creation!" )

  #############################################################################
  def getLimitedInformations( self, startItem, maxitems, path ):
    """statistics"""
    result = None
    if self.parameter_ == self.__bookkeepingParameters[0]:
      result = self._getLimitedFilesConfigParams( {'fullpath':path}, {'need':0}, startItem, maxitems )
    elif self.parameter_ == self.__bookkeepingParameters[1]:
      result = self._getLimitedFilesEventTypeParams( {'fullpath':path}, {'need':0}, startItem, maxitems )
    elif self.parameter_ == self.__bookkeepingParameters[2]:
      result = self._getLimitedFilesProductions( {'fullpath':path}, {'need':0}, startItem, maxitems )
    elif self.parameter_ == self.__bookkeepingParameters[3]:
      result = self._getLimitedFilesRuns( {'fullpath':path}, {'need':0}, startItem, maxitems )

    if result.has_key( 'TotalRecords' ) and result['TotalRecords'] > 0:
      nbe = 0
      fsize = 0
      nbfiles = 0
      params = result['ParameterNames']
      for lfn in result['Records']:
        nbfiles += 1
        if lfn[params.index( 'EventStat' )] != None:
          nbe += int( lfn[params.index( 'EventStat' )] )
        if lfn[params.index( 'FileSize' )] != None:
          fsize += int( lfn[params.index( 'FileSize' )] )
      return S_OK( {'Number of Events':nbe, 'Files Size':fsize, 'Number of files':nbfiles} )
    else:
      return S_ERROR( "Error discoverd during the option file creation!" )

  #############################################################################
  def writeJobOptions( self, files, optionsFile = '', savedType = None, catalog = None, savePfn = None, dataset = None ):
    """crate option"""
    fd = None
    if optionsFile == '':
      if savedType == 'txt':
        result = ''
        for lfn in files:
          result += str( lfn ) + '\n'
        return result

    # get lst of event types
    evtTypes = self.__createEventtypelist( files )

    pythonOpts = None
    if savedType != None:
      pythonOpts = savedType == 'py'
    else:
      fd = open( optionsFile, 'w' )
      ext = os.path.splitext( optionsFile )
      try:
        pythonOpts = ext[1] == '.py'
      except IndexError, ex:
        gLogger.warn( ex )
        pythonOpts = True


    string, comment = self.__addGaudiheader( pythonOpts, evtTypes )

    string = self.__addDatasetcreationmetadata( string, dataset, comment )


    filesandformats, rootFormat, lfn = self.__getFilesandFormats( savePfn, files )

    if rootFormat:
      string = self.__createRootformatstring( string, filesandformats )
    else:
      fileType = self.__getFileType( files )

      string = self.__addEventselector( string, pythonOpts )

      fileType = fileType.split()[0]

      # Allow fileType to be of the form XfileType
      try:
        fileType = fileType.split( "." )[1]
      except IndexError, ex:
        gLogger.warn( str( self.__class__ ) + "writeJobOptions" + str( ex ) )

      string = self.__generatePoolBody( string, files, fileType, savePfn )

      string = self.__addEndcatalog( string, pythonOpts )

    if catalog != None:
      string += "FileCatalog().Catalogs += [ 'xmlcatalog_file:" + catalog + "' ]\n"

    if fd:
      fd.write( string )
      fd.close()
    else:
      return string

  #############################################################################
  @staticmethod
  def __getFileType( files ):
    """it returns the file type. We assume we did not miss the file types.
    """
    allfiletypes = [i['FileType'] for i in files.values()]
    result = allfiletypes.pop() if len( allfiletypes ) > 0 else ''
    return result

  #############################################################################
  def __addDatasetcreationmetadata( self, string, dataset, comment ):
    """it adds the metadata information about the dataset creation.
    """
    if dataset:
      string += "\n\n%s Extra information about the data processing phases:\n" % ( comment )
      retVal = self.db_.getStepsMetadata( dataset )
      if retVal['OK']:
        for record in retVal['Value']['Records']:
          string += "\n\n%s Processing Pass %s \n\n" % ( comment, record )
          for i in retVal['Value']['Records'][record]:
            string += "%s %s : %s \n" % ( comment, i[0], i[1] )
    return string

  #############################################################################
  @staticmethod
  def __addEventselector( string, pythonOpts ):
    """It adds the first line of the catalog
    """
    # Now write the event selector option
    if pythonOpts:
      string += "\nfrom Gaudi.Configuration import * \n"
      string += "\nEventSelector().Input   = [\n"
    else:
      string += "\nEventSelector.Input   = {\n"
    return string

  #############################################################################
  @staticmethod
  def __addEndcatalog( string, pythonOpts ):
    """it adds the end of the catalog
    """
    if pythonOpts:
      string += "]\n"
    else:
      string += "\n};\n"
    return string

  #############################################################################
  @staticmethod
  def __createRootformatstring( string, filesandformats ):
    """It generates the Root format option file.
    """
    string += "\nfrom Gaudi.Configuration import * "
    string += "\nfrom GaudiConf import IOHelper"
    ioCounter = 0
    fileformat = None
    for i in sorted( filesandformats.items() ):
      if ioCounter == 0:
        fileformat = i[1]
        string += "\nIOHelper('%s').inputFiles([" % ( fileformat )
        string += "'LFN:%s',\n" % ( i[0] )
        ioCounter += 1
      elif ioCounter > 0  and fileformat == i[1]:
        string += "'LFN:%s',\n" % ( i[0] )
      elif ioCounter > 0 and fileformat != i[1]:
        fileformat = i[1]
        string = string[:-2]
        if ioCounter == 1 :
          string += '\n], clear=True)\n'
          ioCounter += 1
        else:
          string += '\n])\n'
        string += "\nIOHelper('%s').inputFiles([" % ( fileformat )
        string += "'LFN:%s',\n" % ( i[0] )

    string = string[:-2]
    if ioCounter == 1 :
      string += '\n], clear=True)\n'
    else:
      string += '\n])\n'
    return string

  #############################################################################
  @staticmethod
  def __generatePoolBody( string, files, fileType, savePfn ):
    """it adds the lfns to the pool catalog
    """
    mdfTypes = ["RAW", "MDF"]
    etcTypes = ["SETC", "FETC", "ETC"]
    keys = files.keys()
    keys.sort()
    first = True
    for lfn in keys:
      filename = files[lfn]
      if not first:
        string += ",\n"
      first = False
      if savePfn:
        if fileType in mdfTypes:
          string += "\"   DATAFILE=\'" + savePfn[lfn]['turl'] + "' SVC='LHCb::MDFSelector'\""
        elif fileType in etcTypes:
          string += "\"   COLLECTION='TagCreator/1' DATAFILE=\'" + savePfn[lfn]['turl'] + "' TYP='POOL_ROOT'\""
        else:
          string += "\"   DATAFILE=\'" + savePfn[lfn]['turl'] + "' TYP='POOL_ROOTTREE' OPT='READ'\""
      else:
        if fileType in mdfTypes:
          string += "\"   DATAFILE='LFN:" + filename['FileName'] + "' SVC='LHCb::MDFSelector'\""
        elif fileType in etcTypes:
          string += "\"   COLLECTION='TagCreator/1' DATAFILE='LFN:" + filename['FileName'] + "' TYP='POOL_ROOT'\""
        else:
          string += "\"   DATAFILE='LFN:" + filename['FileName'] + "' TYP='POOL_ROOTTREE' OPT='READ'\""
    return string

  #############################################################################
  @staticmethod
  def __addGaudiheader( pythonOpts, evtTypes ):
    """it creates the header of the job option
    """

    string = ''
    if pythonOpts:
      comment = "#-- "
    else:
      comment = "//-- "

    string += comment + "GAUDI jobOptions generated on " + time.asctime() + "\n"
    string += comment + "Contains event types : \n"
    fileTypes = evtTypes.keys()
    fileTypes.sort()
    for filetype in fileTypes:
      string += comment + "  %8d - %d files - %d events - %.2f GBytes\n" % ( filetype,
                                                                            evtTypes[filetype][0],
                                                                            evtTypes[filetype][1],
                                                                            evtTypes[filetype][2] )
    return string, comment

  #############################################################################
  def __getFilesandFormats( self, savePfn, files ):
    """It returns the format of the lfns and false if the file format is not ROOT type.
    """
    filesandformats = {}
    rootFormat = True
    lfn = []
    if savePfn:  # we have to decide the file type version.
      # This variable contains the file type version, if it is empty I check in the bkk
      lfn = savePfn.keys()[0]
      for i in savePfn:
        if savePfn[i]['pfntype'].upper() == 'ROOT_ALL':
          rootFormat = False
        filesandformats[i] = savePfn[i]['pfntype']
    else:
      retVal = self.db_.getFileTypeVersion( files.keys() )
      if retVal['OK']:
        records = retVal['Value']
        for i in records:
          filesandformats[i] = records[i]
        if 'ROOT_All' in filesandformats.values():
          rootFormat = False
      else:
        return retVal
    return filesandformats, rootFormat, lfn

  #############################################################################
  @staticmethod
  def __createEventtypelist( files ):
    """It creates a dictionary which contains the event types and the size of the data set.
    """
    evtTypes = {}
    for i in files:
      lfn = files[i]
      filetype = int( lfn['EventType'] )
      stat = 0
      if lfn['EventStat'] != None:
        stat = int( lfn['EventStat'] )

      if not evtTypes.has_key( filetype ):
        evtTypes[filetype] = [0, 0, 0.]
      evtTypes[filetype][0] += 1
      evtTypes[filetype][1] += stat
      if files[i]['FileSize'] == None:
        evtTypes[filetype][2] += 0
      else:
        evtTypes[filetype][2] += int( lfn['FileSize'] ) / 1000000000.
    return evtTypes

  #############################################################################
  def getProcessingPassSteps( self, in_dict ):
    """steps for a given processing pass"""
    return self.db_.getProcessingPassSteps( in_dict )

  #############################################################################
  def getMoreProductionInformations( self, prodid ):
    """production statistics"""
    return self.db_.getMoreProductionInformations( prodid )

  #############################################################################
  def getAvailableProductions( self ):
    """available productions"""
    return self.db_.getAvailableProductions()

  #############################################################################
  def getFileHistory( self, lfn ):
    """history"""
    return self.db_.getFileHistory( lfn )

  #############################################################################
  def getProductionProcessingPassSteps( self, in_dict ):
    """step of a production"""
    return self.db_.getProductionProcessingPassSteps( in_dict )

  #############################################################################
  def getAvailableDataQuality( self ):
    """ all the existing data qualities"""
    return self.db_.getAvailableDataQuality()

  #############################################################################
  def setDataQualities( self, values ):
    """setting data quality"""
    self.dataQualities_ = values

  def __getSelectedQualities( self ):
    """data quality"""
    res = []
    for i in self.dataQualities_:
      if self.dataQualities_[i] == True:
        res += [i]
    return res

  #############################################################################
  def getStepsMetadata( self, bkDict ):
    """
    It is a wrapper to the bookkeeping client
    """
    return self.db_.getStepsMetadata( bkDict )
