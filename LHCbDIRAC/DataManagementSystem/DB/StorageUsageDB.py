#####################################################################
# $HeadURL $
# File: StorageUsageDB.py
########################################################################
""" :mod: StorageUsageDB 
    ====================
 
    .. module: StorageUsageDB
    :synopsis: StorageUsageDB class is a front-end to the Storage Usage Database.
"""
## imports
from types import StringType
## from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

__RCSID__ = "$Id$"

#############################################################################
class StorageUsageDB( DB ):
  """ .. class:: StorageUsageDB
  """

  def __init__( self, maxQueueSize = 10 ):
    """ Standard Constructor
    """
    DB.__init__( self, 'StorageUsageDB', 'DataManagement/StorageUsageDB', maxQueueSize )
    self.__initializeDB()

  def __initializeDB( self ):
    """
    Create the tables
    """
    result = self._query( "show tables" )
    if not result[ 'OK' ]:
      return result
    tablesInDB = [ t[0] for t in result[ 'Value' ] ]
    tablesToCreate = {}
    self.__tablesDesc = {}
    self.__tablesDesc[ 'su_Directory' ] = { 'Fields' : { 'DID' : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                           'Path' : 'VARCHAR(255) NOT NULL',
                                                           'Files' : 'INTEGER UNSIGNED NOT NULL',
                                                           'Size' : 'BIGINT UNSIGNED NOT NULL',
                                                           },
                                             'PrimaryKey' : 'Path',
                                             'Indexes': { 'id': [ 'dID' ] }
                                            }

    self.__tablesDesc[ 'su_SEUsage' ] = { 'Fields' : { 'DID' : 'INTEGER UNSIGNED NOT NULL',
                                                       'SEName' : 'VARCHAR(32) NOT NULL',
                                                       'Files' : 'INTEGER UNSIGNED NOT NULL',
                                                       'Size' : 'BIGINT UNSIGNED NOT NULL',
                                                       'Updated' : 'DATETIME NOT NULL'
                                                  },
                                       'PrimaryKey' : [ 'DID', 'SEName' ],
                                       'Indexes': { 'SE': [ 'SEName' ] },
                                     }

    self.__tablesDesc[ 'se_Usage' ] = { 'Fields': { 'DID' : 'INTEGER UNSIGNED NOT NULL',
                                                       'SEName' : 'VARCHAR(32) NOT NULL',
                                                       'Files' : 'INTEGER UNSIGNED NOT NULL',
                                                       'Size' : 'BIGINT UNSIGNED NOT NULL',
                                                       'Updated' : 'DATETIME NOT NULL'
                                                   },
                                        'PrimaryKey' : [ 'DID', 'SEName' ],
                                        }
    self.__tablesDesc[ 'se_STSummary' ] = { 'Fields': {  'Site' : 'VARCHAR(32) NOT NULL',
                                                         'SpaceToken' :  'VARCHAR(32) NOT NULL',
                                                         'TotalSize' : 'BIGINT UNSIGNED NOT NULL',
                                                         'TotalFiles' : 'INTEGER UNSIGNED NOT NULL',
                                                         'Updated' : 'DATETIME NOT NULL'
                                                      },
                                          'PrimaryKey' : [ 'Site', 'SpaceToken' ],
                                          }
    self.__tablesDesc[ 'problematicDirs' ] = { 'Fields': { 'DID' : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                       'Path' : 'VARCHAR(255) NOT NULL',
                                                       'Site' : 'VARCHAR(32) NOT NULL',
                                                       'SpaceToken' : 'VARCHAR(32) NOT NULL',
                                                       'ReplicaType' : 'VARCHAR(32) NOT NULL',
                                                       'SEFiles' : 'INTEGER UNSIGNED NOT NULL',
                                                       'LFCFiles' : 'INTEGER UNSIGNED NOT NULL',
                                                       'SESize' : 'BIGINT UNSIGNED NOT NULL',
                                                       'LFCSize' : 'BIGINT UNSIGNED NOT NULL',
                                                       'Problem' : 'VARCHAR(255) NOT NULL',
                                                       'Updated' : 'DATETIME NOT NULL'
                                                   },
                                        'PrimaryKey' : [ 'DID'],
                                        }
    self.__tablesDesc[ 'Popularity' ] = { 'Fields' : { 'ID' : 'INTEGER UNSIGNED AUTO_INCREMENT NOT NULL',
                                                       'Path' : 'VARCHAR(255) NOT NULL',
                                                       'Site' : 'VARCHAR(32) NOT NULL',
                                                       'Count' : 'INTEGER UNSIGNED NOT NULL',
                                                       'InsertTime' : 'DATETIME NOT NULL',
                                                       'Status' : 'VARCHAR(32) NOT NULL'
                                                  },
                                        'PrimaryKey' : [ 'ID'],
                                     }
    self.__tablesDesc[ 'DirMetadata' ] = { 'Fields' : { 'DID' : 'INTEGER UNSIGNED NOT NULL',
                                                       'ConfigName' : 'VARCHAR(64) NOT NULL',
                                                       'ConfigVersion' : 'VARCHAR(64) NOT NULL',
                                                       'Conditions' : 'VARCHAR(64) NOT NULL',
                                                       'ProcessingPass' : 'VARCHAR(255) NOT NULL',
                                                       'EventType' : 'VARCHAR(255) NOT NULL',
                                                       'FileType' : 'VARCHAR(64) NOT NULL',
                                                       'Production' : 'VARCHAR(64) NOT NULL'
                                                  },
                                       'PrimaryKey' : [ 'DID'],
                                     }

    for tableName in self.__tablesDesc:
      if not tableName in tablesInDB:
        tablesToCreate[ tableName ] = self.__tablesDesc[ tableName ]
    return self._createTables( tablesToCreate )

  ################
  # Bulk insertion
  ################

  def publishDirectories( self, directoryDict ):
    """ Inserts a group of directories with their usage """
    self.log.info( "in publishDirectories: directoryDict is: \n %s" % ( directoryDict ) )

    if not directoryDict:
      return S_OK()
    result = self.__getIDs( directoryDict )
    if not result[ 'OK' ]:
      return result
    dirIDs = result[ 'Value' ]
    for dirPath in directoryDict:
      try:
        files = long( directoryDict[ dirPath ][ 'Files' ] )
        size = long( directoryDict[ dirPath ][ 'TotalSize' ] )
      except ValueError, e:
        return S_ERROR( "Values must be ints: %s" % str( e ) )
      SEUsage = directoryDict[ dirPath ][ 'SEUsage' ]
      if dirPath[-1] != "/":
        dirPath = "%s/" % dirPath
      sqlDirPath = self._escapeString( dirPath )[ 'Value' ]
      if dirPath in dirIDs:
        sqlCmd = "UPDATE `su_Directory` SET Files=%d, Size=%d WHERE Path = %s" % ( files, size, sqlDirPath )
        result = self._update( sqlCmd )
        if not result[ 'OK' ]:
          return result
      else:
        sqlCmd = "INSERT INTO `su_Directory` (DID, Path, Files, Size) VALUES ( 0, %s, %d, %d )" % ( sqlDirPath, 
                                                                                                    files, size )
        result = self._update( sqlCmd )
        if not result[ 'OK' ]:
          self.log.error( "Cannot insert directory", "%s: %s" % ( dirPath, result[ 'Message' ] ) )
          continue
        dirIDs[ dirPath ] = result[ 'lastRowId' ]
      result = self.__updateSEUsage( dirIDs[ dirPath ], SEUsage )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  ####
  # Insert/update to problematicDirs
  ####
  def publishToProblematicDirs( self, directoryDict ):
    """ Publish an entry into the problematic data directory """
    for path in directoryDict.keys():
      spaceToken = directoryDict[ path ][ 'SpaceToken' ]
      problem = directoryDict[ path ][ 'Problem' ]
      site = directoryDict[ path ][ 'Site']
      replicaType = directoryDict[ path ]['ReplicaType']
      try:
        SEfiles = int( directoryDict[ path ][ 'Files' ] )
        SEsize = int( directoryDict[ path ][ 'Size'] )
        LFCfiles = int( directoryDict[ path ]['LFCFiles' ] )
        LFCsize = int( directoryDict[ path ]['LFCSize'] )
      except ValueError:
        return S_ERROR( "ERROR: Files and Size have to be integer!" )
    # check if the tuple (path,Site,SpaceToken) already exists in the table
      if path[-1] != "/":
        path = "%s/" % path
      sqlPath = self._escapeString( path )['Value']
      sqlSpaceToken = self._escapeString( spaceToken )['Value']
      sqlProblem = self._escapeString( problem )['Value']
      sqlSite = self._escapeString( site )['Value']
      sqlReplicaType = self._escapeString( replicaType )['Value']
      sqlCmd = "SELECT DID FROM problematicDirs WHERE Path=%s AND Site=%s AND SpaceToken=%s" % ( sqlPath, 
                                                                                                 sqlSite, 
                                                                                                 sqlSpaceToken )
      self.log.info( "sqlCmd: %s" % sqlCmd )
      result = self._query( sqlCmd )
      if not result[ 'OK' ]:
        self.log.error( "Failed to query problematicDirs" )
        return result
      DID = result['Value']
      if DID:
        # there is an entry for (path, Site, SpaceToken), make an update of the row
        sqlCmd = "UPDATE `problematicDirs` SET SEFiles=%d, SESize=%d, LFCFiles=%d, " \
            "LFCSize=%d, Problem=%s, ReplicaType=%s, Updated=UTC_TIMESTAMP() WHERE " \
            "Path = %s and Site = %s and SpaceToken=%s" % ( SEfiles, SEsize, LFCfiles, LFCsize, sqlProblem, 
                                                            sqlReplicaType, sqlPath, sqlSite, sqlSpaceToken )
        self.log.info( "sqlCmd: %s" % sqlCmd )
        result = self._update( sqlCmd )
        if not result[ 'OK' ]:
          self.log.error( "Cannot update row (%s, %s) in problematicDirs" % ( path, site ) )
          return result
      else:
        # entry is not there, make an insert of a new row
        sqlCmd = "INSERT INTO problematicDirs (Path, Site, SpaceToken, SEFiles, SESize, " \
            "LFCFiles, LFCSize, Problem, ReplicaType, Updated) VALUES " \
            "( %s, %s, %s, %d, %d, %d, %d, %s, %s, UTC_TIMESTAMP())" % ( sqlPath, sqlSite, sqlSpaceToken, 
                                                                         SEfiles, SEsize, LFCfiles, 
                                                                         LFCsize, sqlProblem, sqlReplicaType )
        self.log.info( "sqlCmd: %s" % sqlCmd )
        result = self._update( sqlCmd )
        if not result[ 'OK' ]:
          self.log.error( "Cannot insert row (%s, %s) in problematicDirs" % ( path, site ) )
          return result
    return S_OK()
  ###
  # Insert/Update to se_Usage table
  ###
  def publishToSEReplicas( self , directoryDict ):
    """ Publish an entry to se_Usage table """
    for path in directoryDict.keys():
      SeName = directoryDict[ path ][ 'SEName']
      try:
        files = int( directoryDict[ path ][ 'Files' ] )
        size = int( directoryDict[ path ][ 'Size'] )
      except ValueError:
        return S_ERROR( "ERROR: Files and Size have to be integer!" )
      # check if the tuple (path,SE) already exists in the table
      if path[-1] != "/":
        path = "%s/" % path
      sqlPath = self._escapeString( path )['Value']
      sqlSeName = self._escapeString( SeName )['Value']
      sqlCmd = "SELECT d.DID FROM su_Directory AS d, se_Usage AS u WHERE " \
          "d.DID=u.DID AND d.Path=%s AND u.SEName=%s" % ( sqlPath, sqlSeName )
      self.log.debug( "sqlCmd: %s" % sqlCmd )
      result = self._query( sqlCmd )
      if not result[ 'OK' ]:
        self.log.error( "Failed to query se_Usage" )
        return result
      sqlRes = result['Value']
      if sqlRes:
        try:
          DID = int( sqlRes[0][0] )
        except  ValueError:
          return S_ERROR( "ERROR: DID should be integer!" )
        # there is an entry for (path, SEname), make an update of the row
        sqlCmd = "UPDATE `se_Usage` SET Files=%d, Size=%d, Updated=UTC_TIMESTAMP() WHERE " \
            "DID=%d AND SEName=%s" % ( files, size, DID, sqlSeName )
        self.log.info( "sqlCmd: %s" % sqlCmd )
        result = self._update( sqlCmd )
        if not result[ 'OK' ]:
          self.log.error( "Cannot update row (%s, %s) in se_Usage" % ( path, SeName ) )
          return result
      else:
        # entry is not there, make an insert of a new row
        # get the DID corresponding to this Path, from su_Directory table:
        result = self.__getIDs( directoryDict )
        if not result[ 'OK' ]:
          self.log.error( "Cannot getIds for directory %s" % ( path ) )
          return result
        for dir in result[ 'Value' ].keys():
          try:
            DID = int( result[ 'Value' ][ dir ] )
          except ValueError:
            return S_ERROR( "ERROR: DID should be integer!" )
        sqlCmd = "INSERT INTO se_Usage (DID, SEName, Files, Size, Updated) VALUES " \
            " ( %d, %s, %d, %d, UTC_TIMESTAMP())" % ( DID, sqlSeName, files, size )
        self.log.info( "sqlCmd: %s" % sqlCmd )
        result = self._update( sqlCmd )
        if not result[ 'OK' ]:
          self.log.error( "Cannot insert row (%s, %s) in se_Usage" % ( path, SeName ) )
          return result
    return S_OK()

  def getIDs( self, dirList ):
    """ get IDs for list of directories """
    return self.__getIDs( dirList )

  def __getIDs( self, dirList ):
    dl = []
    for dirPath in dirList:
      if dirPath[-1] != "/":
        dirPath = "%s/" % dirPath
      dl.append( self._escapeString( dirPath )[ 'Value' ] )
    sqlCmd = "SELECT Path, DID FROM `su_Directory` WHERE Path in ( %s )" % ", ".join( dl )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    return S_OK( dict( result[ 'Value' ] ) )

  def __getIDsFromProblematicDirs( self, dirList ):
    """ Get the ID from the problematicDirs table, for a given tuple (Path,SE) """
    self.log.info( "entry to be removed: %s" % dirList )
    dirPath = dirList.keys()[ 0 ]
    if dirPath[-1] != "/":
      dirPath = "%s/" % dirPath
    #spaceToken = dirList[ dirPath ][ 'SEName' ]
    spaceToken = dirList[ dirPath ][ 'SpaceToken' ]
    sqlSpaceToken = self._escapeString( spaceToken )[ 'Value' ]
    sqlPath = self._escapeString( dirPath )[ 'Value' ]
    site = dirList[ dirPath ]['Site']
    sqlSite = self._escapeString( site )[ 'Value' ]
    sqlCmd = "SELECT Path, DID FROM problematicDirs WHERE Path=%s AND Site=%s and SpaceToken=%s" % ( sqlPath, 
                                                                                                     sqlSite, 
                                                                                                     sqlSpaceToken )
    self.log.info( "in __getIDsFromProblematicDirs query %s" % sqlCmd )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    return S_OK( dict( result[ 'Value' ] ) )

  def __getIDsFromSe_Usage( self, dirList ):
    """Get the ID of the entry corresponding to the tuple (path, SE) """
    dirPath = dirList.keys()[ 0 ]
    if dirPath[-1] != "/":
      dirPath = "%s/" % dirPath

    # take into account that SEName might not be available
    SE = ''
    site = ''
    try:
      SE = dirList[ dirPath ][ 'SEName' ]
    except KeyError:
      self.log.info( "SEName attribute not available!" )
    if not SE:
      self.log.info( "The SEName attribute is not available. Try to get the Site attribute.." )
      try:
        site = dirList[ dirPath ]['Site']
      except KeyError:
        self.log.info( "Site attribute not available!" )
    if SE:
      sqlSE = self._escapeString( SE )[ 'Value' ]
    else:
      sqlSE = ''
    if site:
      sqlSite = self._escapeString( site )[ 'Value' ]
    else:
      sqlSite = ''
    sqlPath = self._escapeString( dirPath )[ 'Value' ]
    if sqlSE:
      sqlCmd = "SELECT d.Path, d.DID FROM se_Usage r, su_Directory d WHERE d.DID=r.DID \
      ND d.Path=%s AND r.SEName=%s" % ( sqlPath, sqlSE )
    elif sqlSite:
      sqlCmd = "SELECT d.Path, d.DID FROM se_Usage r, su_Directory d WHERE d.DID=r.DID \
      AND d.Path=%s AND r.SEName LIKE '%s%%'" % ( sqlPath, sqlSite )
    else:
      self.log.error( "Not enough information to formulate the query: either Site or SEName should be provided " )
      return S_ERROR()
    self.log.info( "in __getIDsFromSe_Usage query %s" % sqlCmd )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    return S_OK( dict( result[ 'Value' ] ) )

  def __updateSEUsage( self, dirID, SEUsage ):
    sqlCmd = "DELETE FROM `su_SEUsage` WHERE DID=%d" % dirID
    result = self._update( sqlCmd )
    if not result[ 'OK' ]:
      return result
    if not SEUsage:
      self.log.error( "Ooops. Dir has no SEUsage!", "ID %s" % dirID )
      return S_OK()
    sqlVals = []
    #HACK: Make sure SEName makes sense
    fixedSEUsage = {}
    for SEName in SEUsage:
      if SEName == "CERN-Tape":
        SEName = "CERN-tape"
      if not SEName in fixedSEUsage:
        fixedSEUsage[ SEName ] = { 'Size' : 0, 'Files' : 0 }
      fixedSEUsage[ SEName ][ 'Size' ] += SEUsage[ SEName ][ 'Size' ]
      fixedSEUsage[ SEName ][ 'Files' ] += SEUsage[ SEName ][ 'Files' ]
    if fixedSEUsage != SEUsage:
      self.log.warn( "Fixed dirID %s SEUsage from:\n %s\nto:\n %s" % ( dirID, SEUsage, fixedSEUsage ) )
    SEUsage = fixedSEUsage
    #Insert data
    for SEName in SEUsage:
      try:
        size = SEUsage[ SEName ][ 'Size' ]
        files = SEUsage[ SEName ][ 'Files' ]
      except ValueError, e:
        return S_ERROR( "Values must be ints: %s" % str( e ) )
      sqlVals.append( "( %d, %s, %d, %d, UTC_TIMESTAMP() )" % ( dirID, self._escapeString( SEName )[ 'Value' ], 
                                                                size, files ) )
    sqlIn = "INSERT INTO `su_SEUsage` ( DID, SEname, Size, Files, Updated ) VALUES %s" % ", ".join( sqlVals )
    return self._update( sqlIn )

  #############
  # Remove dir
  #############

  def __getIDsLike( self, dirPath ):
    dirPath = self._escapeString( dirPath )[ 'Value' ][1:-1]
    while dirPath and dirPath[-1] == "/":
      dirPath = dirPath[:-1]
    sqlCmd = "SELECT Path, DID FROM `su_Directory` WHERE Path LIKE '%s/%%'" % dirPath
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    return S_OK( dict( result[ 'Value' ] ) )

  def removeDirectory( self, dirPath ):
    result = self.__getIDsLike( dirPath )
    if not result[ 'OK' ]:
      return result
    dirIDs = result[ 'Value' ]
    if not dirIDs:
      return S_OK()
    sqlIDs = ", ".join( [ str( dirIDs[ path ] ) for path in dirIDs ] )
    for tableName in ( 'su_Directory', 'su_SEUsage' ):
      sqlCmd = "DELETE FROM `%s` WHERE DID in ( %s )" % ( tableName, sqlIDs )
      result = self._update( sqlCmd )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  def removeDirFromSe_Usage( self, dirDict ):
    """ 
      Remove the entry corresponding to the tuple (path, SE) from the se_Usage table.
       This function is typically called when a directory is found to be a problematic directory, 
       and before inserting it into the problematicDirs
       table, it is necessary to remove it from the se_Usage table, if it exists there. 
       In general, one same directory can only exist into either the
       se_Usage or the problematicDirs table. 
     """
    deletedDirs = 0
    result = self.__getIDsFromSe_Usage( dirDict )
    if not result[ 'OK' ]:
      return result
    dirIDs = result[ 'Value' ]
    if not dirIDs:
      return S_OK( deletedDirs )
    deletedDirs = len( dirIDs )
    if deletedDirs > 1:
      return S_ERROR( "There should not be more than 1 match for a tuple (path,SE)!!" )
    sqlIDs = ", ".join( [ str( dirIDs[ path ] ) for path in dirIDs ] )
    sqlCmd = "DELETE FROM se_Usage WHERE DID in ( %s )" % ( sqlIDs )
    result = self._update( sqlCmd )
    if not result[ 'OK' ]:
      return result
    return S_OK( deletedDirs )

  def removeDirFromProblematicDirs( self, dirDict ):
    """ 
    Remove an entry from the problematicDirs table. 
    This is typically used when a directory is found to be correctly registered in the FC
    and it was previously inserted in the problematic data table: 
    before inserting the entry into the se_Usage table, it has to be removed
    from the problematicDirs 
    """
    deletedDirs = 0
    result = self.__getIDsFromProblematicDirs( dirDict )
    if not result[ 'OK' ]:
      return result
    self.log.info( "sqlCmd result for __getIDsFromProblematicDirs is: %s" % result )
    dirIDs = result[ 'Value' ]
    if not dirIDs:
      self.log.info( "No directory to remove" )
      return S_OK( deletedDirs )
    deletedDirs = len( dirIDs )
    if deletedDirs > 1:
      return S_ERROR( "There should not be more than 1 match for a tuple (path,SE)!!" )
    self.log.info( "remove directories: %s" % dirIDs )
    sqlIDs = ", ".join( [ str( dirIDs[ path ] ) for path in dirIDs ] )
    sqlCmd = "DELETE FROM problematicDirs WHERE DID in ( %s )" % ( sqlIDs )
    result = self._update( sqlCmd )
    if not result[ 'OK' ]:
      return result
    self.log.info( "sqlCmd result for DELETE is: %s" % result )
    return S_OK( deletedDirs )

  def removeAllFromProblematicDirs( self, site = False ):
    """ Remove all entries from the problematicDirs table, for a given site (optional).
    """
    if site:
      sqlSite = self._escapeString( site )['Value']
      sqlCond = [ "Site=%s" % sqlSite ]
      sqlCmd = "DELETE FROM problematicDirs WHERE Site=%s" % sqlSite
    else:
      sqlCmd = "DELETE FROM problematicDirs"
    deletedDirs = 0
    self.log.info( "in removeAllFromProblematicDirs command: %s" % sqlCmd )
    result = self._update( sqlCmd )
    if not result[ 'OK' ]:
      return result
    self.log.info( "sqlCmd result for DELETE is: %s" % result )
    deletedDirs = result['Value']
    return S_OK( deletedDirs )

  ################
  # Clean outdated entries
  ################

  def purgeOutdatedEntries( self, rootDir = False, outdatedSeconds = 86400, preserveDirsList = None ):
    preserveDirsList = preserveDirsList if preserveDirsList else []
    try:
      outdatedSeconds = max( 1, long( outdatedSeconds ) )
    except ValueError:
      return S_ERROR( "Ooutdated seconds needs to be a number" )

    sqlCond = [ "TIMESTAMPDIFF( SECOND, su.Updated, UTC_TIMESTAMP() ) > %d" % outdatedSeconds ]
    sqlTables = [ "`su_SEUsage` as su" ]
    sqlLimit = 1000
    if rootDir:
      rootDir = self._escapeString( rootDir )[ 'Value' ][1:-1]
      while rootDir[-1] == "/":
        rootDir = rootDir[:-1]
      sqlTables.append( "`su_Directory` as d" )
      sqlCond.append( "d.Path LIKE '%s/%%'" % rootDir )
      sqlCond.append( "d.DID = su.DID" )
    if preserveDirsList:
      for ignoreDir in preserveDirsList:
        ignoreDir = self._escapeString( ignoreDir )[ 'Value' ][1:-1]
        while ignoreDir[-1] == "/":
          ignoreDir = ignoreDir[:-1]
        sqlCond.append( "d.Path NOT LIKE '%s/%%'" % ignoreDir )
    sqlCmd = "SELECT DISTINCT su.DID FROM %s WHERE %s LIMIT %d" % ( ", ".join( sqlTables ), 
                                                                    " AND ".join( sqlCond ), 
                                                                    sqlLimit )
    cleaned = 0
    while True:
      result = self._query( sqlCmd )
      if not result[ 'OK' ]:
        return result
      idList = [ str( row[0] ) for row in result[ 'Value' ] ]
      if not idList:
        break
      cleaned += len( idList )
      sqlIdList = ", ".join( idList )
      for tblName in ( 'su_Directory', 'su_SEUsage' ):
        result = self._update( "DELETE FROM `%s` WHERE DID IN ( %s )" % ( tblName, sqlIdList ) )
        if not result[ 'OK' ]:
          return result
      if len( idList ) < sqlLimit:
        break
    return S_OK( cleaned )

  ###############
  #  Get Storage info
  ###############

  def getStorageElementSelection( self ):
    """ Retireve the possible selections available through the web-monitor
    """
    sqlCmd = "SELECT DISTINCT SEName FROM `su_SEUsage` ORDER BY SEName"
    result = self._query( sqlCmd )
    if not result['OK']:
      return result
    return S_OK( [ row[0] for row in result[ 'Value' ] ] )

  def __getStorageCond( self, path, fileType, production, SEs ):
    path = self._escapeString( path )[ 'Value' ][1:-1]
    while path and path[-1] == "/":
      path = path[:-1]
    sqlCond = [ "d.DID = su.DID", "d.Path LIKE '%s/%%'" % path ]
    if fileType:
      fileType = self._escapeString( fileType )[ 'Value' ][1:-1]
      sqlCond.append( "d.Path LIKE '%%/%s%%'" % fileType )
    if production:
      try:
        sqlCond.append( "d.Path LIKE '%%/%08.d%%'" % long( production ) )
      except ValueError:
        return S_ERROR( "production has to be a number" )
    if SEs:
      sqlCond.append( "su.SEName in ( %s )" % ", ".join( [ self._escapeString( SEName )[ 'Value' ] 
                                                           for SEName in SEs ] ) )
    return sqlCond

  def __getStorageSummary( self, path , fileType = False, production = False, SEs = [], groupingField = "su.SEName" ):
    """ Retrieves the storage summary for all of the known directories
    """
    sqlCond = self.__getStorageCond( path, fileType, production, SEs )
    sqlFields = ( groupingField, "SUM(su.Size)", "SUM(su.Files)" )
    sqlCmd = "SELECT %s FROM `su_SEUsage` as su,`su_Directory` as d WHERE %s GROUP BY %s" % ( ", ".join( sqlFields ),
                                                                                              " AND ".join( sqlCond ),
                                                                                              groupingField )
    result = self._query( sqlCmd )
    if not result['OK']:
      return result
    usageDict = {}
    for gf, size, files in result['Value']:
      usageDict[ gf ] = { 'Size' : long( size ), 'Files' : long( files ) }
    return S_OK( usageDict )

  def getStorageSummary( self, path, fileType = False, production = False, SEs = [] ):
    return self.__getStorageSummary( path, fileType, production, SEs, "su.SEName" )

  def getStorageDirectorySummary( self, path, fileType = False, production = False, SEs = [] ):
    return self.__getStorageSummary( path, fileType, production, SEs, "d.Path" )

  def getStorageDirectories( self, path, fileType = False, production = False, SEs = [] ):
    sqlCond = self.__getStorageCond( path, fileType, production, SEs )
    sqlCmd = "SELECT DISTINCT d.Path FROM `su_SEUsage` as su,`su_Directory` as d WHERE %s" % ( " AND ".join( sqlCond ) )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    return S_OK( [ row[0] for row in result[ 'Value' ] ] )

  @staticmethod
  def __userExpression( fieldName = "d.Path" ):
    return "SUBSTRING_INDEX( SUBSTRING_INDEX( %s, '/', 5 ), '/', -1 )" % fieldName

  def getUserStorageUsage( self, userName = False ):
    """ Get the usage in the SEs
    """
    sqlCond = [ "d.DID = su.DID" ]
    if userName:
      userName = self._escapeString( userName )[ 'Value' ][1:-1]
      sqlCond.append( "d.Path LIKE '/lhcb/user/%s/%s/%%" % ( userName[0], userName ) )
    else:
      sqlCond.append( "d.Path LIKE '/lhcb/user/_/%'" )
    sqlCmd = "SELECT %s, SUM( su.Size ) FROM `su_Directory` AS d, `su_SEUsage` AS su " \
        "WHERE %s GROUP BY %s" % ( self.__userExpression(), " AND ".join( sqlCond ), self.__userExpression() )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    userStorage = {}
    for row in result[ 'Value' ]:
      userStorage[ row[0] ] = long( row[1] )
    return S_OK( userStorage )

  def getUserSummaryPerSE( self, userName = False ):
    sqlUser = self.__userExpression()
    sqlFields = ( sqlUser, "su.SEName", "SUM(su.Size)", "SUM(su.Files)" )
    sqlCond = [ "d.DID = su.DID", "d.Path LIKE '/lhcb/user/_/%'" ]
    if userName:
      sqlCond.append( "%s = %s" % ( sqlUser, self._escapeString( userName )[ 'Value' ] ) )
    sqlGroup = ( sqlUser, "su.SEName" )
    sqlCmd = "SELECT % s FROM `su_Directory` AS d, `su_SEUsage` AS su WHERE %s GROUP BY %s" % ( ", ".join( sqlFields ),
                                                                                                " AND ".join( sqlCond ),
                                                                                                ", ".join( sqlGroup ) )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    userData = {}
    for row in result[ 'Value' ]:
      userName = row[ 0 ]
      seName = row[1]
      if userName not in userData:
        userData[ userName ] = {}
      userData[ userName ][ seName ] = { 'Size' : long( row[2] ), 'Files' : long( row[3] ) }
    return S_OK( userData )


  def getDirectorySummaryPerSE( self, directory ):
    """ Queries the DB and get a summary (total size and files) for the given directory """
    sqlCmd = "SELECT su.SEName, SUM(su.Size), SUM(su.Files) FROM su_Directory AS d, su_SEUsage AS su " \
        " WHERE d.DID=su.DID AND d.Path LIKE '%s%%' GROUP BY su.SEName" % ( directory )
    gLogger.verbose( "getDirectorySummaryPerSE: sqlCmd is %s " % sqlCmd )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return result
    Data = {}
    for row in result[ 'Value' ]:
      seName = row[ 0 ]
      if seName not in Data.keys():
        Data[ seName ] = {}
      Data[ seName ] = { 'Size' : long( row[1] ), 'Files' : long( row[2] ) }
    return S_OK( Data )

  def publishTose_STSummary( self, site, spaceToken, totalSize, totalFiles ):
    """ Publish total size and total files extracted from the storage
        dumps to the se_STSummary """
    try:
      sqlTotalSize = long( totalSize )
      sqlTotalFiles = long( totalFiles )
    except ValueError, e:
      return S_ERROR( "Values must be ints: %s" % str( e ) )
    sqlSpaceToken = self._escapeString( spaceToken )['Value']
    sqlSite = self._escapeString( site )['Value']
    # check if there is already an entry for the tuple (site, spaceTok)
    res = self.__getSTSummary( site, spaceToken )
    gLogger.info( " __getSTSummary returned: %s " % res )
    if not res[ 'OK' ]:
      return S_ERROR( res )
    if not res[ 'Value' ]:
      gLogger.warn( "Entry for site=%s, spaceToken=%s is not there => insert new entry " % ( site, spaceToken ) )
      sqlCmd = "INSERT INTO `se_STSummary` (Site, SpaceToken, TotalSize, TotalFiles, Updated) VALUES " \
          "( %s, %s, %d, %d, UTC_TIMESTAMP())" % ( sqlSite, sqlSpaceToken, sqlTotalSize, sqlTotalFiles )

    else:
      gLogger.info( "Entry for site=%s, spaceToken=%s is there => update entry " % ( site, spaceToken ) )
      sqlCmd = "UPDATE `se_STSummary` SET TotalSize=%d, TotalFiles=%d, Updated=UTC_TIMESTAMP() WHERE " \
          "Site=%s and SpaceToken=%s" % ( sqlTotalSize, sqlTotalFiles, sqlSite, sqlSpaceToken )

    gLogger.info( " publishTose_STSummary sqlCmd: %s " % sqlCmd )
    result = self._update( sqlCmd )
    if not result[ 'OK' ]:
      gLogger.error( "Cannot insert entry", "%s,%s: %s" % ( sqlSite, sqlSpaceToken, result[ 'Message' ] ) )
      return result
    return S_OK()

  def __getSTSummary( self, site, spaceToken = False ):
    """ Get total files and total size for the space token identified by the input arguments
         (site, space token) , if space token is not specified, return all entries relative
         to the site. """

    sqlSite = self._escapeString( site )['Value']
    sqlCond = [ "Site=%s" % sqlSite ]
    if spaceToken:
      sqlSpaceToken = self._escapeString( spaceToken )['Value']
      sqlCond.append( "SpaceToken=%s" % sqlSpaceToken )

    sqlCmd = "SELECT Site, SpaceToken, TotalSize, TotalFiles, Updated FROM `se_STSummary` WHERE %s" % \
        " AND ".join( sqlCond ) 
    gLogger.info( " __getSTSummary sqlCmd is: %s " % sqlCmd )
    result = self._query( sqlCmd )
    gLogger.info( " __getSTSummary result: %s " % result )
    if not result[ 'OK' ]:
      return S_ERROR( result )
    return S_OK( result[ 'Value' ] )


  def getSTSummary( self, site, spaceToken = False ):
    """Returns a summary of space usage for the given site, on the basis of the information 
       provided by the SRM db dumps from sites """
    return self.__getSTSummary( site, spaceToken )

  def removeSTSummary( self, site, spaceToken = False ):
    """ Remove from se_STSummary table the entry relative to the given site and space token (if specified). 
    If no space token is specified, remove all entries relative to site
    """
    sqlSite = self._escapeString( site )['Value']
    sqlCond = [ "Site=%s" % sqlSite ]
    if spaceToken:
      sqlSpaceToken = self._escapeString( spaceToken )['Value']
      sqlCond.append( "SpaceToken=%s" % sqlSpaceToken )

    sqlCmd = "DELETE FROM `se_STSummary` WHERE %s" % ( " AND ".join( sqlCond ) )
    gLogger.info( " __getSTSummary sqlCmd is: %s " % sqlCmd )
    result = self._update( sqlCmd )
    gLogger.info( " __getSTSummary result: %s " % result )
    if not result[ 'OK' ]:
      return S_ERROR( result )
    return S_OK( result[ 'Value' ] )

  def getProblematicDirsSummary( self, site, problem = False ):
    """ Get a summary of problematic directories for a given site and given problem (optional)
    """
    sqlSite = self._escapeString( site )['Value']
    sqlCond = [ "Site=%s" % sqlSite ]
    if problem:
      sqlProblem = self._escapeString( problem )['Value']
      sqlCond.append( "Problem=%s" % sqlProblem )

    sqlCmd = "SELECT Site, SpaceToken, LFCFiles, SEFiles, Path, Problem, ReplicaType FROM `problematicDirs` " \
        "WHERE %s" % ( " AND ".join( sqlCond ) )
    gLogger.info( " getProblematicDirsSummary sqlCmd is: %s " % sqlCmd )
    result = self._query( sqlCmd )
    gLogger.info( " getProblematicDirsSummary result: %s " % result )
    if not result[ 'OK' ]:
      return S_ERROR( result['Message'] )
    return S_OK( result[ 'Value' ] )


  def getRunSummaryPerSE( self, run ):
    """ Queries the DB and get a summary (total size and files) per SE  for the given run. 
    It assumes that the path in the LFC where the run's file are stored is like:  
    /lhcb/data/[YEAR]/RAW/[STREAM]/[PARTITION]/[ACTIVITY]/[RUNNO]/"""
    # try and implement bulk query
    # check the type of run
    Data = {}
    if type( run ) == type( [] ):
      for thisRun in run:
        sqlCmd = "SELECT su.SEName, SUM(su.Size), SUM(su.Files)  FROM su_Directory AS d, su_SEUsage AS su WHERE " \
            "d.DID=su.DID AND d.Path LIKE '/lhcb/data/%%/RAW/%%/%%/%%/%d/' GROUP BY su.SEName" % ( thisRun )
        gLogger.info( "getRunSummaryPerSE: sqlCmd is %s " % sqlCmd )
        result = self._query( sqlCmd )
        if not result[ 'OK' ]:
          return S_ERROR( result )
        Data[ thisRun ] = {}
        for row in result[ 'Value' ]:
          seName = row[ 0 ]
          if seName not in Data[ thisRun ].keys():
            Data[ thisRun ][ seName ] = {}
          Data[ thisRun ][ seName ] = { 'Size' : long( row[1] ), 'Files' : long( row[2] ) }
    else:
      sqlCmd = "SELECT su.SEName, SUM(su.Size), SUM(su.Files)  FROM su_Directory AS d, su_SEUsage AS su WHERE " \
          "d.DID=su.DID and d.Path LIKE '/lhcb/data/%%/RAW/%%/%%/%%/%d/' GROUP BY su.SEName" % ( run )
      gLogger.info( "getRunSummaryPerSE: sqlCmd is %s " % sqlCmd )
      result = self._query( sqlCmd )
      if not result[ 'OK' ]:
        return S_ERROR( result )
      for row in result[ 'Value' ]:
        seName = row[ 0 ]
        if seName not in Data.keys():
          Data[ seName ] = {}
        Data[ seName ] = { 'Size' : long( row[1] ), 'Files' : long( row[2] ) }

    return S_OK( Data )

  def __getAllReplicasInFC( self, path ):
    ''' Queries the su_seUsage table to get all the entries relative to a given path registered in the FC. Returns
     for every replica the SE, the update, the files and the total size '''
    if path[-1] != "/":
      path = "%s/" % path
    sqlCmd = "SELECT DID FROM su_Directory where Path like '%s%%'" % ( path )
    result = self._query( sqlCmd )
    if not result['OK']:
      return result
    if not result['Value']: # no replica found
      return S_OK( result )

    for row in result['Value']:
      did = row[ 0 ]
      # warning! this has to be corrected! there is no need to make a loop! the query should return only one row
      # add a check for that. And return an error if it return more than one row

    sqlCmd = "SELECT Files, Updated, SEName, Size FROM su_SEUsage WHERE DID=%s" % ( did )
    result = self._query( sqlCmd )
    if not result['OK']:
      return result
    replicasData = {}
    replicasData[ path ] = {}
    for row in result['Value']:
      SEName = row[ 2 ]
      if SEName in replicasData[ path ].keys():
        return S_ERROR( "There cannot be two replicas on the same SE!" )
      replicasData[ path ][ SEName ] = {}
      replicasData[ path ][ SEName ][ 'Files' ] = row[0]
      replicasData[ path ][ SEName ][ 'Updated' ] = row[1]
      replicasData[ path ][ SEName ][ 'Size' ] = row[3]

    return S_OK( replicasData )

  def getAllReplicasInFC( self, path ):
    return self.__getAllReplicasInFC( path )

  ######
  # Catalog queries
  ######

  def __getCatalogCond( self, path, fileType, production ):
    path = self._escapeString( path )[ 'Value' ][1:-1]
    while path and path[-1] == "/":
      path = path[:-1]
    sqlCond = [ "Path LIKE '%s/%%'" % path ]
    if fileType:
      fileType = self._escapeString( fileType )[ 'Value' ][1:-1]
      sqlCond.append( "Path LIKE '%%/%s%%'" % fileType )
    if production:
      try:
        sqlCond.append( "Path LIKE '%%/%08.d%%'" % long( production ) )
      except ValueError:
        return S_ERROR( "production has to be a number" )
    return sqlCond

  def __getSummary( self, path, fileType = False, production = False, groupingField = "Path" ):
    """ Retrieves the storage summary for all of the known directories
    """
    sqlCond = self.__getCatalogCond( path, fileType, production )
    sqlFields = ( groupingField, "SUM(Size)", "SUM(Files)" )
    sqlCmd = "SELECT %s FROM `su_Directory` WHERE %s GROUP BY %s" % ( ", ".join( sqlFields ),
                                                                      " AND ".join( sqlCond ),
                                                                      groupingField )
    result = self._query( sqlCmd )
    if not result['OK']:
      return result
    usageDict = {}
    for gf, size, files in result['Value']:
      usageDict[ gf ] = { 'Size' : long( size ), 'Files' : long( files ) }
    return S_OK( usageDict )

  def getSummary( self, path, fileType = False, production = False ):
    return self.__getSummary( path, fileType, production, "Path" )

  def getUserSummary( self, userName = False ):
    if userName:
      userName = self._escapeString( userName )[ 'Value' ][1:-1]
      path = "/lhcb/user/%s/%s" % ( userName[0], userName )
    else:
      path = "/lhcb/user/_/%"
    return self.__getSummary( path, groupingField = self.__userExpression( "Path" ) )

  ######
  # methods to interact with Popularity table
  ######


  def sendDataUsageReport( self, site, directoryDict, status = 'New' ):
    """ Add a new trace in the Popularity table  """
    self.log.info( "in addPopCount: dirDict: %s site: %s" % ( directoryDict, site ) )
    if not directoryDict:
      return S_OK()
    sqlSite = self._escapeString( site )[ 'Value' ]
    insertedEntries = 0
    for d in directoryDict.keys():
      count = directoryDict[ d ]
      if d[-1] != "/":
        d = "%s/" % d
      sqlPath = self._escapeString( d )[ 'Value' ]
      sqlStatus = self._escapeString( status )[ 'Value' ]
      if type( count ) != int:
        self.log.warn( "in sendDataUsageReport: type is not correct %s" % count )
        continue
      sqlCmd = "INSERT INTO `Popularity` ( Path, Site, Count, Status, InsertTime) VALUES " \
          "( %s, %s, %d, %s, UTC_TIMESTAMP() )" % ( sqlPath, sqlSite, count, sqlStatus )
      self.log.info( "sqlCmd = %s " % sqlCmd )
      result = self._update( sqlCmd )
      if not result[ 'OK' ]:
        self.log.error( "Cannot insert entry: %s" % ( result[ 'Message' ] ) )
        continue
      insertedEntries += 1
    return S_OK( insertedEntries )

  def getDataUsageSummary( self, startTime, endTime, status = 'New' ):
    """ returns a summary of the counts for each tuple (Site,Path) in the given time interval
    """
    if startTime > endTime:
      return S_OK()
    if ( type( startTime ) != StringType or type( endTime ) != StringType ):
      return S_ERROR( 'wrong arguments format' )

    sqlStartTime = self._escapeString( startTime )[ 'Value' ]
    sqlEndTime = self._escapeString( endTime )[ 'Value' ]
    sqlStatus = self._escapeString( status )[ 'Value' ]
    sqlCmd = "SELECT ID, Path, Site, Count from `Popularity` WHERE " \
        "Status = %s AND InsertTime > %s AND InsertTime < %s " % ( sqlStatus, sqlStartTime, sqlEndTime )
    self.log.info( "sqlCmd = %s " % sqlCmd )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return S_ERROR( result['Message'] )
    return result


  def updatePopEntryStatus( self, IdList, newStatus ):
    """ Update the status of the entry identified by the IDList, to the status specified by Newstatus.
    """
    if not IdList:
      self.log.info( "updatePopEntryStatus: no entry to be updated" )
      return S_OK()

    sqlStatus = self._escapeString( newStatus )['Value']
    strIdList = [ str( id ) for id in IdList ]
    sqlIdList = ", ".join( strIdList )
    self.log.info( "updatePopEntryStatus: sqlIdList %s " % sqlIdList )
    sqlCmd = "UPDATE `Popularity` SET Status=%s WHERE ID IN ( %s )" % ( sqlStatus, sqlIdList )
    result = self._update( sqlCmd )
    if not result[ 'OK' ]:
      return result
    return result

  def insertToDirMetadata( self, directoryDict ):
    """ Inserts a new entry into the DirMetadata table. The input dictionary should contain, for each
        lfn directory, a dictionary with all the necessary metadata provided by the bookkeeping """

    if not directoryDict:
      return S_OK()
    # get the IDs from the su_Directory table:
    result = self.__getIDs( directoryDict )
    if not result[ 'OK' ]:
      return result
    dirIDs = result[ 'Value' ]
    if not dirIDs:
      errMsg = "ERROR! the directory %s was not found in the LFN directory table! " % directoryDict
      self.log.error( "%s" % errMsg )
      return S_ERROR( errMsg )
    self.log.info( "in insertToDirMetadata: dirIDs: %s" % ( dirIDs ) )
    # returns a dictionary of type:
    #  {dir1: ID1, dir2: ID2, ...} 
    insertedEntries = 0
    for d in directoryDict.keys():
      if d not in dirIDs.keys():
        self.log.warn( "in insertToDirMetadata: found no DID for directory %s" % d )
        continue
      id = int( dirIDs[ d ] )
      try:
        sqlConfigname = self._escapeString( directoryDict[d]['ConfigName'] )[ 'Value' ]
        sqlConfigversion = self._escapeString( directoryDict[d]['ConfigVersion'] )[ 'Value' ]
        sqlConditions = self._escapeString( directoryDict[d]['ConditionDescription'] )[ 'Value' ]
        sqlProcpass = self._escapeString( directoryDict[d]['ProcessingPass'] )[ 'Value' ]
        sqlEvttype = self._escapeString( directoryDict[d]['EventType'] )[ 'Value' ]
        sqlFiletype = self._escapeString( directoryDict[d]['FileType'] )[ 'Value' ]
        sqlProd = self._escapeString( directoryDict[d]['Production'] )[ 'Value' ]
      except KeyError:
        self.log.error( "in insertToDirMetadata: the input dict was not correctly formatted: %s" % directoryDict )
        return S_ERROR( "Key error in input dictionary %s" % directoryDict )
      sqlCmd = "INSERT INTO `DirMetadata` ( DID, ConfigName, ConfigVersion, Conditions, ProcessingPass, " \
          "EventType, FileType, Production ) VALUES ( %d, %s, %s, %s, %s, %s, %s, %s )" %\
          ( id, sqlConfigname, sqlConfigversion, sqlConditions, sqlProcpass, sqlEvttype, sqlFiletype, sqlProd )
      self.log.info( "sqlCmd = %s " % sqlCmd )
      result = self._update( sqlCmd )
      if not result[ 'OK' ]:
        self.log.error( "Cannot insert entry: %s" % ( result[ 'Message' ] ) )
        return S_ERROR( result['Message'] )
      insertedEntries += 1
    return S_OK( insertedEntries )

  def getDirMetadata( self, directoryList ):
    """ Return the directory meta-data, which have been previously provided by the Bookkeeping and stored in the 
        DirMetadata table. The input is a list of LFN directories
    """
    if not directoryList:
      return S_OK()
    # get the IDs from the su_Directory table:
    directoryDict = {}
    for directory in directoryList:
      directoryDict[ directory ] = {}
    result = self.__getIDs( directoryDict )
    if not result[ 'OK' ]:
      return S_ERROR( result['Message'] )
    dirIDs = result[ 'Value' ]
    if not dirIDs:
      errMsg = "ERROR! the directory %s was not found in the LFN directory table! " % directoryDict
      self.log.error( "%s" % errMsg )
      return S_ERROR( errMsg )
    self.log.info( "in getDirMetadata: dirIDs: %s" % ( dirIDs ) )
    # returns a dictionary of type:
    #  {dir1: ID1, dir2: ID2, ...}
    idList = []
    for d in directoryDict.keys():
      if d not in dirIDs.keys():
        self.log.warn( "in insertToDirMetadata: found no DID for directory %s" % d )
        continue
      id = str( dirIDs[ d ] )
      #idList.append( self._escapeString( id )[ 'Value' ] )
      idList.append( id )
    sqlCmd = "SELECT DID, ConfigName, ConfigVersion, Conditions, ProcessingPass, EventType, FileType, " \
        "Production FROM `DirMetadata` WHERE DID in ( %s )" % ", ".join( idList )
    self.log.info( "sqlCmd = %s " % sqlCmd )
    result = self._query( sqlCmd )
    if not result[ 'OK' ]:
      return S_ERROR( result['Message'] )
    return S_OK( result[ 'Value' ] )
