""" StorageUsageDB class is a front-end to the Storage Usage Database.
"""
import re, os, sys, threading
import time, datetime
from types import *

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.List import intListToString, stringListToString

#############################################################################

class StorageUsageDB( DB ):

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

    for tableName in self.__tablesDesc:
      if not tableName in tablesInDB:
        tablesToCreate[ tableName ] = self.__tablesDesc[ tableName ]

    return self._createTables( tablesToCreate )

  ################
  # Bulk insertion
  ################

  def publishDirectories( self, directoryDict ):
    """ Inserts a group of directoires with their usage """
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
        sqlCmd = "INSERT INTO `su_Directory` ( DID, Path, Files, Size ) VALUES ( 0, %s, %d, %d )" % ( sqlDirPath, files, size )
        result = self._update( sqlCmd )
        if not result[ 'OK' ]:
          return result
        dirIDs[ dirPath ] = result[ 'lastRowId' ]
      result = self.__updateSEUsage( dirIDs[ dirPath ], SEUsage )
      if not result[ 'OK' ]:
        return result
    return S_OK()

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

  def __updateSEUsage( self, dirID, SEUsage ):
    sqlCmd = "DELETE FROM `su_SEUsage` WHERE DID=%d" % dirID
    result = self._update( sqlCmd )
    if not result[ 'OK' ]:
      return result
    if not SEUsage:
      self.log.error( "Ooops. Dir has no SEUsage!", "ID %s" % dirID )
      return S_OK()
    sqlVals = []
    for SEName in SEUsage:
      try:
        size = SEUsage[ SEName ][ 'Size' ]
        files = SEUsage[ SEName ][ 'Files' ]
      except ValueError, e:
        return S_ERROR( "Values must be ints: %s" % str( e ) )
      sqlVals.append( "( %d, %s, %d, %d, UTC_TIMESTAMP() )" % ( dirID, self._escapeString( SEName )[ 'Value' ], size, files ) )
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


  ################
  # Clean outdated entries
  ################

  def purgeOutdatedEntries( self, rootDir = False, outdatedSeconds = 86400 ):
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
    sqlCmd = "SELECT DISTINCT su.DID FROM %s WHERE %s LIMIT %d" % ( ", ".join( sqlTables ), " AND ".join( sqlCond ), sqlLimit )
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
      sqlCond.append( "su.SEName in ( %s )" % ", ".join( [ self._escapeString( SEName )[ 'Value' ] for SEName in SEs ] ) )
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

  def __userExpression( self, fieldName = "d.Path" ):
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
    sqlCmd = "SELECT %s, SUM( su.Size ) FROM `su_Directory` as d, `su_SEUsage` as su WHERE %s GROUP BY %s" % ( self.__userExpression(),
                                                                                                               " AND ".join( sqlCond ),
                                                                                                               self.__userExpression() )
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
    sqlCmd = "SELECT % s FROM `su_Directory` as d, `su_SEUsage` as su WHERE %s GROUP BY %s" % ( ", ".join( sqlFields ),
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

