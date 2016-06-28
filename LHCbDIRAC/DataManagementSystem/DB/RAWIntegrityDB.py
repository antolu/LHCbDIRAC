""" RAWIntegrityDB is the front end for the database containing the files which are awating migration.
    It offers a simple interface to add files, get files and modify their status.
"""

__RCSID__ = "$Id$"

import types

from DIRAC              import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB

gLogger.initialize( 'DMS', '/Databases/RAWIntegrityDB/Test' )

class RAWIntegrityDB( DB ):


  _tablesDict = {}
  # Files table
  _tablesDict[ 'Files' ] = { 'Fields' :
                                       {'ID'             : 'BIGINT NOT NULL AUTO_INCREMENT',
                                        'LFN'            : 'VARCHAR(255) NOT NULL',
                                        'PFN'            : 'VARCHAR(255) NOT NULL',
                                        'Size'           : 'BIGINT NOT NULL',
                                        'StorageElement' : 'VARCHAR(32) NOT NULL',
                                        'GUID'           : 'VARCHAR(255) NOT NULL',
                                        'FileChecksum'   : 'VARCHAR(255) NOT NULL',
                                        'SubmitTime'     : 'DATETIME NOT NULL',
                                        'CompleteTime'   : 'DATETIME',
                                        'Status'         : 'VARCHAR(255) DEFAULT "Active"'
                                       },
                             'PrimaryKey' : 'ID',
                             'Indexes' : { 'Status' : [ 'Status' ],
                                          'LFN' : [ 'LFN' ] }
                           }
  # LastMonitor table
  _tablesDict[ 'LastMonitor' ] = { 'Fields' :
                                             {
                                              'LastMonitorTime' : 'DATETIME NOT NULL PRIMARY KEY'
                                             }
                                 }


  def __init__( self, systemInstance = 'Default' ):
    DB.__init__( self, 'RAWIntegrityDB', 'DataManagement/RAWIntegrityDB' )


  def _checkTable( self ):
    """ _checkTable

    Make sure the table is created
    """

    return self.__createTables()

  def __createTables( self ):
    """ __createTables

    Writes the schema in the database. If a table is already in the schema, it is
    skipped to avoid problems trying to create a table that already exists.
    """

    # Horrible SQL here !!
    existingTables = self._query( "show tables" )
    if not existingTables[ 'OK' ]:
      return existingTables
    existingTables = [ existingTable[0] for existingTable in existingTables[ 'Value' ] ]

    # Makes a copy of the dictionary _tablesDict
    tables = {}
    tables.update( self._tablesDict )

    for existingTable in existingTables:
      if existingTable in tables:
        del tables[ existingTable ]

    res = self._createTables( tables )
    if not res[ 'OK' ]:
      return res
    
    # Human readable S_OK message
    if res[ 'Value' ] == 0:
      res[ 'Value' ] = 'No tables created'
    else:
      res[ 'Value' ] = 'Tables created: %s' % ( ','.join( tables.keys() ) )
    return res



  def getActiveFiles( self ):
    """ Obtain all the active files in the database along with all their associated metadata
    """
    try:
      gLogger.info( "RAWIntegrityDB.getActiveFiles: Obtaining files awaiting migration from database." )
      req = "SELECT LFN,PFN,Size,StorageElement,GUID,FileChecksum,TIME_TO_SEC(TIMEDIFF(UTC_TIMESTAMP(),SubmitTime)) from Files WHERE Status = 'Active';"
      res = self._query( req )
      if not res['OK']:
        gLogger.error( "RAWIntegrityDB.getActiveFiles: Failed to get files from database.", res['Message'] )
        return res
      else:
        fileDict = {}
        for lfn, pfn, size, se, guid, checksum, waittime in res['Value']:
          fileDict[lfn] = {'PFN':pfn, 'Size':size, 'SE':se, 'GUID':guid, 'Checksum':checksum, 'WaitTime':waittime}
        gLogger.info( "RAWIntegrityDB.getActiveFiles: Obtained %s files awaiting migration from database." % len( fileDict.keys() ) )
        return S_OK( fileDict )
    except Exception as x:
      errStr = "RAWIntegrityDB.getActiveFiles: Exception while getting files from database."
      gLogger.exception( errStr, lException = x )
      return S_ERROR( errStr )


  def setFileStatus( self, lfn, status ):
    """ Update the status of the file in the database
    """
    try:
      gLogger.info( "RAWIntegrityDB.setFileStatus: Attempting to update status of %s to '%s'." % ( lfn, status ) )
      req = "UPDATE Files SET Status='%s',CompleteTime=UTC_TIMESTAMP() WHERE LFN = '%s' AND Status = 'Active';" % ( status, lfn )
      res = self._update( req )
      if not res['OK']:
        gLogger.error( "RAWIntegrityDB.setFileStatus: Failed update file status.", res['Message'] )
      else:
        gLogger.info( "RAWIntegrityDB.setFileStatus: Successfully updated file status." )
      return res
    except Exception as x:
      errStr = "RAWIntegrityDB.setFileStatus: Exception while updating file status."
      gLogger.exception( errStr, lException = x )
      return S_ERROR( errStr )


  def addFile( self, lfn, pfn, size, se, guid, checksum ):
    """ Insert file into the database
    """
    try:
      gLogger.info( "RAWIntegrityDB.addFile: Attempting to add %s to database." % lfn )
      req = "INSERT INTO Files (LFN,PFN,Size,StorageElement,GUID,FileChecksum,SubmitTime) VALUES\
            ('%s','%s',%s,'%s','%s','%s',UTC_TIMESTAMP());" % ( lfn, pfn, size, se, guid, checksum )
      res = self._update( req )
      if not res['OK']:
        gLogger.error( "RAWIntegrityDB.addFile: Failed update add file to database.", res['Message'] )
      else:
        gLogger.info( "RAWIntegrityDB.addFile: Successfully added file." )
      return res
    except Exception as x:
      errStr = "RAWIntegrityDB.addFile: Exception while updating file status."
      gLogger.exception( errStr, lException = x )
      return S_ERROR( errStr )


  def setLastMonitorTime( self ):
    """ Set the last time the migration rate was calculated
    """
    try:
      gLogger.info( "RAWIntegrityDB.setLastMonitorTime: Attempting to set the last migration marker." )
      req = "UPDATE LastMonitor SET LastMonitorTime=UTC_TIMESTAMP();"
      res = self._update( req )
      if not res['OK']:
        gLogger.error( "RAWIntegrityDB.setLastMonitorTime: Failed update migration marker.", res['Message'] )
      else:
        gLogger.info( "RAWIntegrityDB.setLastMonitorTime: Successfully updated migration marker." )
      return res
    except Exception as x:
      errStr = "RAWIntegrityDB.setLastMonitorTime: Exception while updating migration marker."
      gLogger.exception( errStr, lException = x )
      return S_ERROR( errStr )


  def getLastMonitorTimeDiff( self ):
    """ Get the last time the migration rate was calculated
    """
    try:
      gLogger.info( "RAWIntegrityDB.getLastMonitorTimeDiff: Attempting to get the last migration marker." )
      req = "SELECT TIME_TO_SEC(TIMEDIFF(UTC_TIMESTAMP(),LastMonitorTime)) FROM LastMonitor LIMIT 1;"
      res = self._query( req )
      if not res['OK']:
        gLogger.error( "RAWIntegrityDB.getLastMonitorTimeDiff: Failed get migration marker.", res['Message'] )
        return res
      else:
        gLogger.info( "RAWIntegrityDB.getLastMonitorTimeDiff: Successfully obtained migration marker." )
        timediff = res['Value'][0][0]
        return S_OK( timediff )
    except Exception as x:
      errStr = "RAWIntegrityDB.getLastMonitorTimeDiff: Exception while getting migration marker."
      gLogger.exception( errStr, lException = x )
      return S_ERROR( errStr )


  def getGlobalStatistics( self ):
    """ Get the count of the file statutes in the DB
    """
    req = "SELECT Status,COUNT(*) FROM Files GROUP BY Status;"
    res = self._query( req )
    if not res['OK']:
      return res
    statusDict = {}
    for resValue in res['Value']:
      status, count = resValue
      statusDict[status] = count
    return S_OK( statusDict )


  def getFileSelections( self ):
    """ Get the unique values of the selection fields
    """
    selDict = {'StorageElement':[], 'Status':[]}
    req = "SELECT DISTINCT(StorageElement) FROM Files;"
    res = self._query( req )
    if not res['OK']:
      return res
    for resValue in res['Value']:
      selDict['StorageElement'].append( resValue[0] )
    req = "SELECT DISTINCT(Status) FROM Files;"
    res = self._query( req )
    if not res['OK']:
      return res
    for resValue in res['Value']:
      selDict['Status'].append( resValue[0] )
    return S_OK( selDict )


  def __buildCondition( self, condDict, older = None, newer = None, timeStamp = 'SubmitTime' ):
    """ build SQL condition statement from provided condDict and other extra conditions
    """
    condition = ''
    conjunction = "WHERE"
    if condDict != None:
      for attrName, attrValue in condDict.items():
        ret = self._escapeString( attrName )
        if not ret['OK']:
          return ret
        attrName = "`" + ret['Value'][1:-1] + "`"
        if type( attrValue ) == types.ListType:
          multiValueList = []
          for x in attrValue:
            ret = self._escapeString( x )
            if not ret['OK']:
              return ret
            x = ret['Value']
            multiValueList.append( x )
          multiValue = ','.join( multiValueList )
          condition = ' %s %s %s in (%s)' % ( condition, conjunction, attrName, multiValue )
        else:
          ret = self._escapeString( attrValue )
          if not ret['OK']:
            return ret
          attrValue = ret['Value']
          condition = ' %s %s %s=%s' % ( condition, conjunction, attrName, attrValue )
        conjunction = "AND"
    if older:
      ret = self._escapeString( older )
      if not ret['OK']:
        return ret
      older = ret['Value']
      condition = ' %s %s %s <= %s' % ( condition, conjunction, timeStamp, older )
      conjunction = "AND"
    if newer:
      ret = self._escapeString( newer )
      if not ret['OK']:
        return ret
      newer = ret['Value']
      condition = ' %s %s %s >= %s' % ( condition, conjunction, timeStamp, newer )
    return condition


  def selectFiles( self, selectDict, orderAttribute = 'LFN', newer = None, older = None, limit = None ):
    """ Select the files which match the selection criteria.
    """
    condition = self.__buildCondition( selectDict, older, newer )
    if orderAttribute:
      orderType = None
      orderField = orderAttribute
      if orderAttribute.find( ':' ) != -1:
        orderType = orderAttribute.split( ':' )[1].upper()
        orderField = orderAttribute.split( ':' )[0]
      condition = condition + ' ORDER BY ' + orderField
      if orderType:
        condition = condition + ' ' + orderType
    if limit:
      condition = condition + ' LIMIT ' + str( limit )
    cmd = 'SELECT LFN,PFN,Size,StorageElement,GUID,FileChecksum,SubmitTime,CompleteTime,Status from Files %s' % condition
    print cmd
    res = self._query( cmd )
    if not res['OK']:
      return res
    if not len( res['Value'] ):
      return S_OK( [] )
    return S_OK( res['Value'] )

# ...............................................................................
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
