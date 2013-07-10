""" lhcb_ci.utils.db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

import collections
import MySQLdb
import os

# DIRAC
from DIRAC.Core.Utilities import InstallTools

# lhcb_ci
from lhcb_ci import logger

workspace = os.getenv( 'WORKSPACE' )

def getDatabases():
  """ getDatabases
  
  Reads file $WORKSPACE/databases and transforms it into a dictionary  
  """
  
  databases = collections.defaultdict( set )   
       
  with open( os.path.join( workspace, 'databases' ), 'r' ) as f:
    db_data = f.read().split( '\n' )
   
  for db_line in db_data:
      
    if not db_line:
      continue
      
    logger.debug( db_line )
  
    system, dbName = db_line.split( ' ' )
    databases[ system ].update( [ dbName.split( '.' )[ 0 ] ] )  

  return databases


def __readPass( passFile ):
  """ __readPass
  
  Reads file passFile and returns its content
  """
  
  with open( os.path.join( workspace, passFile ), 'r' ) as f:  
    return f.read().split( '\n' )[ 0 ]


def getRootPass():
  """ getRootPass
  
  Reads file containing random MySQL root password
  """
  
  return __readPass( 'rootMySQL' )
  

def getUserPass():
  """ getUserPass
  
  Reads file containing random MySQL user password
  """
  
  return __readPass( 'userMySQL' )


def installDB( dbName ):
  """ installDB
  
  Installs Database using DIRAC standard tools.
  """

  InstallTools.getMySQLPasswords()
  logger.debug( "install database %s" % dbName )
  result = InstallTools.installDatabase( dbName )
  logger.debug( "result: %s" % result )
  return result
  
  
def dropDB( dbName ):
  """ dropDB
  
  Drops database connecting directly to MySQL server.
  """  

  logger.debug( "dropDB %s" % dbName )
  # I know... it is unsafe, but the current version does not work with
  # parametrics... 
  query = "drop database %s" % dbName
  
  return _execute( query )


def getInstalledDBs():
  """ getInstalledDBs
  
  Gets from MySQL server all DBs installed ( without _EXCEPTIONS ).
  """

  _EXCEPTIONS = [ 'test', 'mysql', 'information_schema' ]

  logger.debug( "getInstalledDBs" )
  
  dbs = _execute( "show databases" )
  if not dbs[ 'OK' ] or not dbs[ 'Value' ]:
    return dbs

  result = []
    
  for db in dbs[ 'Value' ]:
    if db[ 0 ] in _EXCEPTIONS:
      continue
    result.append( db[ 0 ] )

  return { 'OK' : True, 'Value' : result }  
  

def getTables( dbName ):
  """ getTables
  
  Given a dbName, returns all its tables ( installed ).
  """

  logger.debug( "getTables %s" % dbName )
  
  tables = _execute( "show tables", dbName )
  
  if not tables[ 'OK' ] or not tables[ 'Value' ]:
    return tables
    
  return { 'OK' : True, 'Value' : [ t[0] for t in tables[ 'Value' ] ] } 


def __getCursor( dbName ):
  """ __getCursor
  
  Given a dbName, get a connection and a cursor to execute queries.
  """

  InstallTools.getMySQLPasswords()
  
  conn = MySQLdb.connect( host   = InstallTools.mysqlHost,
                          port   = InstallTools.mysqlPort,
                          user   = InstallTools.mysqlRootUser,
                          passwd = InstallTools.mysqlRootPwd,
                          db     = dbName )
  
  cursor = conn.cursor()
  
  return conn, cursor 


def _execute( query, dbName = '' ):
  """ _execute
  
  Given a query and a dbName ( if not given it will not connect to any DB in
  particular ), executes it and closes connection and cursor.
  """

  conn, cursor = __getCursor( dbName )  

  errMsg = ''

  try:
    # I know... it is unsafe, but the current version does not work with
    # parametrics...
    res = cursor.execute( query )
    if res > 0:
      res = cursor.fetchall()
    logger.debug( "result: %s" % str( res ) )
    
  except MySQLdb.Error, e:
    logger.error( 'Error executing' )
    logger.error( e )
    errMsg = e
    
  cursor.close()
  try:
    conn.close()
  except MySQLdb.Error, e:
    logger.error( 'Error closing connection' )
    logger.error( e )
    errMsg += e
    
  if errMsg:
    return { 'OK' : False, 'Message' : errMsg }
  return { 'OK' : True, 'Value' : res }  
  

#...............................................................................
#EOF