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
  with open( os.path.join( workspace, passFile ), 'r' ) as f:  
    return f.read().split( '\n' )[ 0 ]


def getRootPass():
  return __readPass( 'rootMySQL' )
  

def getUserPass():
  return __readPass( 'userMySQL' )


def installDB( dbName ):
  """ install
  
  """

  InstallTools.getMySQLPasswords()
  logger.debug( "install database %s" % dbName )
  result = InstallTools.installDatabase( dbName )
  logger.debug( "result: %s" % result )
  return result
  
  
def dropDB( dbName ):
  """ dropDB
  
  """  

  logger.debug( "dropDB %s" % dbName )

#  conn, cursor = _getCursor( dbName )

  # I know... it is unsafe, but the current version does not work with
  # parametrics... 
  query = "drop database %s" % dbName
  
  return _execute( query )
  
#  try:
#    # I know... it is unsafe, but the current version does not work with
#    # parametrics...
#    res = cursor.execute( "drop database %s" % dbName )
#    logger.debug( "result: %s" % str( res ) )
#  except MySQLdb.Error, e:
#    logger.error( 'Error executing' )
#    logger.error( e )
#    errMsg = e
#    
#  cursor.close()
#  try:
#    conn.close()
#  except MySQLdb.Error, e:
#    logger.error( 'Error closing connection' )
#    logger.error( e )
#    errMsg += e
#    
#  if errMsg:
#    return { 'OK' : False, 'Message' : errMsg }  
#  
#  return { 'OK' : True, 'Value' : None }   


def getTables( dbName ):
  """ getTables
  
  """

  logger.debug( "getTables %s" % dbName )
  
  tables = _execute( "show tables", dbName )
  
  if not tables[ 'OK' ] or not tables[ 'Value' ]:
    return tables
    
  return { 'OK' : True, 'Value' : [ t[0] for t in tables[ 'Value' ] ] } 
    
  
#  tables = []
#  
#  conn, cursor = _getCursor( dbName )
#
#  try:
#    # I know... it is unsafe, but the current version does not work with
#    # parametrics...
#    res = cursor.execute( "show tables" )
#    if res > 0:
#      tables = [ table[0] for table in cursor.fetchall() ]
#      
#    logger.debug( "result: %s" % str( tables ) )
#    
#  except MySQLdb.Error, e:
#    logger.error( 'Error executing' )
#    logger.error( e )
#    
#  cursor.close()
#  try:
#    conn.close()
#  except MySQLdb.Error, e:
#    logger.error( 'Error closing connection' )
#    logger.error( e )
#        
#  return tables   


#def dropTable( dbName, tableName ):
#  """ dropTable
#  
#  """
#  
#  logger.debug( "dropTable %s.%s" % ( dbName, tableName ) )
#
#  conn, cursor = _getCursor( dbName ) 
#
#  try:
#    # I know... it is unsafe, but the current version does not work with
#    # parametrics...
#    res = cursor.execute( "drop table %s" % tableName )
#    logger.debug( "result: %s" % str( res ) )
#    result = True
#  except MySQLdb.Error, e:
#    logger.error( 'Error executing' )
#    logger.error( e )
#    
#  cursor.close()
#  try:
#    conn.close()
#  except MySQLdb.Error, e:
#    logger.error( 'Error closing connection' )
#    logger.error( e )
#    result = False
#    
#  return result   


def __getCursor( dbName = None ):

  InstallTools.getMySQLPasswords()
  
  conn = MySQLdb.connect( host   = InstallTools.mysqlHost,
                          port   = InstallTools.mysqlPort,
                          user   = InstallTools.mysqlRootUser,
                          passwd = InstallTools.mysqlRootPwd,
                          db     = dbName )
  
  cursor = conn.cursor()
  
  return conn, cursor 


def _execute( query, dbName = None ):

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