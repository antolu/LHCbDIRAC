""" lhcb_ci.utils.db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import MySQLdb

# DIRAC
from DIRAC.Core.Utilities import InstallTools

# lhcb_ci
from lhcb_ci import logger


def install( dbName ):
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

  result = False
  logger.debug( "dropDB %s" % dbName )

  conn, cursor = _getCursor( dbName )
  
  try:
    # I know... it is unsafe, but the current version does not work with
    # parametrics...
    res = cursor.execute( "drop database %s" % dbName )
    logger.debug( "result: %s" % str( res ) )
    result = True
  except MySQLdb.Error, e:
    logger.error( 'Error executing' )
    logger.error( e )
    
  cursor.close()
  try:
    conn.close()
  except MySQLdb.Error, e:
    logger.error( 'Error closing connection' )
    logger.error( e )
    result = False
    
  return result   


def getTables( dbName ):
  """ getTables
  
  """

  logger.debug( "getTables %s" % dbName )
  
  tables = []
  
  conn, cursor = _getCursor( dbName )

  try:
    # I know... it is unsafe, but the current version does not work with
    # parametrics...
    res = cursor.execute( "show tables" )
    if res > 0:
      tables = [ table[0] for table in cursor.fetchall() ]
      
    logger.debug( "result: %s" % str( tables ) )
    
  except MySQLdb.Error, e:
    logger.error( 'Error executing' )
    logger.error( e )
    
  cursor.close()
  try:
    conn.close()
  except MySQLdb.Error, e:
    logger.error( 'Error closing connection' )
    logger.error( e )
        
  return tables   


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


def _getCursor( dbName = None ):

  InstallTools.getMySQLPasswords()
  
  conn = MySQLdb.connect( host   = InstallTools.mysqlHost,
                          port   = InstallTools.mysqlPort,
                          user   = InstallTools.mysqlRootUser,
                          passwd = InstallTools.mysqlRootPwd,
                          db     = dbName )
  
  cursor = conn.cursor()
  
  return conn, cursor 

#...............................................................................
#EOF