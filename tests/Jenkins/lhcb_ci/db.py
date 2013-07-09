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
  
  #if not result[ 'OK' ]:
  #  return result
  #
  #_extension, system = result[ 'Value' ]
  #return InstallTools.addDatabaseOptionsToCS( None, system, dbName, overwrite = True )
  
  
def dropDB( dbName ):
  """ drop DB
  
  """  

  result = False
  logger.debug( "dropDB %s" % dbName )

  InstallTools.getMySQLPasswords()
  
  conn = MySQLdb.connect( host = InstallTools.mysqlHost,
                          port = InstallTools.mysqlPort,
                          user = InstallTools.mysqlRootUser,
                          passwd = InstallTools.mysqlRootPwd )
  
  cursor = conn.cursor()
  
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

#...............................................................................
#EOF