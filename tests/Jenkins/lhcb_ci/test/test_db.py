""" lhcb_ci.test.test_db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci.basecase
import lhcb_ci.db


from DIRAC.Core.Base.DB   import DB
from DIRAC.Core.Utilities import InstallTools


class Installation_Test( lhcb_ci.basecase.DB_TestCase ):
  
  
  def test_passwords( self ):
    """ test_passwords
    
    Makes sure the passwords are properly set on the dirac.cfg and accessed via
    the InstallTools module.
    """
    self.log.info( 'test_passwords' )
    
    
    self.assertEquals( InstallTools.mysqlRootPwd,  self.rootPass )
    self.assertEquals( InstallTools.mysqlPassword, self.userPass )
    
    res = InstallTools.getMySQLPasswords()
    self.assertEquals( res[ 'OK' ], True )
    
    self.assertEquals( InstallTools.mysqlRootPwd,  self.rootPass )
    self.assertEquals( InstallTools.mysqlPassword, self.userPass )    
  
  
  def test_databases_reachable( self ):
    """ test_databases_reachable
    
    Tests that we can import the DIRAC DB objects pointing to an specific Database.
    """
  
    self.log.info( 'test_databases_db_reachable' )
  
    for diracSystem, systemDBs in self.databases.iteritems():   
      
      diracSystem = diracSystem.replace( 'System', '' )
      
      for systemDB in systemDBs:
        
        try:
          self.log.debug( '%s/%s' % ( diracSystem, systemDB ) )
          db = DB( systemDB, '%s/%s' % ( diracSystem, systemDB ), 10 )
        except RuntimeError, msg:
          self.log.error( 'Error importing %s/%s' % ( diracSystem, systemDB ) )
          self.log.error( msg )
          self.fail( msg )   
        
        result = db._query( "show status" )
        if not result[ 'OK' ]:
          # Print before it crashes
          self.log.error( result[ 'Message' ] )
        self.assertEquals( result[ 'OK' ], True )
        del db
   
          
  def test_databases_drop( self ):
    """ test_databases_drop
    
    Tests that we can drop directly databases from the MySQL server
    """
   
    self.log.info( 'test_databases_drop' )

    for systemDBs in self.databases.itervalues():   
    
      for dbName in systemDBs:
        
        self.log.debug( "Dropping %s" % dbName )
        
        res = lhcb_ci.db.dropDB( dbName )
        self.assertEquals( res, True )
        
        self.log.debug( "Installing %s" % dbName )
        
        res = lhcb_ci.db.install( dbName )
        if not res[ 'OK' ]:
          self.log.error( res[ 'Message' ] )
        self.assertEquals( res[ 'OK' ], True )  

#...............................................................................
#EOF