""" lhcb_ci.test.test_db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci.basecase
import lhcb_ci.db
import lhcb_ci.extensions


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
          self.log.debug( 'Reaching %s/%s' % ( diracSystem, systemDB ) )
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
                
        res = lhcb_ci.db.dropDB( dbName )
        self.assertEquals( res, True )
        
        res = lhcb_ci.db.install( dbName )
        self.assertEquals( res[ 'OK' ], True )  
   

  def test_install_tables( self ):
    """ test_install_tables
    
    This test only applies to databases defined on Python.
    """
    
    self.log.info( 'test_install_tables' )
    
    for diracSystem, systemDBs in self.databases.iteritems():
      
      for dbName in systemDBs:
        
        tables = lhcb_ci.db.getTables( dbName )
        if tables:
          self.log.debug( 'Tables found for %s/%s' % ( diracSystem, dbName ) )
          self.log.debug( tables )
          continue
        
        dbPath = 'DIRAC.%s.DB.%s' % ( diracSystem, dbName )
        
        self.log.debug( 'Importing %s' % dbPath )
        
        dbMod = lhcb_ci.extensions.import_( 'DIRAC.%s.DB.%s' % ( diracSystem, dbName ) )
        self.assertEquals( hasattr( dbMod, dbName ), True )
        
        dbClass = getattr( dbMod, dbName )
        
        try:
          dbInstance = dbClass()
        except Exception, e:
          self.log.error( e )
          self.fail( 'Creating db instance crashed. This should not happen' )
        
        if not hasattr( dbInstance, '_checkTable' ):
          self.log.error( '%s NOT FOLLOWING STANDARDS' % dbName )
          continue 
        
          
        # Each DB Instance using the pythonic DB definition must have this method  
        self.assertEquals( hasattr( dbInstance, "_checkTable" ), True )
        
        res = dbInstance._checkTable()
        
        self.assertEquals( res[ 'OK' ], True )
        self.log.debug( res )  
          
        del dbMod
        del dbClass
        del dbInstance    
    
#...............................................................................
#EOF