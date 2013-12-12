""" lhcb_ci.test.test_db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci.basecase
import lhcb_ci.component
import lhcb_ci.db
import lhcb_ci.extensions


class ConfigureTest( lhcb_ci.basecase.DB_TestCase ):
  """ ConfigureTest
  
  This class contains dirty & sticky tests. The configuration steps have been
  transformed into simple unittests, which are run here. Dirty & sticky because
  the tests will alter the CS structure, adding the necessary configuration 
  parameters to be able to run the rest of the tests. 
  
  Disclaimer: do not change the name of the tests, as some of them need to
  run in order.
  
  """
  
  @lhcb_ci.basecase.timeDecorator
  def test_configure_mysql_passwords( self ):
    """ test_configure_mysql_passwords
    
    Makes sure the passwords are properly set on the dirac.cfg and accessed via
    the InstallTools module.
    
    """
    
    self.logTestName()
        
    self.assertEquals( lhcb_ci.db.InstallTools.mysqlRootPwd,  self.rootPass )
    self.assertEquals( lhcb_ci.db.InstallTools.mysqlPassword, self.userPass )
    
    res = lhcb_ci.db.InstallTools.getMySQLPasswords()
    self.assertEquals( res[ 'OK' ], True )
    
    self.assertEquals( lhcb_ci.db.InstallTools.mysqlRootPwd,  self.rootPass )
    self.assertEquals( lhcb_ci.db.InstallTools.mysqlPassword, self.userPass )       
  
  @lhcb_ci.basecase.timeDecorator
  def test_configure_dbs( self ):
    """ test_configure_dbs
    
    Tests that we can configure databases on an "empty CS".
    """
    
    self.logTestName()
  
    for systemName, systemDBs in self.databases.iteritems():   
      
      for dbName in systemDBs:
        
        db  = lhcb_ci.component.Component( systemName, 'DB', dbName )
        res = db.configure()
        self.assertDIRACEquals( res[ 'OK' ], True, res ) 


  #.............................................................................
  # Nosetests attrs

  
  # test_configured_mysql_passwords
  test_configure_mysql_passwords.configure = 1
  test_configure_mysql_passwords.db        = 1
  
  # test_configure_db
  test_configure_dbs.configure = 1
  test_configure_dbs.db        = 1   


class InstallationTest( lhcb_ci.basecase.DB_TestCase ):
  """ InstallationTest
  
  Tests performing operations related with the DBs installation.
  
  """ 
  

  #FIXME: this test is redundant and should be deleted.
  @lhcb_ci.basecase.timeDecorator      
  def test_databases_install_drop( self ):
    """ test_databases_install_drop
    
    Tests that we can install databases on the MySQL server using a DIRAC command
    and drop directly databases from the MySQL server using a SQL statement.
    
    """
   
    self.logTestName()

    for systemName, dbNames in self.databases.iteritems():   
    
      for dbName in dbNames:

        db  = lhcb_ci.component.Component( systemName, 'DB', dbName )
        res = db.install()
        self.assertDIRACEquals( res[ 'OK' ], True, res )
                
        res = db.uninstall()
        self.assertDIRACEquals( res[ 'OK' ], True, res )  
  
  @lhcb_ci.basecase.timeDecorator
  def test_databases_common_import( self ):
    """ test_databases_common_import
    
    Tests that we can import the DIRAC DB objects pointing to an specific Database.
    It iterates over all databases discovered on the code *DB.py objects and instantiates
    a DIRAC.Core.Base.DB object to interact with them. 
    
    """
  
    self.logTestName()
  
    for systemName, systemDBs in self.databases.iteritems():   
      
      diracSystem = systemName.replace( 'System', '' )
      
      for dbName in systemDBs:
        
        # First installs database on  server
        db  = lhcb_ci.component.Component( systemName, 'DB', dbName )
        res = db.install()
        self.assertDIRACEquals( res[ 'OK' ], True, res )
          
        
        # Tries to connect to the DB using the DB DIRAC module
        try:
          self.log.debug( 'Reaching %s/%s' % ( diracSystem, dbName ) )
          #dbObj = lhcb_ci.db.getDB( dbName, '%s/%s' % ( diracSystem, dbName ), 10 )
          dbObj = db.rawObj()
        except RuntimeError, msg:
          self.log.error( 'Error importing %s/%s' % ( diracSystem, dbName ) )
          self.log.error( msg )
          self.fail( msg )   
        
        # If the DB is installed, we make a simple query
        res = dbObj._query( "show status" )
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        
        # Cleanup
        del dbObj
        res = db.uninstall()
        self.assertDIRACEquals( res[ 'OK' ], True, res )
    
  @lhcb_ci.basecase.timeDecorator
  def test_install_tables( self ):
    """ test_install_tables
    
    This test only applies to databases defined on Python ( some of the databases
    are still using the sql schema definition ).
    
    """
    
    self.logTestName()
    
    for systemName, systemDBs in self.databases.iteritems():
      
      for dbName in systemDBs:
    
        if self.isException( dbName ):
          continue

        # Installs DB
        db  = lhcb_ci.component.Component( systemName, 'DB', dbName )
        res = db.install()
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        
        # Gets tables of the DB ( if sql schema provided, this if is positive )
        tables = db.getTables()
        self.assertDIRACEquals( tables[ 'OK' ], True, tables )
        
        if tables[ 'Value' ]:
          self.log.exception( 'Tables found for %s/%s' % ( systemName, dbName ) )
          self.log.exception( tables[ 'Value' ] )
          res = db.uninstall()
          self.assertDIRACEquals( res[ 'OK' ], True, res )
          continue
        
        # Import DIRAC module and get object
        dbPath = 'DIRAC.%s.DB.%s' % ( systemName, dbName )
        self.log.debug( 'VO Importing %s' % dbPath )
        
        dbMod = lhcb_ci.extensions.import_( 'DIRAC.%s.DB.%s' % ( systemName, dbName ) )
        self.assertEquals( hasattr( dbMod, dbName ), True )
        
        dbClass = getattr( dbMod, dbName )
        
        try:
          dbInstance = dbClass()
        except Exception, e:
          self.log.error( e )
          self.fail( 'Creating db instance crashed. This should not happen' )
        
        if not hasattr( dbInstance, '_checkTable' ):
          self.log.exception( 'EXCEPTION: %s NOT FOLLOWING STANDARDS' % dbName )
          res = db.uninstall()
          self.assertDIRACEquals( res[ 'OK' ], True, res )
          del dbMod
          del dbClass
          del dbInstance
          continue
          
        # Each DB Instance using the pythonic DB definition must have this method  
        self.assertEquals( hasattr( dbInstance, "_checkTable" ), True )
        
        res = dbInstance._checkTable()
        self.assertDIRACEquals( res[ 'OK' ], True, res )

        # Cleaning                  
        del dbMod
        del dbClass
        del dbInstance    
        res = db.uninstall()
        self.assertDIRACEquals( res[ 'OK' ], True, res )


  #.............................................................................    
  # Nosetests attrs

  # test_databases_install_drop
  test_databases_install_drop.install = 1
  test_databases_install_drop.db      = 1  
  
  # test_databases_reachable
  test_databases_common_import.install = 0
  test_databases_common_import.db      = 0
  
  # test_install_tables
  test_install_tables.install = 0
  test_install_tables.db      = 0


class SmokeTest( lhcb_ci.basecase.DB_TestCase ):
  """ SmokeTest
  
  Tests performing basic common operations on the databases.
  
  """ 

  pass
  
    
#...............................................................................
#EOF