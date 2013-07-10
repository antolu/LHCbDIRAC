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
  """ Installation_Test
  
  Tests performing operations related with the DBs installation.
  """
  
  
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
  
    self.log.info( 'test_databases_reachable' )
  
    for diracSystem, systemDBs in self.databases.iteritems():   
      
      diracSystem = diracSystem.replace( 'System', '' )
      
      for dbName in systemDBs:
        
        # First installs database on  server
        res = lhcb_ci.db.installDB( dbName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )  
        
        # Tries to connect to the DB using the DB DIRAC module
        try:
          self.log.debug( 'Reaching %s/%s' % ( diracSystem, dbName ) )
          db = DB( dbName, '%s/%s' % ( diracSystem, dbName ), 10 )
        except RuntimeError, msg:
          self.log.error( 'Error importing %s/%s' % ( diracSystem, dbName ) )
          self.log.error( msg )
          self.fail( msg )   
        
        # If the DB is installed, we make a simple query
        res = db._query( "show status" )
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        
        # Cleanup
        del db
        res = lhcb_ci.db.dropDB( dbName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )
   
          
  def test_databases_install_drop( self ):
    """ test_databases_install_drop
    
    Tests that we can install / drop directly databases from the MySQL server
    """
   
    self.log.info( 'test_databases_install_drop' )

    for systemDBs in self.databases.itervalues():   
    
      for dbName in systemDBs:

        res = lhcb_ci.db.installDB( dbName )         
        self.assertDIRACEquals( res[ 'OK' ], True, res )
                
        res = lhcb_ci.db.dropDB( dbName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )  


  def test_import_db_modules( self ):
    """ test_import_db_modules
    
    Tries to import the DB modules and create a class Object.
    """
    
    _EXCEPTIONS = [ 'TransformationDB', 'RAWIntegrityDB' ]

    self.log.info( 'test_import_db_modules' )
    
    for diracSystem, systemDBs in self.databases.iteritems():
      
      for dbName in systemDBs:

        if dbName in _EXCEPTIONS:
          self.log.exception( 'EXCEPTION: Skipped %s' % dbName )
          continue
          
        # Import DIRAC module and get object
        dbPath = 'DIRAC.%s.DB.%s' % ( diracSystem, dbName )
        self.log.debug( 'VO Importing %s' % dbPath )
        
        dbMod = lhcb_ci.extensions.import_( 'DIRAC.%s.DB.%s' % ( diracSystem, dbName ) )
        self.assertEquals( hasattr( dbMod, dbName ), True )
        
        dbClass = getattr( dbMod, dbName )
        
        try:
          dbInstance = dbClass()
          del dbInstance
          self.fail( 'Created instance of %s should have failed' % dbPath )
        except RuntimeError, e:          
          self.assertEquals( str( e ).startswith( 'Can not connect to DB' ), True )
    

  def test_install_tables( self ):
    """ test_install_tables
    
    This test only applies to databases defined on Python.
    """
    
    # Some DBs are a bit different ( to be ironed ), so for the time being are
    # skipped
    _EXCEPTIONS = [ 'SystemLoggingDB' ]
    
    self.log.info( 'test_install_tables' )
    
    for diracSystem, systemDBs in self.databases.iteritems():
      
      for dbName in systemDBs:
    
        if dbName in _EXCEPTIONS:
          self.log.exception( 'EXCEPTION: Skipped %s' % dbName )
          continue

        # Installs DB
        res = lhcb_ci.db.installDB( dbName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        
        # Gets tables of the DB ( if sql schema provided, this if is positive )
        tables = lhcb_ci.db.getTables( dbName )
        if tables:
          self.log.exception( 'Tables found for %s/%s' % ( diracSystem, dbName ) )
          self.log.exception( tables )
          res = lhcb_ci.db.dropDB( dbName )
          self.assertDIRACEquals( res[ 'OK' ], True, res )
          continue
        
        # Import DIRAC module and get object
        dbPath = 'DIRAC.%s.DB.%s' % ( diracSystem, dbName )
        self.log.debug( 'VO Importing %s' % dbPath )
        
        dbMod = lhcb_ci.extensions.import_( 'DIRAC.%s.DB.%s' % ( diracSystem, dbName ) )
        self.assertEquals( hasattr( dbMod, dbName ), True )
        
        dbClass = getattr( dbMod, dbName )
        
        try:
          dbInstance = dbClass()
        except Exception, e:
          self.log.error( e )
          self.fail( 'Creating db instance crashed. This should not happen' )
        
        if not hasattr( dbInstance, '_checkTable' ):
          self.log.exception( '%s NOT FOLLOWING STANDARDS' % dbName )
          continue
          
        # Each DB Instance using the pythonic DB definition must have this method  
        self.assertEquals( hasattr( dbInstance, "_checkTable" ), True )
        
        res = dbInstance._checkTable()
        self.assertDIRACEquals( res[ 'OK' ], True, res )

        # Cleaning                  
        del dbMod
        del dbClass
        del dbInstance    
        res = lhcb_ci.db.dropDB( dbName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )
    
#...............................................................................
#EOF