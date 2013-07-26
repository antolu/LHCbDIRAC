""" lhcb_ci.test.test_db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci.basecase
import lhcb_ci.db
import lhcb_ci.extensions


class Installation_Test( lhcb_ci.basecase.DB_TestCase ):
  """ Installation_Test
  
  Tests performing operations related with the DBs installation.
  
  """ 
  
  
  def test_databases_reachable( self ):
    """ test_databases_reachable
    
    Tests that we can import the DIRAC DB objects pointing to an specific Database.
    It iterates over all databases discovered on the code *DB.py objects and instantiates
    a DIRAC.Core.Base.DB object to interact with them. 
    
    """
  
    self.logTestName( 'test_databases_reachable' )
  
    for diracSystem, systemDBs in self.databases.iteritems():   
      
      diracSystem = diracSystem.replace( 'System', '' )
      
      for dbName in systemDBs:
        
        # First installs database on  server
        res = lhcb_ci.db.installDB( dbName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )  
        
        # Tries to connect to the DB using the DB DIRAC module
        try:
          self.log.debug( 'Reaching %s/%s' % ( diracSystem, dbName ) )
          db = lhcb_ci.db.getDB( dbName, '%s/%s' % ( diracSystem, dbName ), 10 )
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
    
    Tests that we can install databases on the MySQL server using a DIRAC command
    and drop directly databases from the MySQL server using a SQL statement.
    
    """
   
    self.logTestName( 'test_databases_install_drop' )

    for systemDBs in self.databases.itervalues():   
    
      for dbName in systemDBs:

        res = lhcb_ci.db.installDB( dbName )         
        self.assertDIRACEquals( res[ 'OK' ], True, res )
                
        res = lhcb_ci.db.dropDB( dbName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )  


  def test_import_db_modules( self ):
    """ test_import_db_modules
    
    Tries to import the DB modules and create a class Object. Iterating over all
    databases found in the code, tries to import their modules and instantiate
    one class object.
    
    """
    
    self.logTestName( 'test_import_db_modules' )
   
    for diracSystem, systemDBs in self.databases.iteritems():
      
      for dbName in systemDBs:

        if self.isException( dbName ):
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
    
    This test only applies to databases defined on Python ( some of the databases
    are still using the sql schema definition ).
    
    """
    
    self.logTestName( 'test_install_tables' )
    
    for diracSystem, systemDBs in self.databases.iteritems():
      
      for dbName in systemDBs:
    
        if self.isException( dbName ):
          continue

        # Installs DB
        res = lhcb_ci.db.installDB( dbName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )
        
        # Gets tables of the DB ( if sql schema provided, this if is positive )
        tables = lhcb_ci.db.getTables( dbName )
        self.assertDIRACEquals( tables[ 'OK' ], True, tables )
        
        if tables[ 'Value' ]:
          self.log.exception( 'Tables found for %s/%s' % ( diracSystem, dbName ) )
          self.log.exception( tables[ 'Value' ] )
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
          self.log.exception( 'EXCEPTION: %s NOT FOLLOWING STANDARDS' % dbName )
          res = lhcb_ci.db.dropDB( dbName )
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
        res = lhcb_ci.db.dropDB( dbName )
        self.assertDIRACEquals( res[ 'OK' ], True, res )


  #.............................................................................    
  # Nosetests attrs

  
  # test_databases_reachable
  test_databases_reachable.install = 1
  test_databases_reachable.db      = 1
  
  # test_databases_install_drop
  test_databases_install_drop.install = 1
  test_databases_install_drop.db      = 1
  
  # test_import_db_modules
  test_import_db_modules.install = 1
  test_import_db_modules.db      = 1
  
  # test_install_tables
  test_install_tables.install = 1
  test_install_tables.db      = 1

    
#...............................................................................
#EOF