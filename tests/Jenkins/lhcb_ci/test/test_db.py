""" lhcb_ci.test.test_db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci.basecase


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
  
  
  def test_databases_db_reachable( self ):
  
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
          
  def test_import( self ):
  
    self.log.info( 'test_import' )
    
    pass 

#...............................................................................
#EOF