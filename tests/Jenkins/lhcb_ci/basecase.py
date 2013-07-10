""" lhcb_ci.basecase

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci
import lhcb_ci.db
import lhcb_ci.service
import unittest


from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration


class Base_TestCase( unittest.TestCase ):  

  log = lhcb_ci.logger
  
  
  @classmethod
  def setUpClass( cls ):

    localCfg = LocalConfiguration()
    localCfg.isParsed = True
    localCfg.loadUserData()
    
    cls.workspace = lhcb_ci.db.workspace
  
  
  def setUp( self ):
    self.log.info( '*** %s ***' % self.__class__.__name__ )  
    
    
class DB_TestCase( Base_TestCase ):

  
  @classmethod
  def setUpClass( cls ):

    super( DB_TestCase, cls ).setUpClass()
    cls.log.info( '=== DB_TestCase ===' )
    
    cls.databases = lhcb_ci.db.getDatabases()         
    cls.rootPass  = lhcb_ci.db.getRootPass()
    cls.userPass  = lhcb_ci.db.getUserPass()

  
  def setUp( self ):
    """ setUp
    
    Makes sure there are no DBs installed before starting the test.
    """
    
    super( DB_TestCase, self ).setUp()
    
    res = lhcb_ci.db.getInstalledDBs()  
    if not res[ 'OK' ]:
      self.log.error( 'setUp' )
      self.fail( res[ 'Message' ] )
      
    if res[ 'Value' ]:
      self.log.error( 'setUp' )
      self.fail( 'DBs still installed: %s' % res[ 'Value' ] )  

    
  def tearDown( self ):
    """ tearDown
    
    Makes sure there are no DBs installed after the test.
    """
    
    res = lhcb_ci.db.getInstalledDBs()
    if not res[ 'OK' ]:
      self.log.error( 'tearDown' )
      self.fail( res[ 'Message' ] )
    
    if res[ 'Value' ]:
      self.log.error( 'tearDown' )
      self.fail( 'DBs still installed: %s' % res[ 'Value' ] )      


  def assertDIRACEquals( self, first, second, res ):
    
    _message = ( not res[ 'OK' ] and res[ 'Message' ] ) or ''   
    self.assertEquals( first, second, _message )


class Service_TestCase( Base_TestCase ):  

  @classmethod
  def setUpClass( cls ):

    super( Service_TestCase, cls ).setUpClass()
    cls.log.info( '=== Service_TestCase ===' )
    
    cls.services = lhcb_ci.service.getCodedServices()

#...............................................................................
#EOF