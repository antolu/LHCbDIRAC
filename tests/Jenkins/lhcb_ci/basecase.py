""" lhcb_ci.basecase

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

import datetime
import inspect
import os
import unittest


import lhcb_ci.agent
import lhcb_ci.db
import lhcb_ci.exceptions
import lhcb_ci.links
import lhcb_ci.service


from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration

def time_test( test ):
  
  def wrapper( *args, **kwargs ):
    
    start = datetime.datetime.utcnow()
    result = test( *args, **kwargs )
    end = datetime.datetime.utcnow()
    
    timmings = os.path.join( lhcb_ci.reports, 'timmings.txt' )
    tFile    = open( timmings, 'a' )
    tFile.write( test.__name__ )
    tFile.write( '\n start %s' % start )
    tFile.write( '\n end   %s\n' % end )
    tFile.close()
    
    return result
    
  return wrapper  
    


class Base_TestCase( unittest.TestCase ):
  """ Base_TestCase
  
  BaseCase extending unittests used by lhcb_ci tests. It sets a logger with two
  handlers: file and stream for errors. It also provides automatic exception detection
  making use of the exceptions module.
  
  """

  log        = lhcb_ci.logger
  exceptions = None
    
  
  def logTestName( self ):
    """ logTestName
    
    Prints a header with the test name.
    
    """
    
    self.log.debug( '.' * 80 )
    self.log.info( self.__testMethod() )
    self.log.debug( '.' * 80 )
  
  
  def __testMethod( self ):
    """ testMethod
    
    Inspects the calls stack to get the name of the caller method. Use it carefully,
    if you add more calls to the stack, the indexes may not work !
    
    """
    
    return inspect.stack()[2][3]
    
    
  @classmethod
  def setUpClass( cls ):
    """ setUpClass
    
    When a test is loaded, the local configuration is loaded and parsed to detect
    proxies and environment variables.
    
    """

    # Print separator
    cls.log.info( '=' * 80 )
    cls.log.info( 'setup test')

    localCfg = LocalConfiguration()
    localCfg.isParsed = True
    localCfg.loadUserData()
    
    cls.workspace = lhcb_ci.db.workspace


  @classmethod
  def tearDownClass( cls ):
    """ tearDownClass
    
    Nothing special, just a separator.
    
    """
    
    # Print separator
    cls.log.info( '#' * 80 )
     
  
  def setUp( self ):
    """ setUp
    
    Prints a header with the class name and loads exceptions using the MODULE
    name, not the CLASS name.
    
    """
    
    self.log.debug( '-' * 80 )
    self.log.debug( self.__class__.__name__ )  
    self.log.debug( '-' * 80 )

    self.exceptions = getattr( lhcb_ci.exceptions, self.__module__, {} )

    self.currentThreads_, self.activeThreads_ = lhcb_ci.commons.trackThreads()


  def tearDown( self ):
    """ tearDown
    
    Makes sure there are no threads left running
    
    """

    threadsAfterPurge = lhcb_ci.commons.killThreads( self.currentThreads_ )
    if not threadsAfterPurge == self.activeThreads_:
      self.fail( 'Not all threads down' )


  def assertDIRACEquals( self, first, second, res ):
    """ assertDIRACEquals
    
    This is a wrapper around unittest.assertEquqls that makes use of res[ 'Message ' ]
    if there is one.
        
    """
    
    _message = ( not res[ 'OK' ] and res[ 'Message' ] ) or ''   
    self.assertEquals( first, second, _message )
    
  
  def reportPath( self ):
    """ reportPath 
    
    Returns a path where to write the report.
    """

    return os.path.join( lhcb_ci.reports, '%s.txt' % self.__testMethod() )
        
    
  def isException( self, value ):
    """ isException
    
    Given a value and the already loaded exceptions dictionary, decides whether
    it is an exception or not.
    
    """
    
    try:
      if value in self.exceptions[ self.__testMethod() ]:
        self.log.exception( 'EXCEPTION: skipped %s' % value )
        return True
    except KeyError:
      pass
        
    return False    
    
    
class DB_TestCase( Base_TestCase ):
  """ DB_TestCase
  
  TestCase for database related tests. It parses databases file and transforms it
  into a dictionary ( databases ). Similarly, MySQL passwords are parsed from files. 
  
  """

  
  @classmethod
  def setUpClass( cls ):
    """ setUpClass
    
    Prints a little header, and parses files with relevant information which will
    be used on the tests.
    
    """

    super( DB_TestCase, cls ).setUpClass()
    cls.log.debug( '::: DB_TestCase setUpClass :::' )
    
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
    
    super( DB_TestCase, self ).tearDown()
    
    #FIXME: apart from failing, we should wipe them out 
    res = lhcb_ci.db.getInstalledDBs()
    if not res[ 'OK' ]:
      self.log.error( 'tearDown' )
      self.fail( res[ 'Message' ] )
    
    if res[ 'Value' ]:
      self.log.error( 'tearDown' )
      self.fail( 'DBs still installed: %s' % res[ 'Value' ] )


class Service_TestCase( DB_TestCase ):
  """ Service_TestCase
  
  TestCase for service related tests. It discovers the service modules in the
  code from a quick inspection of *Handler.py
  
  """

  @classmethod
  def setUpClass( cls ):
    """ setUpClass
    
    Prints a little header and discovers tests.
    
    """

    super( Service_TestCase, cls ).setUpClass()
    cls.log.info( '::: Service_TestCase setUpClass :::' )
    
    cls.swServices = lhcb_ci.service.getSoftwareServices()
    

  def setUp( self ):
    """ setUp
    
    Makes sure there are no Services installed before starting the test.
    
    """
    
    super( Service_TestCase, self ).setUp()
    
    installedServices = lhcb_ci.service.getInstalledServices()  
    
    # Configuration Service is ALWAYS installed ( Master ! )
    del installedServices[ 'Configuration' ]
      
    if installedServices:
      self.log.error( 'setUp' )
      self.fail( 'Services still installed: %s' % installedServices )  

    
  def tearDown( self ):
    """ tearDown
    
    Makes sure there are no Services installed after the test.
    
    """
    
    super( Service_TestCase, self ).tearDown()
    
    installedServices = lhcb_ci.service.getInstalledServices()
   
    # Configuration Service is ALWAYS installed ( Master ! )
    del installedServices[ 'Configuration' ]
    
    if installedServices:
      self.log.error( 'tearDown' )
      self.fail( 'Services still installed: %s' % installedServices )


class Agent_TestCase( Service_TestCase ):
  """ Agent_TestCase
  
  TestCase for agent related tests. It discovers the agent modules in the
  code from a quick inspection of *Agent.py
  
  """
  
  @classmethod
  def setUpClass( cls ):
    """ setUpClass
    
    Prints a little header and discovers tests.
    
    """

    super( Agent_TestCase, cls ).setUpClass()
    cls.log.info( '::: Agent_TestCase setUpClass :::' )
    
    cls.swAgents = lhcb_ci.agent.getSoftwareAgents()  


  def setUp( self ):
    """ setUp
    
    Makes sure there are no Agents installed before starting the test.
    
    """
    
    super( Agent_TestCase, self ).setUp()
    
    installedAgents = lhcb_ci.agent.getInstalledAgents()  
      
    if installedAgents:
      self.log.error( 'setUp' )
      self.fail( 'Agents still installed: %s' % installedAgents )  

    
  def tearDown( self ):
    """ tearDown
    
    Makes sure there are no Agents installed after the test.
    
    """
    
    super( Agent_TestCase, self ).tearDown()
    
    installedAgents = lhcb_ci.agent.getInstalledAgents()
   
    if installedAgents:
      self.log.error( 'tearDown' )
      self.fail( 'Agents still installed: %s' % installedAgents )
  

class Client_TestCase( Agent_TestCase ): 
  """ Chain_TestCase
  
  TestCase for client-service-db related tests. It discovers the client modules 
  in the code from a quick inspection of *Client.py
  
  """
 
 
  SUT = ''
  

  @classmethod
  def setUpClass( cls ):
    """ setUpClass
    
    Prints a little header and discovers tests.
    
    """

    super( Client_TestCase, cls ).setUpClass()
    cls.log.info( '::: Client_TestCase setUpClass :::' )


  def setUp( self ):
    """ setUp
    
    Makes sure there is nothing installed before starting the test.
    
    """
    
    super( Client_TestCase, self ).setUp()
    
    self.chain = lhcb_ci.links.Link( self.SUT )
    
    #self.log.info( self.databases )
    #self.log.info( self.swServices )
    
    self.chain.reset( self.databases, self.swServices )
    self.chain.build()
    
    sutMod      =  lhcb_ci.extensions.import_( 'DIRAC.' + self.SUT )
    self.sutCls = getattr( sutMod, self.SUT.split('.')[ -1 ] )
    
    
  def tearDown( self ):
    """ tearDown
    
    Makes sure there is nothing installed after the test.
    
    """
    
    self.chain.destroy()
    
    super( Client_TestCase, self ).tearDown()
    
    
#...............................................................................
#EOF
