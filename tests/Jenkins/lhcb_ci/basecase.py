""" lhcb_ci.basecase

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import lhcb_ci
import lhcb_ci.db
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
    
#...............................................................................
#EOF