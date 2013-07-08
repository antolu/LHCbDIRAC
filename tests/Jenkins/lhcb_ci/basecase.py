""" lhcb_ci.basecase

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import collections
import lhcb_ci
import os
import unittest


#from DIRAC.ConfigurationSystem.Client.LocalConfiguration import LocalConfiguration


class Base_TestCase( unittest.TestCase ):  

  log = lhcb_ci.logger
  
  @classmethod
  def setUpClass( cls ):

#    localCfg = LocalConfiguration()
#    localCfg.isCSEnabled()
#    localCfg.disableParsingCommandLine()
    
    cls.workspace = os.getenv( 'WORKSPACE' ) 
  
  
  def setUp( self ):
    self.log.info( '*** %s ***' % self.__class__.__name__ )  
    
    
class DB_TestCase( Base_TestCase ):
  
  @classmethod
  def setUpClass( cls ):

    super( DB_TestCase, cls ).setUpClass()
    cls.log.info( '=== DB_TestCase ===' )
    
    cls.databases = collections.defaultdict( set )   
       
    with open( os.path.join( cls.workspace, 'databases' ), 'r' ) as f:
      db_data = f.read().split( '\n' )
    
    for db_line in db_data:
      
      if not db_line:
        continue
      
      cls.log.info( db_line )
    
      system, dbName = db_line.split( ' ' )
      cls.databases[ system ].update( [ dbName.split( '.' )[ 0 ] ] )  

    with open( os.path.join( cls.workspace, 'rootMySQL' ), 'r' ) as f:  
      cls.rootPass = f.read().split( '\n' )[ 0 ]
    
    with open( os.path.join( cls.workspace, 'userMySQL' ), 'r' ) as f:
      cls.userPass = f.read().split( '\n' )[ 0 ]  

#...............................................................................
#EOF