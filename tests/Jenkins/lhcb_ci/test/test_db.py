""" lhcb_ci.test.test_db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

import collections
import os
import unittest


from DIRAC.Core.Utilities import InstallTools


class SystemDB_TestCase( unittest.TestCase ):
  
  def setUp( self ):

    workspace = os.environ.get( 'WORKSPACE', '' )
    
    print workspace
    
    self.databases = collections.defaultdict( set )   
       
    with open( os.path.join( workspace, 'databases' ), 'r' ) as f:
      db_line = f.readline()  

      print db_line
      
      if db_line:
      
        system, dbName = db_line.split( '' )
        self.databases[ system ].update( dbName.split( '.' )[ 0 ] )
  
  def tearDown( self ):
    
    del self.databases  
      
class Basic_Test( SystemDB_TestCase ):
  
  def test_passwords( self ):
    
    res = InstallTools.getMySQLPasswords()
    self.assertEquals( res[ 'OK' ], True )
  
  def test_databases( self ):
    
    pass
        
  
  def test_import( self ):
    pass 

#...............................................................................
#EOF