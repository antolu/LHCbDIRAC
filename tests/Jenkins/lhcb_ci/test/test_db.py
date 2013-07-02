""" lhcb_ci.test.test_db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

import collections
import logging
import os
import unittest

from DIRAC.Core.Utilities import InstallTools

class SystemDB_TestCase( unittest.TestCase ):
  
  def setUp( self ):

    self.log = logging.getLogger( self.__class__.__name__ )

    if 'DEBUGMODE' in os.environ: 
      level = logging.DEBUG
    else:
      level = logging.INFO
        
    self.log.setLevel( level )

    workspace = os.environ.get( 'WORKSPACE', '' )
    logging.info( 'WORKSPACE: %s' % workspace )
    
    self.databases = collections.defaultdict( set )   
       
    with open( os.path.join( workspace, 'databases' ), 'r' ) as f:
      db_line = f.readline()
      
      logging.info( db_line )
      
      if db_line:
      
        system, dbName = db_line.split( ' ' )
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