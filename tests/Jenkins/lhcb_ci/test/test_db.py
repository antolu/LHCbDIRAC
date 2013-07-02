""" lhcb_ci.test.test_db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

import lhcb_ci.basecase

from DIRAC.Core.Utilities import InstallTools

class Installation_Test( lhcb_ci.basecase.DB_TestCase ):
  
  def test_passwords( self ):
    
    res = InstallTools.getMySQLPasswords()
    self.assertEquals( res[ 'OK' ], True )
  
  def test_databases( self ):
  
    self.databases   
    pass
        
  
  def test_import( self ):
    pass 

#...............................................................................
#EOF