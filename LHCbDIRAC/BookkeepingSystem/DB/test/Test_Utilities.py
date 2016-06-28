"""
It is used to test the Bookkeeping utilities
"""
import unittest

__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.DB.Utilities import enoughParams
from LHCbDIRAC.BookkeepingSystem.DB.Utilities import _ONE
from LHCbDIRAC.BookkeepingSystem.DB.Utilities import _TWO

class UtilitiesTestCase( unittest.TestCase ):
  
  def test_enoughParams( self ):
    
    result = enoughParams( {} )
    self.assertEqual( result, False )
    
    result = enoughParams( {'ReplicaFlag':'Test', 'Visible': 'Test'} )
    self.assertEqual( result, False )
    
    for i in _ONE:
      result = enoughParams( {i:'Test'} )
      self.assertEqual( result, False )
    
    for i in _TWO:
      for j in _TWO:
        result = enoughParams( {i:'Test', j:'Test'} )    
        self.assertEqual( result, False )
    
    result = enoughParams( {'ConfigName':'Test', 'ConfigVersion':'Test', 'Production': 'Test'} )
    self.assertEqual( result, True )
    
    result = enoughParams( {'Production': 'Test', 'ReplicaFlag':'Test', 'Visible': 'Test'} )
    self.assertEqual( result, True )
    
        
if __name__ == '__main__':

  suite = unittest.defaultTestLoader.loadTestsFromTestCase( UtilitiesTestCase )
  testResult = unittest.TextTestRunner( verbosity = 2 ).run( suite )


  
