''' Test_RSS_Policy_Configurations
'''

import unittest

__RCSID__ = "$Id$"

dummyResults = {}
class DummyReturn( object ):
  
  def __init__( self, *args, **kwargs ):
    pass
  def __getattr__( self, name ):
    return self.dummyMethod
  def dummyMethod( self, *args, **kwargs ):
    if dummyResults.has_key( self.__class__.__name__ ):
      return dummyResults[ self.__class__.__name__ ]
    return None
  
class dCS( DummyReturn ): pass 


################################################################################

class Configurations_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Policy.Configurations as moduleTested   
    moduleTested.CS = dCS()
      
    self.configurations = moduleTested
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.configurations
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF