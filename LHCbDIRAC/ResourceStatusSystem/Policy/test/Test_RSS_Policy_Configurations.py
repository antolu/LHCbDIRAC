# $HeadURL: $
''' Test_RSS_Policy_Configurations
'''

import unittest

__RCSID__ = '$Id: $'

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
      
    self.configurations = configurations
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.configurations  

################################################################################  

class Configurations_Success( Configurations_TestCase ):

  def test_getPolicyParameters( self ):
    ''' test we can execute function getPolicyParameters
    '''
    
    global dummyResults
    
    dummyResults[ 'CS' ] = None
    res = self.configurations.getPolicyParameters()
    self.assertEquals( res, None )
    
    dummyResults[ 'CS' ] = [ 1,2 ]
    res = self.configurations.getPolicyParameters()
    self.assertEquals( res, [ 1, 2 ] )
        
    dummyResults[ 'CS' ] = 'HastaLaVistaBaby'
    res = self.configurations.getPolicyParameters()
    self.assertEquals( res, 'HastaLaVistaBaby' )    
        
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF