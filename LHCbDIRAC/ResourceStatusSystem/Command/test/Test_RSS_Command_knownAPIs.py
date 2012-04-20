# $HeadURL: $
''' Test_RSS_Command_knownAPIs
'''

import unittest

__RCSID__ = '$Id: $'

dummyResults = {}
class DummyReturn( object ):
  
  def __init__( self, *args, **kwargs ):
    pass
  def __getattr__( self, name ):
    return self.dummyMethod
#  def __call__( self, *args, **kwargs ):
#    if hasattr( self, 'returnArgs' ) and self.returnArgs is None:
#      return args[ 0 ]
#    return self.dummyMethod()
  def dummyMethod( self, *args, **kwargs ):
    if dummyResults.has_key( self.__class__.__name__ ):
      return dummyResults[ self.__class__.__name__ ]
#    return None

class dDIRACknownAPIs( DummyReturn ): 
  __APIs__ = { 'ResourceManagementClient' : None }

################################################################################

class KnownAPIs_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Command.knownAPIs as moduleTested
    moduleTested.DIRACknownAPIs        = dDIRACknownAPIs() 
    
    self.func = moduleTested.initAPIs
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.func

################################################################################

class KnownAPIs_Success( KnownAPIs_TestCase ):
  
  def test_initAPIs( self ):
    ''' tests initAPIs
    '''

    global dummyResults
    dummyResults[ 'dDIRACknownAPIs' ] = {}
    
    res = self.func( 1, 2, 3 )
    self.assertEquals( res, {} )
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF