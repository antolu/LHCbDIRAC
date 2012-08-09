# $HeadURL: $
''' Test_RSS_Command_SAMResultsCommand
'''

import unittest

__RCSID__ = '$Id: $'

dummyResults = {}
class DummyReturn( object ):
  
  def __init__( self, *args, **kwargs ):
    pass
  def __getattr__( self, name ):
    return self.dummyMethod
  def __call__( self, *args, **kwargs ):
    return self.dummyMethod()
  def dummyMethod( self, *args, **kwargs ):
    if dummyResults.has_key( self.__class__.__name__ ):
      return dummyResults[ self.__class__.__name__ ]
    return None

class dS_ERROR( DummyReturn )       : pass  
class dCommand( DummyReturn )       : pass  
class dgetGOCSiteName( DummyReturn ): pass
class dinitAPIs( DummyReturn)       : pass   
  
################################################################################

class SAMResults_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Command.SAMResultsCommand as moduleTested
    moduleTested.S_ERROR        = dS_ERROR
    moduleTested.Command        = dCommand
    moduleTested.getGOCSiteName = dgetGOCSiteName
    moduleTested.initAPIs       = dinitAPIs     
     
    moduleTested.SAMResultsCommand.__bases__ = ( dCommand, ) 
    
    self.command = moduleTested.SAMResultsCommand
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.command  

################################################################################

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  