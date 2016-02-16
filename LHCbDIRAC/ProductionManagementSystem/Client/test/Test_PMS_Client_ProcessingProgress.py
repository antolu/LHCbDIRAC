''' Test_PMS_Client_ProcessingProgress

'''

import unittest

__RCSID__ = '$Id: $'

dummyResults = {}
class DummyReturn():
  
  def __init__( self, *args, **kwargs ):
    pass
  def __getattr__( self, name ):
    return self.dummyMethod
  def dummyMethod( self, *args, **kwargs ):
    return dummyResults[ self.__class__.__name__ ]

class dTable( DummyReturn )  : pass

################################################################################

class HTMLProgressTable_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ProductionManagementSystem.Client.ProcessingProgress as moduleTested
    
    self.progress = moduleTested.HTMLProgressTable
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.progress
    
################################################################################

class HTMLProgressTable_Success( HTMLProgressTable_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    
    global dummyResults
    dummyResults[ 'dTable'   ] = None
    
    progress = self.progress( 'processingPass' )
    self.assertEqual( 'HTMLProgressTable', progress.__class__.__name__ )  

################################################################################    
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF    