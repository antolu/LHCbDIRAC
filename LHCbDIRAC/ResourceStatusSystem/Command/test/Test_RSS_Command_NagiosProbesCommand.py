# $HeadURL: $
''' Test_RSS_Command_NagiosProbesCommand

'''

import unittest

__RCSID__ = '$Id: $'

class NagiosProbesCommand_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''
    
    self.command = None
    
  def tearDown( self ):
    '''
    TearDown
    '''
    
    del self.command  

class NagiosProbesCommand_Success( NagiosProbesCommand_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    c = self.command()
    self.assertEqual( 'NagiosProbes_Command', c.__class__.__name__ )    
    

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF