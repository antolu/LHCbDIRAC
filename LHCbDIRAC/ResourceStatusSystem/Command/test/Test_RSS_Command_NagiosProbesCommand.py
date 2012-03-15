# $HeadURL: $
''' Test_RSS_Command_NagiosProbesCommand

'''

import unittest

__RCSID__ = '$Id: $'

################################################################################

def nagiosProbesCommandFunc( *args, **kwargs ):
  return kwargs.pop( 'result' )

def initAPIS( desiredAPIs, knownAPIs, force = False ):
  
  class Dummy():
    
    def __getattr__( self, name ):
      return dummyFunc  
    
    dummyFunc = nagiosProbesCommandFunc
  
  return { 'ResourceManagementClient' : Dummy() }

class Command( object ):
  
  def __init__( self, args ):
    self.args = args
    
  def doCommand( self ):
    pass 

################################################################################

class NagiosProbesCommand_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Command.NagiosProbesCommand as moduleTested
    moduleTested.Command = Command   
    moduleTested.NagiosProbesCommand.__bases__ = ( Command, ) 
    
    self.command = moduleTested.NagiosProbesCommand
    
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
    self.assertEqual( 'NagiosProbesCommand', c.__class__.__name__ )    
    

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF