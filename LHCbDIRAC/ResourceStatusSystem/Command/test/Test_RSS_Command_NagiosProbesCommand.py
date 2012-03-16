# $HeadURL: $
''' Test_RSS_Command_NagiosProbesCommand

'''

import unittest

__RCSID__ = '$Id: $'

################################################################################

result = None

def nagiosProbesCommandFunc( *args, **kwargs ):
  return result#kwargs.pop( 'result' )

class Dummy():
    
  def __getattr__( self, name ):
    return dummyFunc  
     
  dummyFunc = nagiosProbesCommandFunc

def initAPIs( desiredAPIs, knownAPIs, force = False ):
  
  return { 'ResourceManagementClient' : Dummy() }

class Command( object ):
  
  def __init__( self, args ):
    self.args = args
    self.APIs = {}
    
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
    moduleTested.Command  = Command
    moduleTested.initAPIs = initAPIs      
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
    c = self.command( None )
    self.assertEqual( 'NagiosProbesCommand', c.__class__.__name__ )    
  
  def test_doCommand_nok( self ):
    
    result = { 'OK' : False }
    c = self.command( [ 1, 2, 3 ] )
    res = c.doCommand()
    self.assertEqual( { 'Result' : res }, result )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF