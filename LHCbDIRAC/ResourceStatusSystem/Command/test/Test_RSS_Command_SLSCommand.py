# $HeadURL: $
''' Test_RSS_Command_SLSCommand

'''

import unittest

__RCSID__ = '$Id: $'

################################################################################

slsResult = None

def slsCommandFunc( *args, **kwargs ):
  return slsResult

class Dummy():
  
  def __getattr__( self, name ):
    return slsCommandFunc  

class Command( object ):
  
  def __init__( self, args ):
    self.args = args
    self.APIs = {}
    
  def doCommand( self ):
    pass 

class CS( object ):
  
  @staticmethod
  def getSEToken( name ):
    return name

################################################################################

class SLSStatusCommand_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Command.SLSCommand as moduleTested
    moduleTested.Command   = Command  
    moduleTested.CS        = CS() 
    moduleTested.SLSClient = Dummy()
    moduleTested.SLSStatusCommand.__bases__ = ( Command, ) 
    
    self.command = moduleTested.SLSStatusCommand
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.command  

class SLSLinkCommand_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Command.SLSCommand as moduleTested
    moduleTested.Command   = Command 
    moduleTested.CS        = CS()
    moduleTested.SLSClient = Dummy()
    moduleTested.SLSLinkCommand.__bases__ = ( Command, ) 
    
    self.command = moduleTested.SLSLinkCommand
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.command  

class SLSServiceInfoCommand_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Command.SLSCommand as moduleTested
    moduleTested.Command   = Command    
    moduleTested.CS        = CS()
    moduleTested.SLSClient = Dummy()
    moduleTested.SLSServiceInfoCommand.__bases__ = ( Command, ) 
    
    self.command = moduleTested.SLSServiceInfoCommand
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.command  

################################################################################

class SLSStatusCommand_Success( SLSStatusCommand_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    c = self.command( None )
    self.assertEqual( 'SLSStatusCommand', c.__class__.__name__ )    
  
  def test_doCommand_nok( self ):
    ''' tests that check execution when S_ERROR is returned by backend
    '''
    
    global slsResult
    slsResult = { 'OK' : False }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res[ 'Result' ][ 'OK' ], False )

    global slsResult
    slsResult = { 'OK' : False, 'Message' : 'TestMessage' }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res[ 'Result' ][ 'OK' ], False )

  def test_doCommand_ok( self ):
    ''' tests that check execution when S_OK is returned by backend
    '''

    global slsResult
    slsResult = { 'OK' : True, 'Value' : { 'Availability' : 1 } }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : 1 } } )
    
    global slsResult
    slsResult = { 'OK' : True, 'Value' : { 'Availability' : 'A', 2 : 3 } }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : 'A' } } )
    
class SLSLinkCommand_Success( SLSLinkCommand_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    c = self.command( None )
    self.assertEqual( 'SLSLinkCommand', c.__class__.__name__ )    
  
  def test_doCommand_nok( self ):
    ''' tests that check execution when S_ERROR is returned by backend
    '''
    
    global slsResult
    slsResult = { 'OK' : False }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res[ 'Result' ][ 'OK' ], False )

    global slsResult
    slsResult = { 'OK' : False, 'Message' : 'TestMessage' }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res[ 'Result' ][ 'OK' ], False )

  def test_doCommand_ok( self ):
    ''' tests that check execution when S_OK is returned by backend
    '''

    global slsResult
    slsResult = { 'OK' : True, 'Value' : { 'Weblink' : 1 } }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : 1 } } )
    
    global slsResult
    slsResult = { 'OK' : True, 'Value' : { 'Weblink' : 'A', 2 : 3 } }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : 'A' } } )    

class SLSServiceInfoCommand_Success( SLSServiceInfoCommand_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    c = self.command( None )
    self.assertEqual( 'SLSServiceInfoCommand', c.__class__.__name__ )    
  
  def test_doCommand_nok( self ):
    ''' tests that check execution when S_ERROR is returned by backend
    '''
    
    global slsResult
    slsResult = { 'OK' : False }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res[ 'Result' ][ 'OK' ], False )

    global slsResult
    slsResult = { 'OK' : False, 'Message' : 'TestMessage' }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res[ 'Result' ][ 'OK' ], False )

  def test_doCommand_ok( self ):
    ''' tests that check execution when S_OK is returned by backend
    '''

    global slsResult
    slsResult = { 'OK' : True, 'Value' : None }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res, { 'Result' : slsResult } )
    
    global slsResult
    slsResult = { 'OK' : True, 'Value' : 1 }
    c = self.command( [ 1, 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res, { 'Result' : slsResult } )  

    global slsResult
    slsResult = { 'OK' : True, 'Value' : { 1 : 1 } }
    c = self.command( [ 'StorageElement', 2, 3 ] ) 
    res    = c.doCommand()
    self.assertEqual( res, { 'Result' : slsResult } )  
    
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF