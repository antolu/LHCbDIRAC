## $HeadURL: $
#''' Test_RSS_Command_SLSCommand
#
#'''
#
#import unittest
#
#__RCSID__ = '$Id: $'
#
#################################################################################
#
#slsResult = None
#
#def slsCommandFunc( *args, **kwargs ):
#  return slsResult
#
#class Dummy():
#  
#  def __getattr__( self, name ):
#    return slsCommandFunc  
#
#class Command( object ):
#  
#  def __init__( self, args ):
#    self.args = args
#    self.APIs = {}
#    
#  def doCommand( self ):
#    pass 
#
#class CS( object ):
#  
#  @staticmethod
#  def getSEToken( name ):
#    return name
#
#################################################################################
#
#class ClassFunctions_TestCase( unittest.TestCase ):
#  
#  def setUp( self ):
#    '''
#    Setup
#    '''
#
#    # We need the proper software, and then we overwrite it.
#    import LHCbDIRAC.ResourceStatusSystem.Command.SLSCommand as moduleTested   
#    moduleTested.CS = CS()
#      
#    self.func = moduleTested.slsid_of_service
#    
#  def tearDown( self ):
#    '''
#    TearDown
#    '''
#    del self.func  
#
#class SLSStatusCommand_TestCase( unittest.TestCase ):
#  
#  def setUp( self ):
#    '''
#    Setup
#    '''
#
#    # We need the proper software, and then we overwrite it.
#    import LHCbDIRAC.ResourceStatusSystem.Command.SLSCommand as moduleTested
#    moduleTested.Command   = Command  
#    moduleTested.CS        = CS() 
#    moduleTested.SLSClient = Dummy()
#    moduleTested.SLSStatusCommand.__bases__ = ( Command, ) 
#    
#    self.command = moduleTested.SLSStatusCommand
#    
#  def tearDown( self ):
#    '''
#    TearDown
#    '''
#    del self.command  
#
#class SLSLinkCommand_TestCase( unittest.TestCase ):
#  
#  def setUp( self ):
#    '''
#    Setup
#    '''
#
#    # We need the proper software, and then we overwrite it.
#    import LHCbDIRAC.ResourceStatusSystem.Command.SLSCommand as moduleTested
#    moduleTested.Command   = Command 
#    moduleTested.CS        = CS()
#    moduleTested.SLSClient = Dummy()
#    moduleTested.SLSLinkCommand.__bases__ = ( Command, ) 
#    
#    self.command = moduleTested.SLSLinkCommand
#    
#  def tearDown( self ):
#    '''
#    TearDown
#    '''
#    del self.command  
#
#class SLSServiceInfoCommand_TestCase( unittest.TestCase ):
#  
#  def setUp( self ):
#    '''
#    Setup
#    '''
#
#    # We need the proper software, and then we overwrite it.
#    import LHCbDIRAC.ResourceStatusSystem.Command.SLSCommand as moduleTested
#    moduleTested.Command   = Command    
#    moduleTested.CS        = CS()
#    moduleTested.SLSClient = Dummy()
#    moduleTested.SLSServiceInfoCommand.__bases__ = ( Command, ) 
#    
#    self.command = moduleTested.SLSServiceInfoCommand
#    
#  def tearDown( self ):
#    '''
#    TearDown
#    '''
#    del self.command  
#
#################################################################################
#
#class ClassFunctions_Success( ClassFunctions_TestCase ):
# 
#  def test_vobox( self ):
#    
#    res = self.func( 1, '1.1', 'VO-BOX' )
#    self.assertEquals( res, '1_VOBOX' )
#    
#    res = self.func( 1, 'a.b', 'VO-BOX' )
#    self.assertEquals( res, 'b_VOBOX' )
#
#    res = self.func( '1', 'a.b', 'VO-BOX' )
#    self.assertEquals( res, 'b_VOBOX' )
#
#  def test_voms( self ):
#    
#    res = self.func( 1, '1.1', 'VOMS' )
#    self.assertEquals( res, 'VOMS' )
#    
#    res = self.func( 1, 'a@b', 'VOMS' )
#    self.assertEquals( res, 'VOMS' )
#
#    res = self.func( '1', '', 'VOMS' )
#    self.assertEquals( res, 'VOMS' )
#
#  def test_none( self ):
#    
#    res = self.func( 'StorageElement', '1.1', None )
#    self.assertEquals( res, '1.1_1.1' )
#    
#    res = self.func( 'StorageElement', 'a@b', None )
#    self.assertEquals( res, 'a@b_a@b' )
#
#    res = self.func( 'StorageElement', '', None )
#    self.assertEquals( res, '_' )
#
#  def test_castor( self ):
#    
#    res = self.func( 1, '1-1', 'CASTOR' )
#    self.assertEquals( res, 'CASTORLHCB_LHCB1' )
#    
#    res = self.func( 1, 'A-B', 'CASTOR' )
#    self.assertEquals( res, 'CASTORLHCB_LHCBB' )
#    
#    res = self.func( 1, 'a-b', 'CASTOR' )
#    self.assertEquals( res, 'CASTORLHCB_LHCBB' )
#
#    res = self.func( 1, '1_1', 'CASTOR' )
#    self.assertEquals( res, 'CASTORLHCB_LHCB1' )
#    
#    res = self.func( 1, 'A_B', 'CASTOR' )
#    self.assertEquals( res, 'CASTORLHCB_LHCBB' )
#    
#    res = self.func( 1, 'a_b', 'CASTOR' )
#    self.assertEquals( res, 'CASTORLHCB_LHCBB' )
#
#  def test_else( self ):
#    
#    res = self.func( 1, '1.1', 'else' )
#    self.assertEquals( res, '' )
#
#    res = self.func( 1, '1@1', 'else' )
#    self.assertEquals( res, '' )
#
#    res = self.func( 1, '', 'else' )
#    self.assertEquals( res, '' )
#
#
#class SLSStatusCommand_Success( SLSStatusCommand_TestCase ):
#  
#  def test_instantiate( self ):
#    ''' tests that we can instantiate one object of the tested class
#    '''  
#    c = self.command( None )
#    self.assertEqual( 'SLSStatusCommand', c.__class__.__name__ )    
#  
#  def test_doCommand_nok( self ):
#    ''' tests that check execution when S_ERROR is returned by backend
#    '''
#    
#    global slsResult
#    slsResult = { 'OK' : False }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res[ 'Result' ][ 'OK' ], False )
#
#    slsResult = { 'OK' : False, 'Message' : 'TestMessage' }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res[ 'Result' ][ 'OK' ], False )
#
#  def test_doCommand_ok( self ):
#    ''' tests that check execution when S_OK is returned by backend
#    '''
#
#    global slsResult
#    slsResult = { 'OK' : True, 'Value' : { 'Availability' : 1 } }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : 1 } } )
#    
#    slsResult = { 'OK' : True, 'Value' : { 'Availability' : 'A', 2 : 3 } }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : 'A' } } )
#    
#class SLSLinkCommand_Success( SLSLinkCommand_TestCase ):
#  
#  def test_instantiate( self ):
#    ''' tests that we can instantiate one object of the tested class
#    '''  
#    c = self.command( None )
#    self.assertEqual( 'SLSLinkCommand', c.__class__.__name__ )    
#  
#  def test_doCommand_nok( self ):
#    ''' tests that check execution when S_ERROR is returned by backend
#    '''
#    
#    global slsResult
#    slsResult = { 'OK' : False }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res[ 'Result' ][ 'OK' ], False )
#
#    slsResult = { 'OK' : False, 'Message' : 'TestMessage' }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res[ 'Result' ][ 'OK' ], False )
#
#  def test_doCommand_ok( self ):
#    ''' tests that check execution when S_OK is returned by backend
#    '''
#
#    global slsResult
#    slsResult = { 'OK' : True, 'Value' : { 'Weblink' : 1 } }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : 1 } } )
#    
#    slsResult = { 'OK' : True, 'Value' : { 'Weblink' : 'A', 2 : 3 } }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : 'A' } } )    
#
#class SLSServiceInfoCommand_Success( SLSServiceInfoCommand_TestCase ):
#  
#  def test_instantiate( self ):
#    ''' tests that we can instantiate one object of the tested class
#    '''  
#    c = self.command( None )
#    self.assertEqual( 'SLSServiceInfoCommand', c.__class__.__name__ )    
#  
#  def test_doCommand_nok( self ):
#    ''' tests that check execution when S_ERROR is returned by backend
#    '''
#    
#    global slsResult
#    slsResult = { 'OK' : False }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res[ 'Result' ][ 'OK' ], False )
#
#    slsResult = { 'OK' : False, 'Message' : 'TestMessage' }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res[ 'Result' ][ 'OK' ], False )
#
#  def test_doCommand_ok( self ):
#    ''' tests that check execution when S_OK is returned by backend
#    '''
#
#    global slsResult
#    slsResult = { 'OK' : True, 'Value' : None }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : slsResult } )
#    
#    slsResult = { 'OK' : True, 'Value' : 1 }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : slsResult } )  
#
#    slsResult = { 'OK' : True, 'Value' : { 1 : 1 } }
#    c = self.command( [ 'StorageElement', '2', 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : slsResult } )  
#    
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF