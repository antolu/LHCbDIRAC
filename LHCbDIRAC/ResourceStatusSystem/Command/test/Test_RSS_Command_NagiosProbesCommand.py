# $HeadURL: $
''' Test_RSS_Command_NagiosProbesCommand

'''

#import unittest
#
__RCSID__ = "$Id$"
#
#################################################################################
#
#forcedResult = None
#
#def nagiosProbesCommandFunc( *args, **kwargs ):
#  return forcedResult
#
#class Dummy():
#    
#  def __getattr__( self, name ):
#    return nagiosProbesCommandFunc  
#
#def initAPIs( desiredAPIs, knownAPIs, force = False ):
#  
#  return { 'ResourceManagementClient' : Dummy() }
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
#################################################################################
#
#class NagiosProbesCommand_TestCase( unittest.TestCase ):
#  
#  def setUp( self ):
#    '''
#    Setup
#    '''
#
#    # We need the proper software, and then we overwrite it.
#    import LHCbDIRAC.ResourceStatusSystem.Command.NagiosProbesCommand as moduleTested
#    moduleTested.Command  = Command
#    moduleTested.initAPIs = initAPIs      
#    moduleTested.NagiosProbesCommand.__bases__ = ( Command, ) 
#    
#    self.command = moduleTested.NagiosProbesCommand
#    
#  def tearDown( self ):
#    '''
#    TearDown
#    '''
#    del self.command  
#
#class NagiosProbesCommand_Success( NagiosProbesCommand_TestCase ):
#  
#  def test_instantiate( self ):
#    ''' tests that we can instantiate one object of the tested class
#    '''  
#    c = self.command( None )
#    self.assertEqual( 'NagiosProbesCommand', c.__class__.__name__ )    
#  
#  def test_doCommand_nok( self ):
#    ''' tests that check execution when S_ERROR is returned by backend
#    '''
#    
#    global forcedResult
#    forcedResult = { 'OK' : False }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : forcedResult  } )
#
#    global forcedResult
#    forcedResult = { 'OK' : False, 'Message' : 'TestMessage' }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : forcedResult  } )
#
#
#  def test_doCommand_ok( self ):
#    ''' tests that check execution when S_OK is returned by backend
#    '''
#
#    global forcedResult
#    forcedResult = { 'OK' : True, 'Value' : [] }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : {} } } )
#    
#    global forcedResult
#    forcedResult = { 'OK' : True, 'Value' : [ [1,2,3] ] }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : { 1 : [ 2,3 ]} } } )
#    
#    global forcedResult
#    forcedResult = { 'OK' : True, 'Value' : [ [1,2,3], [4,5,6] ] }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : { 
#                                                                  1 : [ 2,3 ],
#                                                                  4 : [ 5,6 ],
#                                                                  } } } )
#
#    global forcedResult
#    forcedResult = { 'OK' : True, 'Value' : [ [1,2,3], ['a','b','c'] ] }
#    c = self.command( [ 1, 2, 3 ] ) 
#    res    = c.doCommand()
#    self.assertEqual( res, { 'Result' : { 'OK' : True, 'Value' : { 
#                                                                  1   : [ 2,3 ],
#                                                                  'a' : [ 'b','c' ],
#                                                                  } } } )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF