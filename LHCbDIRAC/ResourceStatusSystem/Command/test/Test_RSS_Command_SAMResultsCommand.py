## $HeadURL: $
#''' Test_RSS_Command_SAMResultsCommand
#'''
#
#import unittest
#
#__RCSID__ = '$Id: $'
#
#dummyResults = {}
#class DummyReturn( object ):
#  
#  def __init__( self, *args, **kwargs ):
#    pass
#  def __getattr__( self, name ):
#    return self.dummyMethod
#  def __call__( self, *args, **kwargs ):
#    if hasattr( self, 'returnArgs' ) and self.returnArgs is None:
#      return args[ 0 ]
#    return self.dummyMethod()
#  def dummyMethod( self, *args, **kwargs ):
#    if dummyResults.has_key( self.__class__.__name__ ):
#      return dummyResults[ self.__class__.__name__ ]
##    return None
#
#class dS_ERROR( DummyReturn ): returnArgs = None  
#
#class dCommand( DummyReturn ): 
#  def doCommand(self): 
#    pass
#class dgetGOCSiteName( DummyReturn ) : pass
#class dinitAPIs( DummyReturn )       : pass 
#
#class dSAMResultsClient( DummyReturn )    : pass
#class dResourceStatusClient( DummyReturn ): pass 
#  
#################################################################################
#
#class SAMResults_TestCase( unittest.TestCase ):
#  
#  def setUp( self ):
#    '''
#    Setup
#    '''
#
#    # We need the proper software, and then we overwrite it.
#    import LHCbDIRAC.ResourceStatusSystem.Command.SAMResultsCommand as moduleTested
#    moduleTested.S_ERROR        = dS_ERROR()
#    moduleTested.Command        = dCommand
#    moduleTested.getGOCSiteName = dgetGOCSiteName()
#    moduleTested.initAPIs       = dinitAPIs()     
#     
#    moduleTested.SAMResultsCommand.__bases__ = ( dCommand, ) 
#    
#    self.command = moduleTested.SAMResultsCommand
#    
#  def tearDown( self ):
#    '''
#    TearDown
#    '''
#    del self.command  
#
#################################################################################
#
#class SAMResults_Success( SAMResults_TestCase ):
#
#  def test_instantiate( self ):
#    ''' tests that we can instantiate one object of the tested class
#    '''  
#    c = self.command()
#    self.assertEqual( 'SAMResultsCommand', c.__class__.__name__ )
#    
#  def test_doCommand( self ):
#    ''' tests doCommand
#    '''
#    
#    global dummyResults
#    dummyResults[ 'dinitAPIs' ] = { 
#                                   'SAMResultsClient'     : dSAMResultsClient(), 
#                                   'ResourceStatusClient' : dResourceStatusClient()
#                                   }
#    
#    c      = self.command()
#    
#    c.args = ( None, None )  
#    res = c.doCommand()
#    self.assertEquals( res, { 'Result' : 'None is not a valid granularity' } )
#    
#    c.args = ( 'A', None )  
#    res = c.doCommand()
#    self.assertEquals( res, { 'Result' : 'A is not a valid granularity' } )
#
#    c.args = ( 'Site', None )  
#    dummyResults[ 'dgetGOCSiteName' ] = { 'OK' : False, 'Message' : 'Nonono' }
#    res = c.doCommand()
#    self.assertEquals( res, { 'OK' : False, 'Message' : 'Nonono' } )
#
#    c.args = ( 'Site', None )  
#    dummyResults[ 'dgetGOCSiteName' ] = { 'OK' : True, 'Value' : 'Site1' }
#    dummyResults[ 'dSAMResultsClient' ] = { 'OK' : True, 'Value' : 1 }
#    res = c.doCommand()
#    self.assertEquals( res, { 'Result' : { 'OK' : True, 'Value' : 1 } } )
#
#    c.args = ( 'Site', None, None )  
#    dummyResults[ 'dgetGOCSiteName' ] = { 'OK' : True, 'Value' : 'Site1' }
#    dummyResults[ 'dSAMResultsClient' ] = { 'OK' : True, 'Value' : 1 }
#    res = c.doCommand()
#    self.assertEquals( res, { 'Result' : { 'OK' : True, 'Value' : 1 } } )
#
#    c.args = ( 'Site', None, 'SiteName' )  
#    dummyResults[ 'dgetGOCSiteName' ] = { 'OK' : True, 'Value' : 'Site1' }
#    dummyResults[ 'dSAMResultsClient' ] = { 'OK' : True, 'Value' : 1 }
#    res = c.doCommand()
#    self.assertEquals( res, { 'Result' : { 'OK' : True, 'Value' : 1 } } )
#          
#    c.args = ( 'Resource', None )  
#    dummyResults[ 'dgetGOCSiteName' ] = None
#    dummyResults[ 'dResourceStatusClient' ] = { 'OK' : False, 'Message' : 'Notnow' }
#    res = c.doCommand()
#    self.assertEquals( res, { 'OK' : False, 'Message' : 'Notnow' } )
#    
#    c.args = ( 'Resource', None )  
#    dummyResults[ 'dResourceStatusClient' ] = { 'OK' : True, 'Value' : 'SiteR1' }   
#    dummyResults[ 'dSAMResultsClient' ] = { 'OK' : True, 'Value' : 'R1' }
#    res = c.doCommand()
#    self.assertEquals( res, { 'Result' : { 'OK' : True, 'Value' : 'R1' } } )
#    
#    c.args = ( 'Resource', None, 'SiteName2' )  
#    dummyResults[ 'dResourceStatusClient' ] = None
#    dummyResults[ 'dgetGOCSiteName' ] = { 'OK' : False, 'Message' : 'No site R2' }   
#    res = c.doCommand()
#    self.assertEquals( res, { 'OK' : False, 'Message' : 'No site R2' } )
#    
#    c.args = ( 'Resource', None, 'SiteName2' )  
#    dummyResults[ 'dResourceStatusClient' ] = None
#    dummyResults[ 'dgetGOCSiteName' ] = { 'OK' : True, 'Value' : 'SiteR2' }
#    dummyResults[ 'dSAMResultsClient' ] = { 'OK' : True, 'Value' : 'R2' }   
#    res = c.doCommand()
#    self.assertEquals( res, { 'Result' : { 'OK' : True, 'Value' : 'R2' } })
#     
#    c.args = ( 'Resource', None, 'SiteName2' )  
#    dummyResults[ 'dResourceStatusClient' ] = None
#    dummyResults[ 'dgetGOCSiteName' ] = { 'OK' : True, 'Value' : 'SiteR2' }
#    dummyResults[ 'dSAMResultsClient' ] = { 'OK' : False, 'Message' : 'Not R2' }   
#    res = c.doCommand()
#    self.assertEquals( res, { 'Result' : { 'OK' : False, 'Message' : 'Not R2' } }) 
#
#    c.args = ( 'Resource', None, 'SiteName2', None )  
#    dummyResults[ 'dResourceStatusClient' ] = None
#    dummyResults[ 'dgetGOCSiteName' ] = { 'OK' : True, 'Value' : 'SiteR2' }
#    dummyResults[ 'dSAMResultsClient' ] = { 'OK' : False, 'Message' : 'Not R2' }   
#    res = c.doCommand()
#    self.assertEquals( res, { 'Result' : { 'OK' : False, 'Message' : 'Not R2' } })     
#          
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  