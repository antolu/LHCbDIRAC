# $HeadURL: $
''' Test_RSS_Client_ResourceManagementClient
'''

import unittest

__RCSID__ = '$Id: $'

dummyResults = {}
class DummyReturn:
  
  def __init__( self, *args, **kwargs ):
    pass
  def __getattr__( self, name ):
    return self.dummyMethod
  def __call__( self, *args, **kwargs ):
    if hasattr( self, 'returnArgs' ) and self.returnArgs is None:
      return args[ 0 ]
    return self.dummyMethod()
  def dummyMethod( self, *args, **kwargs ):
    if dummyResults.has_key( self.__class__.__name__ ):
      if dummyResults[ self.__class__.__name__ ] == 'returnArgs':
        return args
      return dummyResults[ self.__class__.__name__ ]
    return None

class dS_ERROR( DummyReturn ): 
  returnArgs = None
class dgLogger( DummyReturn )                 : pass
class dResourceManagementClient( DummyReturn ):
  returnArgs = None  
class dGate( DummyReturn ): 
  returnArgs = None
  
################################################################################

class ResourceManagementClient_TestCase( unittest.TestCase ):
  
  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient as moduleTested
    moduleTested.S_ERROR                       = dS_ERROR()
    moduleTested.gLogger                       = dgLogger()
    moduleTested.DIRACResourceManagementClient = dResourceManagementClient
     
    moduleTested.ResourceManagementClient.__bases__ = ( dResourceManagementClient, ) 
    
    self.client = moduleTested.ResourceManagementClient
    
  def tearDown( self ):
    '''
    TearDown
    '''
    del self.client 

################################################################################

class ResourceManagementClient_Success( ResourceManagementClient_TestCase ):
  
  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    client = self.client()
    self.assertEqual( 'ResourceManagementClient', client.__class__.__name__ )    

  def test_query( self ):
    ''' tests that we can instantiate one object of the tested class
    '''  
    client = self.client()
    res = client._ResourceManagementClient__query( 1, 2, 3 )
    self.assertEqual( res, '"1" is not a proper gate call' )   
    
    global dummyResults
    dummyResults[ 'DIRACResourceManagementClient' ] = 'returnArgs'
        
    client.gate = dGate()
    res = client._ResourceManagementClient__query( 'insert', 'tableName', { 'self' : 1, 'meta' : False } )
    #raise NameError( res )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF