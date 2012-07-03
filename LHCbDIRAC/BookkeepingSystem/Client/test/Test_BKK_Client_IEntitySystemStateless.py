# $HeadURL: $
''' Test_BKK_Client_IEntitySystemStateless

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

def dummyCallable( whatever ):
  return whatever

################################################################################

class IEntitySystemStateless_TestCase( unittest.TestCase ):

  def setUp( self ):
    '''
    Setup
    '''

    # We need the proper software, and then we overwrite it.
    import LHCbDIRAC.BookkeepingSystem.Client.LHCB_BKKDBManager   as moduleTested
    moduleTested.gLogger = dummyCallable

    self.iEntity = moduleTested.LHCB_BKKDBManager

  def tearDown( self ):
    '''
    TearDown
    '''
    del self.iEntity

################################################################################

class IEntitySystemStateless_Success( IEntitySystemStateless_TestCase ):

  def test_instantiate( self ):
    ''' tests that we can instantiate one object of the tested class
    '''
    iEntity = self.iEntity()
    self.assertEqual( 'IEntitySystemStateless', iEntity.__class__.__name__ )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF