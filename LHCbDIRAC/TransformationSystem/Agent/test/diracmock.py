""" diracmock

This module must be imported before running any unit tests. It provides all the
necessary mocks.

"""

import mock
import unittest

__RCSID__ = "$Id$"

# The following is the check to make sure it is in place
#if mock.__version__ < '1.0.1':
# raise ImportError( 'Too old version of mock, we need %s < 1.0.1' % mock.__version__ )

#...............................................................................
# TestCases
# * regular test case
# * Agents test case

class DIRAC_TestCase( unittest.TestCase ):

  sutPath = ''

  def setUp( self ):

    self.moduleTested = sut( self.sutPath )

  def tearDown( self ):

    del self.moduleTested


class DIRACAgent_TestCase( unittest.TestCase ):

  sutPath = ''

  def setUp( self ):

    self.moduleTested = mockAgent( self.sutPath )

  def tearDown( self ):

    del self.moduleTested
    unmock()


#...............................................................................

def sut( sutPath ):
  """ sut ( Software Under Test )
  Imports the module ( not the class ! ) to be tested and returns it.

  examples:
    >>> sut( 'DIRAC.ResourceStatusSytem.Client.ResourceStatusClient' )

  :Parameters:
    **sutPath** - `str`
    path to the module to be tested

  """

  return __import__( sutPath, globals(), locals(), '*' )


def mockAgentModule():

  patcher  = mock.patch( 'DIRAC.Core.Base.AgentModule.AgentModule', autospec = True )
  # The patcher needs to be started...
  pStarted = patcher.start()
  # Why all this ugliness ?
  # Python is a tricky bastard that allows you doing everything you want except
  # using a mock class as a base class. We create a REAL python class, and then
  # once one object is instantiated, we overwrite all methods and class variables
  # from the mock into the REAL class
  class AgentModule():
    def __init__( self, *args, **kwargs ):
      self.log = mock.Mock()
  for k, v in pStarted.__dict__.iteritems():
    setattr( AgentModule, k, v )

  return AgentModule


def mockAgent( sutPath ):

  moduleTested = sut( sutPath )
  agentClass   = getattr( moduleTested, sutPath.split( '.' )[ -1 ] )

  if hasattr( moduleTested, 'AgentModule' ):

    AgentModule = mockAgentModule()

    moduleTested.AgentModule = AgentModule

    baseClassBuffer = []

    for baseClass in agentClass.__bases__:
      if baseClass.__name__ == 'AgentModule':
        baseClassBuffer.append( AgentModule )
      else:
        baseClassBuffer.append( baseClass )

    newBases = tuple( baseClassBuffer )
    if agentClass.__bases__ != newBases:
      agentClass.__bases__ = newBases

  else:
    for baseClass in agentClass.__bases__:
      mockAgent( baseClass.__module__ )

  return moduleTested


def unmock():

  mock.patch.stopall()

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
