''' AgentModule  
'''

__RCSID__ = "$Id:$"

import mock

# We need a version of mock which it is not currently shipped with the externals
# We have to download it using:
# % source bashrc 
# % python `which easy_install` mock

# The following is the check to make sure it is in place
if mock.__version__ < '1.0.1':
  raise ImportError( 'Too old version of mock, we need %s < 1.0.1' % mock.__version__ )

#...............................................................................
# Import Software Under Test ( sut )

# Import before starting the patchers the Software Under Test ! Otherwise, nothing
# will work. This is VERY important.
import ExampleModule as sut

#...............................................................................
# Create patchers and mocked base class 

# Import the AgentModule class with mock.patch and the autospec option so that
# it copies the AgentModule specification ( mimics all methods ! )
patcher  = mock.patch( 'DIRAC.Core.Base.AgentModule.AgentModule', 
                       autospec = True )
# The patcher needs to be started...
pStarted = patcher.start()

# Why all this ugliness ?
# Python is a tricky bastard that allows you doing everything you want except
# using a mock class as a base class. We create a REAL python class, and then
# once one object is instantiated, we overwrite all methods and class variables
# from the mock into the REAL class
      
class AgentModule():
  def __init__( self, *args, **kwargs ): pass
for k,v in pStarted.__dict__.iteritems():
  setattr( AgentModule, k, v )
      
#...............................................................................
# Let's use a dummy example to show how to mock an agent.

class ExampleAgent( AgentModule ):
  '''
    This ExampleAgent is inheriting from the mocked base class we have just defined.
    We can do whatever we want with it, there is no real connection with the DIRAC
    AgentModule, appart that the spec is the same !
  '''
  def __init__( self, agentName, loadName, baseAgentName = False, properties = {} ):
    ''' constructor
    '''   
    AgentModule.__init__( self, agentName, loadName, baseAgentName, properties )
  def initialize( self ):
    ''' intialize
    '''
    return 'initialize'  
  def execute( self ):
    ''' execute
    '''
    # ...
    return 'execute'
  def finalize( self ):
    ''' finalize ( this method is mocked but the parent class !!! )
    '''
    #...
    return 'finalize'
 
#...............................................................................
# Now we can instantiate our agent without bothering what is AgentModule doing
# behind the scenes

ea = ExampleAgent( 'agentName', 'loadName' )

res = ea.initialize()
if res != 'initialize':
  raise AssertionError( 'wrong initialize return: %s' % res )

res = ea.execute()
if res != 'execute':
  raise AssertionError( 'wrong execute return: %s' % res )

res = ea.finalize()
if res != 'finalize':
  raise AssertionError( 'wrong finalize return: %s' % res )


# We have not defined it, but the patched did it for us !
# its signature is
# def am_initialize( self, *initArgs )
if not hasattr( ea, 'am_initialize' ):
  raise AssertionError( 'Not a proper mock !' )

try:
  _ = ea.am_initialize()
except TypeError:
  raise AssertionError( 'Not a proper mock on empty call' )  

try:
  _ = ea.am_initialize( 1, 2, 3, 4, 5 )
except TypeError:
  raise AssertionError( 'Not a proper mock on *args call' )  

try:
  _ = ea.am_initialize( 1, 4, a = 1, b = 2 )
  raise AssertionError( 'Not a proper mock on *kwargs call' )
except TypeError:
  # our code should have crashed
  pass

# Sweet, isn't it ?

#...............................................................................
# That was easy, but what if ExampleAgent is on another module, for example,
# ExampleModule, as it is ALWAYS the case. When testing, we do not want to touch
# the original file, but 'hack' few modules here and there to do whatever we
# want.

# Remember we had already imported sut ?
# DIRAC.Core.Base.... it is still the REAL one
if not str( sut.AgentModule ) == 'DIRAC.Core.Base.AgentModule.AgentModule':
  raise AssertionError( 'AgentModule is %s' % str( sut.AgentModule ) )

try:
  _ = sut.ExampleAgent( '','' )
  raise AssertionError( 'This instantiation should have crashed... as we still have the real sut' )
except RuntimeError:
  # As expected, we are using the original AgentModule
  pass

# We need to overwrite both, otherwise this will not work. Keep this ALWAYS on mind !
sut.AgentModule            = AgentModule
sut.ExampleAgent.__bases__ = ( AgentModule, )

# DIRAC.Core.Base.... now is different !
if not str( sut.AgentModule ) == '__main__.AgentModule':
  raise AssertionError( 'AgentModule is %s' % str( sut.AgentModule ) )  

# Let's create a new-mocked-bases ExampleAgent 
ea2 = sut.ExampleAgent( '', '' )

res = ea2.initialize()
if not res[ 'OK' ]:
  print res
  raise AssertionError( 'initialize should be OK' )

res = ea2.execute()
if not res[ 'OK' ]:
  print res
  raise AssertionError( 'execution should be OK' )

# We have not defined it, but the patched did it for us !
# its signature is
# def am_initialize( self, *initArgs )
if not hasattr( ea2, 'am_initialize' ):
  raise AssertionError( 'Not a proper mock !' )

try:
  _ = ea2.am_initialize()
except TypeError:
  raise AssertionError( 'Not a proper mock on empty call' )

try:
  _ = ea2.am_initialize( 1, 2, 3, 4, 5 )
except TypeError:
  raise AssertionError( 'Not a proper mock on *args call' )
  
try:
  _ = ea2.am_initialize( 1, 4, a = 1, b = 2 )
  raise AssertionError( 'Not a proper mock on *kwargs call' )
except TypeError:
  # our code should have crashed
  pass

#...............................................................................
# This is fun, do not mess with reload

try:
  reload( sut )
  raise AssertionError( 'Should have raised a RuntimeError' )
except RuntimeError:
  # Do not import / reload sut if patchers are active !
  pass  

# Stop patchers
mock.patch.stopall()

# Let's reload sut and voila, AgentModule is the original one again
reload( sut )
if not str( sut.AgentModule ) == 'DIRAC.Core.Base.AgentModule.AgentModule':
  raise AssertionError( 'AgentModule is %s' % str( sut.AgentModule ) )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF