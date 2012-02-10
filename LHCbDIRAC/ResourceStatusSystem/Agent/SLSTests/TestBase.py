################################################################################
# $HeadURL:  $
################################################################################
__RCSID__  = "$Id:  $"

from DIRAC import gLogger

import threading, sys

class TestBase( threading.Thread ):
  '''
    Base class for all SLS tests.
  '''
  def __init__( self, testName, testConfig ):
    # Initialize the Threading
    threading.Thread.__init__( self )
    
    # If not timeout provided, the default is 600 secs.
    timeout   = testConfig.get( 'timeout', 600 ) 
    self.t    = threading.Timer( timeout, self.nuke )
    self.name = testName 
   
  def run( self ):
    
    gLogger.info( 'Starting %s thread' % self.name )
    # Start timer
    self.t.start()
    
    try:
      
      self.launchTest()
      
    except Exception, e:
      
      gLogger.exception( '%s test crashed. \n %s' % ( self.name, e ) )
        
    # kill via a schedule
    gLogger.info( str( self ) + ' Happily scheduling my own destruction.' )
    self.t.cancel()
    self.t = threading.Timer( 1, self.nuke )
    self.t.start() 
   
  def launchTest( self ):
    '''
      This function should have all logic needed to run the test.
      Overwrite me.
    '''
    pass 
   
  def nuke( self ):
    '''
      Self destruction by exiting thread.
    '''
    gLogger.info( str( self ) + ' au revoir.' )
    sys.exit()    
    
  def writeXml( self ):
    
    pass
  
  

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF