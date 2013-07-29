""" lhcb_ci.commons

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import threading

# lhcb_ci
import lhcb_ci



def trackThreads():
  """ trackThreads
  
  Method that returns the active threads at this moment. It is used together with
  killThreads, to ensure a cleaner running test environment.
  """

  # This also includes the _limbo threads..
  currentThreads = threading.enumerate() 
  activeThreads  = threading.active_count()
  
  return currentThreads, activeThreads    


def killThreads( threadsToBeAvoided = [] ):
  """ killThreads
  
  Kills leftover threads to prevent them from interacting with the next execution.
  """
  
  for th in threading.enumerate():
    
    if th in threadsToBeAvoided:
      continue
    
    if th.isAlive():
      try:
        th._Thread__stop()               
        del threading._active[ th.ident ]
      except:
        lhcb_ci.logger.debug( 'Cannot kill thread %s : %s' % ( th.ident, th.name ) )
        
  return threading.active_count()         

#...............................................................................
#EOF