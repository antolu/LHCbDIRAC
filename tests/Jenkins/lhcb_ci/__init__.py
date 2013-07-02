""" lhcb_ci 

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import logging

logger = logging.getLogger( 'lhcb_ci' )
logger.setLevel( level = logging.DEBUG )

def _init_once():
  """
  Utility function that is ran once on Library import.

  This checks for the LHCB_CI_DEBUG enviroment variable, which if it exists
  is where we will log debug information about the provider transports.
  """

  import os

  logFormat = '[%(asctime)s]%(levelname)-8s: %(message)s'

  ch = logging.StreamHandler()
  ch.setFormatter( logging.Formatter( '\n%s' % logFormat ) )
  ch.setLevel( logging.ERROR )
  logger.addHandler( ch )
 
  filename = os.getenv( 'LHCB_CI_DEBUG' )
  if filename:
    
    fh = logging.FileHandler( filename )
    fh.setLevel( logging.DEBUG )
    fh.setFormatter( logging.Formatter( logFormat ) )
    
    logger.addHandler( fh )    

_init_once()

#...............................................................................
#EOF