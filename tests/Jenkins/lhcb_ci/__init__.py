""" lhcb_ci 

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""


import logging
import os
import warnings


# Get rid of annoying MySQLdb warnings 
with warnings.catch_warnings():
  warnings.simplefilter( 'ignore', DeprecationWarning )
  warnings.filterwarnings( 'ignore' , 'Unknown table.*' )
  import MySQLdb


logger = logging.getLogger( 'lhcb_ci' )
logger.setLevel( level = logging.DEBUG )

workspace = os.getenv( 'WORKSPACE' )


def _init_once():
  """
  Utility function that is ran once on Library import.

  This checks for the LHCB_CI_DEBUG environment variable, which if it exists
  is where we will log debug information.
  """

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