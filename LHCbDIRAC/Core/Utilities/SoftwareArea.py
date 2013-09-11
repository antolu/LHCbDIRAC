""" SoftwareArea

  Module containing functions to discover local and shared areas, as well as
  building the MYSITEROOT path.

"""


import os

# DIRAC
from DIRAC import gConfig, gLogger, rootPath


__RCSID__ = "$Id: $"


def getLocalArea():
  """ getLocalArea
   
  Discover Location of Local SW Area.
  This area is populated by DIRAC job Agent for jobs needing SW not present
  in the Shared Area.
  
  """
  
  if gConfig.getValue( '/LocalSite/LocalArea', '' ):
    localArea = gConfig.getValue( '/LocalSite/LocalArea' )
  else:
    localArea = os.path.join( rootPath, 'LocalArea' )

  # check if already existing directory
  if not os.path.isdir( localArea ):
    # check if we can create it
    if os.path.exists( localArea ):
      try:
        os.remove( localArea )
      except OSError, msg:
        gLogger.error( 'Cannot remove: %s %s' % ( localArea, msg ) )
        localArea = ''
    else:
      try:
        os.mkdir( localArea )
      except OSError, msg:
        gLogger.error( 'Cannot create: %s %s' % ( localArea, msg ) )
        localArea = ''
        
  return localArea


def getSharedArea():
  """ getSharedArea
  
  Discover location of Shared SW area. This area is populated by a tool independent 
  of the DIRAC jobs
  
  """
  
  sharedArea = ''
  if os.environ.has_key( 'VO_LHCB_SW_DIR' ):
    sharedArea = os.path.join( os.environ[ 'VO_LHCB_SW_DIR' ], 'lib' )
    gLogger.debug( 'Using VO_LHCB_SW_DIR at "%s"' % sharedArea )
    if os.environ[ 'VO_LHCB_SW_DIR' ] == '.':
      if not os.path.isdir( 'lib' ):
        os.mkdir( 'lib' )
  elif gConfig.getValue( '/LocalSite/SharedArea', '' ):
    sharedArea = gConfig.getValue( '/LocalSite/SharedArea' )
    gLogger.debug( 'Using CE SharedArea at "%s"' % sharedArea )

  if sharedArea:
    # if defined, check that it really exists
    if not os.path.isdir( sharedArea ):
      gLogger.error( 'Missing Shared Area Directory:', sharedArea )
      sharedArea = ''

  return sharedArea


def mySiteRoot():
  """ mySiteRoot
  
  Returns the mySiteRoot for the current local and / or shared areas.
  
  """
  
  localmySiteRoot = ''
  localArea = getLocalArea()
  if not localArea:
    gLogger.error( 'Failed to determine Local SW Area' )
    return localmySiteRoot
  sharedArea = getSharedArea()
  if not sharedArea:
    gLogger.error( 'Failed to determine Shared SW Area' )
    return localArea
  localmySiteRoot = '%s:%s' % ( localArea, sharedArea )
  return localmySiteRoot


#...............................................................................
#EOF
