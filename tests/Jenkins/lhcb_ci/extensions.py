""" lhcb_ci.utils.extensions

  LHCb Continuous Integration system libraries

  @author: mario.ubeda.garcia@cern.ch

"""

from DIRAC import gConfig

def import_( base_mod ):
  """ import_
    
  Imports taking into account the extensions.  
  """
  
  extensions = gConfig.getValue( '/DIRAC/Extensions', [] )
  for ext in extensions:  
    try:
      return  __import__( ext + base_mod, globals(), locals(), ['*'] )
    except ImportError:
      continue
  
  # If not found in extensions, import it in DIRAC base.
  return  __import__( base_mod, globals(), locals(), ['*'] )

#...............................................................................
#EOF