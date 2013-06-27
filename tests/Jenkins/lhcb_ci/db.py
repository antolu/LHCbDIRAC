""" lhcb_ci.utils.db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

from DIRAC.Core.Utilities import InstallTools


def install( dbName ):
  """ install
  
  """

  return InstallTools.installDatabase( dbName )
  
  
def scratch( dbName ):
  pass  
  

#...............................................................................
#EOF