""" lhcb_ci.utils.db

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
"""

from DIRAC.Core.Utilities import InstallTools


def install( dbName ):
  """ install
  
  """

  InstallTools.getMySQLPasswords()
  result = InstallTools.installDatabase( dbName )
  if not result[ 'OK' ]:
    return result
  
  _extension, system = result[ 'Value' ]
  
  return InstallTools.addDatabaseOptionsToCS( None, system, dbName, overwrite = True )
  
  
def scratch( dbName ):
  pass  
  

#...............................................................................
#EOF