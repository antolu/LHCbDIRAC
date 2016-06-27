"""
Bookkeeping utilities
"""

__RCSID__ = "$Id$"

from DIRAC import S_ERROR
import errno

      
def checkArguments( func ):
  
  def checkMethodArguments(self, *args, **kwargs ):
    """
    This is used to check the conditions of a given query. We assume a dictionary can not be empty and 
    it has more than one element, if we do not take into account the replica flag and the visibility flag
    """
    print args
    print 'DSDSDS', self.getRemoteCredentials()
    if len( args ) > 0:
      arguments = args[1]
      print '!!!!!', arguments
      return S_ERROR( errno.EINVAL, "WRONG arguments" )
    result = func( *args, **kwargs )
    return result
  return checkMethodArguments
