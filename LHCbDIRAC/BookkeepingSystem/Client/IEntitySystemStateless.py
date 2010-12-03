########################################################################
# $Id: IEntitySystemStateless.py 18161 2009-11-11 12:07:09Z acasajus $
########################################################################

"""
  Interface for stateless entity system operations
"""

from DIRAC                                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: IEntitySystemStateless.py 18161 2009-11-11 12:07:09Z acasajus $"

#############################################################################
class IEntitySystemStateless(object):

  #############################################################################
  def list(self, path="/", SelectionDict = {}, SortDict={}, StartItem=0, Maxitems=0):
    gLogger.warn('not implemented')
    

