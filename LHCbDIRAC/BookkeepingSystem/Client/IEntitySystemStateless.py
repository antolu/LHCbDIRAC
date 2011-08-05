########################################################################
# $Id$
########################################################################

"""
  Interface for stateless entity system operations
"""

from DIRAC                                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id$"

#############################################################################
class IEntitySystemStateless(object):

  #############################################################################
  def list(self, path="/", SelectionDict = {}, SortDict={}, StartItem=0, Maxitems=0):
    gLogger.warn('not implemented')
    

