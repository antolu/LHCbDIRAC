########################################################################
# $Id: IEntitySystemStateless.py,v 1.4 2009/05/08 15:23:25 zmathe Exp $
########################################################################

"""
  Interface for stateless entity system operations
"""

from DIRAC                                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: IEntitySystemStateless.py,v 1.4 2009/05/08 15:23:25 zmathe Exp $"

#############################################################################
class IEntitySystemStateless(object):

  #############################################################################
  def list(self, path="/", SelectionDict = {}, SortDict={}, StartItem=0, Maxitems=0):
    gLogger.warn('not implemented')
    

