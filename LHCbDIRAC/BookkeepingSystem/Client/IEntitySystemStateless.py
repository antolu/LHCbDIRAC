########################################################################
# $Id: IEntitySystemStateless.py,v 1.3 2008/06/09 10:22:13 zmathe Exp $
########################################################################

"""
  Interface for stateless entity system operations
"""

from DIRAC                                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: IEntitySystemStateless.py,v 1.3 2008/06/09 10:22:13 zmathe Exp $"

#############################################################################
class IEntitySystemStateless(object):

  #############################################################################
  def list(self, path=""):
    gLogger.warn('not implemented')
    

