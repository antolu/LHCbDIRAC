########################################################################
# $Id: IEntitySystemStateless.py,v 1.1 2008/06/09 10:10:56 zmathe Exp $
########################################################################

  """
   Interface for stateless entity system operations
  """

from DIRAC                                                           import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: IEntitySystemStateless.py,v 1.1 2008/06/09 10:10:56 zmathe Exp $"

#############################################################################
class IEntitySystemStateless(object):

  #############################################################################
  def __init__(self):
    pass
  
  #############################################################################
  def list(self, path=""):
    gLogger.warn('not implemented')
    

