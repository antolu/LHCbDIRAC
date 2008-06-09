########################################################################
# $Id: IEntitySystemClient.py,v 1.1 2008/06/09 10:10:57 zmathe Exp $
########################################################################


  """
   Interface for entity system clients
  """

from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Client.IEntitySystemStateless           import IEntitySystemStateless  

__RCSID__ = "$Id: IEntitySystemClient.py,v 1.1 2008/06/09 10:10:57 zmathe Exp $"

#############################################################################
class IEntitySystemClient(IEntitySystemStateless):
  
  #############################################################################
  def __init__(self):
    pass
  
  #############################################################################
  def getManager(self):
    gLogger.warn('not implemented')
  
  #############################################################################
  def get(self, path):
    gLogger.warn('not implemented')

