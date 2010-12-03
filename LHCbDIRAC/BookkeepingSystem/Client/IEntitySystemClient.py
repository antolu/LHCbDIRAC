########################################################################
# $Id: IEntitySystemClient.py 18173 2009-11-11 14:01:58Z zmathe $
########################################################################


"""
 Interface for entity system clients
"""

from DIRAC                                                               import gLogger, S_OK, S_ERROR
from LHCbDIRAC.BookkeepingSystem.Client.IEntitySystemStateless           import IEntitySystemStateless  

__RCSID__ = "$Id: IEntitySystemClient.py 18173 2009-11-11 14:01:58Z zmathe $"

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

