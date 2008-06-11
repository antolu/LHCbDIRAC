########################################################################
# $Id: LHCB_BKKDBClient.py,v 1.6 2008/06/11 15:27:38 zmathe Exp $
########################################################################

"""
 LHCb Bookkeeping database client
"""


from DIRAC.BookkeepingSystem.Client.BaseESClient                        import BaseESClient
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBManager                   import LHCB_BKKDBManager        

__RCSID__ = "$Id: LHCB_BKKDBClient.py,v 1.6 2008/06/11 15:27:38 zmathe Exp $"

#############################################################################
class LHCB_BKKDBClient(BaseESClient):
  
  #############################################################################
  def __init__(self, manager = LHCB_BKKDBManager()):
    super(LHCB_BKKDBClient, self).__init__(manager, '/')
        
  #############################################################################  
  def get(self, path = ""):
    return self.getManager().get(path)
  
  #############################################################################
  def help(self):
    return self.getManager().help()
  
  #############################################################################
  def getPossibleParameters(self):
    return self.getManager().getPossibleParameters()
  
  #############################################################################
  def setParameter(self, name):
    return self.getManager().setParameter(name)

  #############################################################################
  def getLogicalFiles(self):
    return self.getManager().getLogicalFiles()
    
  #############################################################################
  def getFilesPFN(self):
    return self.getManager().getFilesPFN()
      
