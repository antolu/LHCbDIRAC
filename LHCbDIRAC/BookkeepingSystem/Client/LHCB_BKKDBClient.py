########################################################################
# $Id: LHCB_BKKDBClient.py,v 1.4 2008/06/11 14:19:20 zmathe Exp $
########################################################################

"""
 LHCb Bookkeeping database client
"""


from DIRAC.BookkeepingSystem.Client.BaseESClient                        import BaseESClient
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBManager                   import LHCB_BKKDBManager        

__RCSID__ = "$Id: LHCB_BKKDBClient.py,v 1.4 2008/06/11 14:19:20 zmathe Exp $"

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
  def setParameter(self, name):
    return self.getManager().setParameter(name)

  #############################################################################
  def getFiles(self):
    return self.getManager().getFiles()



        
