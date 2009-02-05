########################################################################
# $Id: LHCB_BKKDBClient.py,v 1.13 2009/02/05 11:03:16 zmathe Exp $
########################################################################

"""
 LHCb Bookkeeping database client
"""


from DIRAC.BookkeepingSystem.Client.BaseESClient                        import BaseESClient
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBManager                   import LHCB_BKKDBManager        

__RCSID__ = "$Id: LHCB_BKKDBClient.py,v 1.13 2009/02/05 11:03:16 zmathe Exp $"

#############################################################################
class LHCB_BKKDBClient(BaseESClient):
  
  #############################################################################
  def __init__(self, rpcClinet = None, manager = None ):
    super(LHCB_BKKDBClient, self).__init__(LHCB_BKKDBManager(rpcClinet), '/')
        
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
  
  #############################################################################
  def getNumberOfEvents(self, files):
    return self.getManager().getNumberOfEvents(files)
  
  #############################################################################
  def writeJobOptions(self, files, optionsFile = "jobOptions.opts"):
    return self.getManager().writeJobOptions(files, optionsFile)
  
  #############################################################################
  def getJobInfo(self, lfn):
    return self.getManager().getJobInfo(lfn)
  
  #############################################################################
  def setVerbose(self, Value):
    return self.getManager().setVerbose(Value)
  
  #############################################################################
  def setAdvancedQueries(self, Value):
    return self.getManager().setAdvancedQueries(Value)
  
  #############################################################################
  def getLimitedFiles(self,SelectionDict, SortDict, StartItem, Maxitems):
    return self.getManager().getLimitedFiles(SelectionDict, SortDict, StartItem, Maxitems)
  
  #############################################################################
  def getAncestors(self, files, depth):
    return self.getManager().getAncestors(files, depth)
  
  #############################################################################
  def getLogfile(self, filename):
    return self.getManager().getLogfile(filename)