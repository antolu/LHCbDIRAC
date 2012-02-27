########################################################################
# $Id$
########################################################################

"""
 LHCb Bookkeeping database client
"""


from LHCbDIRAC.NewBookkeepingSystem.Client.BaseESClient                        import BaseESClient
from LHCbDIRAC.NewBookkeepingSystem.Client.LHCB_BKKDBManager                   import LHCB_BKKDBManager

__RCSID__ = "$Id$"

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
  def writeJobOptions(self, files, optionsFile = "jobOptions.opts", savedType = None, catalog = None, savePfn = None, dataset = None):
    return self.getManager().writeJobOptions(files, optionsFile, savedType, catalog, savePfn, dataset)

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

  #############################################################################
  def writePythonOrJobOptions(self, StartItem, Maxitems, path, type ):
    return self.getManager().writePythonOrJobOptions(StartItem, Maxitems, path, type )

  #############################################################################
  def getLimitedInformations(self, StartItem, Maxitems, path):
    return self.getManager().getLimitedInformations(StartItem, Maxitems, path)

  #############################################################################
  def getProcessingPassSteps(self, dict):
    return self.getManager().getProcessingPassSteps(dict)

  #############################################################################
  def getMoreProductionInformations(self, prodid):
    return self.getManager().getMoreProductionInformations(prodid)

  #############################################################################
  def getAvailableProductions(self):
    return self.getManager().getAvailableProductions()

  #############################################################################
  def getFileHistory(self, lfn):
    return self.getManager().getFileHistory(lfn)

  #############################################################################
  def getCurrentParameter(self):
    return self.getManager().getCurrentParameter()

  #############################################################################
  def getQueriesTypes(self):
    return self.getManager().getQueriesTypes()

  #############################################################################
  def getProductionProcessingPassSteps(self, dict):
    return self.getManager().getProductionProcessingPassSteps(dict)

  #############################################################################
  def getAvailableDataQuality(self):
    return self.getManager().getAvailableDataQuality()

  #############################################################################
  def setDataQualities(self, values):
    self.getManager().setDataQualities(values)
