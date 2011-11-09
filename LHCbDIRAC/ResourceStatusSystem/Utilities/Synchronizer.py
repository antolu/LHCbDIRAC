from DIRAC.ResourceStatusSystem.Utilities import Utils
from DIRAC.ResourceStatusSystem.Utilities import CS
from DIRAC.ResourceStatusSystem.Utilities import Synchronizer as BaseSync

class Synchronizer(BaseSync.Synchronizer):
  """
  LHCb specific Synchronizer class. This class extends the
  correspondant DIRAC class. It adds two specific things to
  synchronize: VOBOX and CondDBs, which are LHCb specific.
  """
  def __init__(self, rsClient = None, rmClient = None):
    super(Synchronizer, self).__init__(rsClient, rmClient)
    self.synclist = self.synclist + ["VOBOX", "CondDBs"]

  def _syncVOBOX( self ):
    """
    Sync DB content with VOBoxes
    """

    # services in the DB now
    VOBOXesInCS = set(Utils.unpack(CS.getT1s()))
    VOBOXesInDB = set(Utils.list_flatten(Utils.unpack(self.rsClient.getServicePresent(
          serviceType = "VO-BOX", meta = { 'columns' : "SiteName" } ))))

    print "Updating %d VOBOXes on DB" % len(VOBOXesInCS - VOBOXesInDB)
    for site in VOBOXesInCS - VOBOXesInDB:
      service = 'VO-BOX@' + site
      Utils.protect2(self.rsClient.addOrModifyService, service, 'VO-BOX', site )

  def _syncCondDBs(self):
    CondDBinCS = set(Utils.unpack(CS.getCondDBs()))
    CondDBinDB = set(Utils.list_flatten(Utils.unpack(self.rsClient.getServicePresent(
            serviceType = "CondDB", meta = { 'columns' : "SiteName" } ))))

    print "Updating %d CondDBs on DB" % len (CondDBinCS - CondDBinDB)
    for site in CondDBinCS - CondDBinDB:
      service = "CondDB@" + site
      Utils.protect2(self.rsClient.addOrModifyService, service, 'CondDB', site )
