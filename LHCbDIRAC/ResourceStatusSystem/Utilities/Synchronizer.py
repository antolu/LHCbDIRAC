# $HeadURL:  $
''' Synchronizer

  Extension for LHCb of the RSS Synchronizer

'''

from DIRAC                                import gLogger
from DIRAC.ResourceStatusSystem.Utilities import Utils
from DIRAC.ResourceStatusSystem.Utilities import CS
from DIRAC.ResourceStatusSystem.Utilities import Synchronizer as BaseSync

__RCSID__  = '$Id: $'

class Synchronizer( BaseSync.Synchronizer ):
  '''
  LHCb specific Synchronizer class. This class extends the
  correspondant DIRAC class. It adds two specific things to
  synchronize: VOBOX and CondDBs, which are LHCb specific.
  '''
  
  def __init__( self, rsClient = None, rmClient = None ):
    '''
    Init.
    '''
    super( Synchronizer, self ).__init__( rsClient, rmClient )
    self.synclist = self.synclist + ["VOBOX", "CondDBs"]

  def _syncVOBOX( self ):
    '''
    Sync DB content with VOBoxes
    '''

    # services in the DB now
    voBoxInCS = set(CS.getT1s())
    voBOXInDB = set(Utils.list_flatten(Utils.unpack(self.rsClient.getServicePresent(
          serviceType = "VO-BOX", meta = { 'columns' : "SiteName" } ))))

    gLogger.info( "Updating %d VOBOXes on DB" % len(voBoxInCS - voBOXInDB) )
    for site in voBoxInCS - voBOXInDB:
      service = 'VO-BOX@' + site
      Utils.protect2(self.rsClient.addOrModifyService, service, 'VO-BOX', site )

  def _syncCondDBs(self):
    '''
    Sync DB content with CondDBs
    '''
    
    condDBInCS = set(CS.getCondDBs())
    condDBInDB = set(Utils.list_flatten(Utils.unpack(self.rsClient.getServicePresent(
            serviceType = "CondDB", meta = { 'columns' : "SiteName" } ))))

    gLogger.info("Updating %d CondDBs on DB" % len (condDBInCS - condDBInDB))
    for site in condDBInCS - condDBInDB:
      service = "CondDB@" + site
      Utils.protect2(self.rsClient.addOrModifyService, service, 'CondDB', site )
      
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF      