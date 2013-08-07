''' LHCbDIRAC.ResourceStatusSystem.Utilities.Synchronizer

   Synchronizer.__bases__:
     DIRAC.ResourceStatusSystem.Utilities.Synchronizer.Synchronizer

'''

from DIRAC.ResourceStatusSystem.Utilities.Synchronizer   import Synchronizer as DIRACSyncrhonizer

__RCSID__  = '$Id$'

class Synchronizer( DIRACSyncrhonizer ):
  '''
    Extension for LHCb of the RSS Synchronizer

    LHCb specific Synchronizer class. This class extends the
    correspondant DIRAC class. It adds two specific things to
    synchronize: VOBOX and CondDBs, which are LHCb specific.
  '''

  def __init__( self, rStatus = None, rManagement = None ):
    super( Synchronizer, self ).__init__( rStatus, rManagement )
  
  #FIXME: VOBOX ?
  #FIXME: CONDDB ?
  #FIXME: DISET ?

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
    
#  def _syncVOBOX( self ):
#    '''
#    Sync DB content with VOBoxes
#    '''
#
#    # Fix to avoid loading CS if not needed
#    from DIRAC.ResourceStatusSystem.Utilities import CS
#
#    # services in the DB now
#    voBoxInCS = set(CS.getT1s())
#    
#    #voBOXInDB = set(Utils.list_flatten(Utils.unpack(self.rsClient.getServicePresent(
#    #      serviceType = "VO-BOX", meta = { 'columns' : "SiteName" } ))))
#    
#    voBOXInDB = self.rsClient.getServicePresent( serviceType = "VO-BOX", 
#                                                 meta = { 'columns' : "SiteName" } )
#    if not voBOXInDB[ 'OK' ]:
#      gLogger.error( voBOXInDB[ 'Message' ] )
#      return voBOXInDB
#    
#    voBOXInDB = set( Utils.list_flatten( voBOXInDB[ 'Value' ] ) )
#
#    gLogger.info( "Updating %d VOBOXes on DB" % len(voBoxInCS - voBOXInDB) )
#    for site in voBoxInCS - voBOXInDB:
#      service = 'VO-BOX@' + site
#      #Utils.protect2(self.rsClient.addOrModifyService, service, 'VO-BOX', site )
#      res = self.rsClient.addOrModifyService( service, 'VO-BOX', site )
#      if not res[ 'OK' ]:
#        gLogger.error( res[ 'Message' ] )
#        return res
#
#  def _syncCondDBs(self):
#    '''
#    Sync DB content with CondDBs
#    '''
#
#    # Fix to avoid loading CS if not needed
#    from DIRAC.ResourceStatusSystem.Utilities import CS
#    
#    condDBInCS = set(CS.getCondDBs())
#    #condDBInDB = set(Utils.list_flatten(Utils.unpack(self.rsClient.getServicePresent(
#    #        serviceType = "CondDB", meta = { 'columns' : "SiteName" } ))))
#    condDBInDB = self.rsClient.getServicePresent( serviceType = "CondDB", 
#                                                  meta = { 'columns' : "SiteName" } )
#    
#    if not condDBInDB[ 'OK' ]:
#      gLogger.error( condDBInDB[ 'Message' ] )
#      return condDBInDB
#    
#    condDBInDB = set(Utils.list_flatten( condDBInDB[ 'Value' ] ) )
#            
#    gLogger.info("Updating %d CondDBs on DB" % len (condDBInCS - condDBInDB))
#    for site in condDBInCS - condDBInDB:
#      service = "CondDB@" + site
#      #Utils.protect2(self.rsClient.addOrModifyService, service, 'CondDB', site )
#      res = self.rsClient.addOrModifyService( service, 'CondDB', site )
#      if not res[ 'OK' ]:
#        gLogger.error( res[ 'Message' ] )
#        return res
#
#  def _syncDISET( self ):
#    '''
#    Sync StorageElements with DISET BackendType
#    '''
#
#    # Fix to avoid loading CS if not needed
#    from DIRAC.ResourceStatusSystem.Utilities import CS
#
#    ses = CS.getSEs()
#
#    opHelper = Operations()
#
#    for se in ses:
#      backEnd = opHelper.getValue( 'StorageElements/%s/BackendType' % se )
#      #backEnd = gConfig.getValue( '/Resources/StorageElements/%s/BackendType' % se )
#      if backEnd == 'DISET':
#        knownSE = self.rsClient.getStorageElement( se )
#        if knownSE[ 'OK' ] and not knownSE[ 'Value' ]:
#          res = self.rsClient.addOrModifyStorageElement( se, 'VOBOX', 'DISET' )
#          if not res[ 'OK' ]:
#            gLogger.error( res[ 'Message' ] )            