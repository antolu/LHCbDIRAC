########################################################################
# File: RAWIntegrityAgent.py
########################################################################

""" :mod: RAWIntegrityAgent
    =======================
 
    .. module: RAWIntegrityAgent
    :synopsis: RAWIntegrityAgent determines whether RAW files in CASTOR were migrated correctly.
"""

__RCSID__ = "$Id $"

## imports
import os
## from DIRAC
from DIRAC import gMonitor, S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.DISET.RPCClient import RPCClient 
from DIRAC.RequestManagementSystem.Client.RequestClient import RequestClient
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.DataManagementSystem.Client.DataLoggingClient import DataLoggingClient
## from LHCbDIRAC
from LHCbDIRAC.DataManagementSystem.DB.RAWIntegrityDB import RAWIntegrityDB

AGENT_NAME = 'DataManagement/RAWIntegrityAgent'

class RAWIntegrityAgent( AgentModule ):
  """
  .. class:: RAWIntegirtyAgent

  :param RequestClient requestClient: RequestDB client
  :param ReplicaManager replicaManager: ReplicaManager instance
  :param RAWIntegrityDB rawIntegrityDB: RAWIntegrityDB instance
  :param DataLoggingClient dataLoggingClient: DataLoggingClient instance
  :param str gatewayUrl: URL to online RequestClient
  """
  requestClient = None
  onlineRequestMgr = None
  replicaManager = None
  rawIntegrityDB = None
  dataLoggingClient = None
  gatewayUrl = None

  def initialize( self ):
    """ agent initialisation """

    self.requestClient = RequestClient()
    self.replicaManager = ReplicaManager()
    self.rawIntegrityDB = RAWIntegrityDB()
    self.dataLoggingClient = DataLoggingClient()

    self.gatewayUrl = PathFinder.getServiceURL( 'RequestManagement/onlineGateway' )
    self.onlineRequestMgr = RPCClient( self.gatewayUrl )
    
    gMonitor.registerActivity( "Iteration", "Agent Loops/min", 
                               "RAWIntegriryAgent", "Loops", gMonitor.OP_SUM )
    gMonitor.registerActivity( "WaitingFiles", "Files waiting for migration", 
                               "RAWIntegriryAgent", "Files", gMonitor.OP_MEAN )
    gMonitor.registerActivity( "NewlyMigrated", "Newly migrated files", 
                               "RAWIntegriryAgent", "Files", gMonitor.OP_SUM )
    gMonitor.registerActivity( "TotMigrated", "Total migrated files", 
                               "RAWIntegriryAgent", "Files", gMonitor.OP_ACUM )
    gMonitor.registerActivity( "SuccessfullyMigrated", "Successfully migrated files", 
                               "RAWIntegriryAgent", "Files", gMonitor.OP_SUM )
    gMonitor.registerActivity( "TotSucMigrated", "Total successfully migrated files", 
                               "RAWIntegriryAgent", "Files", gMonitor.OP_ACUM )
    gMonitor.registerActivity( "FailedMigrated", "Erroneously migrated files", 
                               "RAWIntegriryAgent", "Files", gMonitor.OP_SUM )
    gMonitor.registerActivity( "TotFailMigrated", "Total erroneously migrated files", 
                               "RAWIntegriryAgent", "Files", gMonitor.OP_ACUM )
    gMonitor.registerActivity( "MigrationTime", "Average migration time", 
                               "RAWIntegriryAgent", "Seconds", gMonitor.OP_MEAN )
    gMonitor.registerActivity( "TotMigratedSize", "Total migrated file size", 
                               "RAWIntegriryAgent", "GB", gMonitor.OP_ACUM )
    gMonitor.registerActivity( "TimeInQueue", "Average current wait for migration", 
                               "RAWIntegriryAgent", "Minutes", gMonitor.OP_MEAN )
    gMonitor.registerActivity( "WaitSize", "Size of migration buffer", 
                               "RAWIntegrityAgent", "GB", gMonitor.OP_MEAN )
    gMonitor.registerActivity( "MigrationRate", "Observed migration rate", 
                               "RAWIntegriryAgent", "MB/s", gMonitor.OP_MEAN )

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )

    return S_OK()

  def execute( self ):
    """ execution in one cycle 
    
    TODO: needs some refactoring and splitting, it's just way too long
    """
    gMonitor.addMark( "Iteration", 1 )

    ############################################################
    #
    # Obtain the files which have not yet been migrated
    #
    self.log.info( "Obtaining un-migrated files." )
    res = self.rawIntegrityDB.getActiveFiles()
    if not res['OK']:
      errStr = "Failed to obtain un-migrated files."
      self.log.error( errStr, res['Message'] )
      return S_OK()
    activeFiles = res['Value']

    gMonitor.addMark( "WaitingFiles", len( activeFiles ) )
    self.log.info( "Obtained %s un-migrated files." % len( activeFiles ) )
    if not activeFiles:
      return S_OK()
    totalSize = 0
    for lfn, fileDict in activeFiles.items():
      totalSize += int( fileDict['Size'] )
      gMonitor.addMark( "TimeInQueue", ( fileDict['WaitTime'] / 60 ) )
    gMonitor.addMark( "WaitSize", ( totalSize / ( 1024 * 1024 * 1024.0 ) ) )

    ############################################################
    #
    # Obtain the physical file metadata for the files awating migration
    #
    self.log.info( "Obtaining physical file metadata." )
    sePfns = {}
    pfnDict = {}
    for lfn, metadataDict in activeFiles.items():
      pfn = metadataDict['PFN']
      pfnDict[pfn] = lfn
      se = metadataDict['SE']
      if se not in sePfns:
        sePfns[se] = []
      sePfns[se].append( pfn )
    pfnMetadata = { 'Successful':{}, 'Failed':{} }
    for se, pfnList in sePfns.items():
      res = self.replicaManager.getStorageFileMetadata( pfnList, se )
      if not res['OK']:
        errStr = "Failed to obtain physical file metadata."
        self.log.error( errStr, res['Message'] )
        for pfn in pfnList:
          pfnMetadata['Failed'][pfn] = errStr
      else:
        pfnMetadata['Successful'].update( res['Value']['Successful'] )
        pfnMetadata['Failed'].update( res['Value']['Failed'] )
    if len( pfnMetadata['Failed'] ) > 0:
      self.log.info( "Failed to obtain physical file metadata for %s files." % len( pfnMetadata['Failed'] ) )
    self.log.info( "Obtained physical file metadata for %s files." % len( pfnMetadata['Successful'] ) )
    if not len( pfnMetadata['Successful'] ) > 0:
      return S_OK()

    ############################################################
    #
    # Determine the files that have been newly migrated and their success
    #
    filesToRemove = []
    filesToTransfer = []
    filesMigrated = []
    for pfn, pfnMetadataDict in pfnMetadata['Successful'].items():
      if pfnMetadataDict.get( 'Migrated', False ):
        lfn = pfnDict[pfn]
        filesMigrated.append( lfn )
        self.log.info( "%s is newly migrated." % lfn )
        if "Checksum" not in pfnMetadataDict:
          self.log.error( "No checksum information available.", lfn )
          comm = "nsls -lT --checksum /castor/%s" % pfn.split( '/castor/' )[-1]
          res = shellCall( 180, comm )
          returnCode, stdOut, stdErr = res['Value']
          if not returnCode:
            pfnMetadataDict['Checksum'] = stdOut.split()[9]
          else:
            pfnMetadataDict['Checksum'] = 'Not available'
        castorChecksum = pfnMetadataDict['Checksum']
        onlineChecksum = activeFiles[lfn]['Checksum']
        if castorChecksum.lower().lstrip( '0' ) == onlineChecksum.lower().lstrip( '0' ).lstrip( 'x' ):
          self.log.info( "%s migrated checksum match." % lfn )
          self.dataLoggingClient.addFileRecord( lfn, 'Checksum match', castorChecksum, '', 'RAWIntegrityAgent' )
          filesToRemove.append( lfn )
          activeFiles[lfn]['Checksum'] = castorChecksum
        elif pfnMetadataDict['Checksum'] == 'Not available':
          self.log.info( "Unable to determine checksum.", lfn )
        else:
          self.log.error( "Migrated checksum mis-match.", "%s %s %s" % ( lfn, castorChecksum.lstrip( '0' ), 
                                                                         onlineChecksum.lstrip( '0' ).lstrip( 'x' ) ) )
          self.dataLoggingClient.addFileRecord( lfn, 'Checksum mismatch', 
                                                '%s %s' % ( castorChecksum.lower().lstrip( '0' ), 
                                                            onlineChecksum.lower().lstrip( '0' ) ), 
                                                '', 'RAWIntegrityAgent' )
          filesToTransfer.append( lfn )

    migratedSize = 0
    for lfn in filesMigrated:
      migratedSize += int( activeFiles[lfn]['Size'] )
    res = self.rawIntegrityDB.getLastMonitorTimeDiff()
    if res['OK']:
      timeSinceLastMonitor = res['Value']
      migratedSizeMB = migratedSize / ( 1024 * 1024.0 )
      gMonitor.addMark( "MigrationRate", migratedSizeMB / timeSinceLastMonitor )
    res = self.rawIntegrityDB.setLastMonitorTime()
    migratedSizeGB = migratedSize / ( 1024 * 1024 * 1024.0 )
    gMonitor.addMark( "TotMigratedSize", migratedSizeGB )
    gMonitor.addMark( "NewlyMigrated", len( filesMigrated ) )
    gMonitor.addMark( "TotMigrated", len( filesMigrated ) )
    gMonitor.addMark( "SuccessfullyMigrated", len( filesToRemove ) )
    gMonitor.addMark( "TotSucMigrated", len( filesToRemove ) )
    gMonitor.addMark( "FailedMigrated", len( filesToTransfer ) )
    gMonitor.addMark( "TotFailMigrated", len( filesToTransfer ) )
    self.log.info( "%s files newly migrated." % len( filesMigrated ) )
    self.log.info( "Found %s checksum matches." % len( filesToRemove ) )
    self.log.info( "Found %s checksum mis-matches." % len( filesToTransfer ) )


    if len( filesToRemove ) > 0:
      ############################################################
      #
      # Register the correctly migrated files to the file catalogue
      #
      self.log.info( "Registering correctly migrated files to the File Catalog." )
      for lfn in filesToRemove:
        pfn = activeFiles[lfn]['PFN']
        size = activeFiles[lfn]['Size']
        se = activeFiles[lfn]['SE']
        guid = activeFiles[lfn]['GUID']
        checksum = activeFiles[lfn]['Checksum']
        fileTuple = ( lfn, pfn, size, se, guid, checksum )
        res = self.replicaManager.registerFile( fileTuple )
        if not res['OK']:
          self.dataLoggingClient.addFileRecord( lfn, 'RegisterFailed', se, '', 'RAWIntegrityAgent' )
          self.log.error( "Completely failed to register successfully migrated file.", res['Message'] )
        elif lfn in res['Value']['Failed']:
          self.dataLoggingClient.addFileRecord( lfn, 'RegisterFailed', se, '', 'RAWIntegrityAgent' )
          self.log.error( "Failed to register lfn in the File Catalog.", res['Value']['Failed'][lfn] )
        else:
          self.dataLoggingClient.addFileRecord( lfn, 'Register', se, '', 'RAWIntegrityAgent' )
          self.log.info( "Successfully registered %s in the File Catalog." % lfn )
          ############################################################
          #
          # Create a removal request and set it to the gateway request DB
          #
          self.log.info( "Creating removal request for correctly migrated files." )
          oRequest = RequestContainer()
          subRequestIndex = oRequest.initiateSubRequest( 'removal' )['Value']
          attributeDict = { 'Operation':'physicalRemoval', 'TargetSE':'OnlineRunDB' }
          oRequest.setSubRequestAttributes( subRequestIndex, 'removal', attributeDict )
          filesDict = [ {'LFN' : lfn, 'PFN' : pfn } ]
          oRequest.setSubRequestFiles( subRequestIndex, 'removal', filesDict )
          fileName = os.path.basename( lfn )
          requestName = 'remove_%s.xml' % fileName
          requestString = oRequest.toXML()['Value']
          self.log.info( "Attempting to put %s to gateway requestDB." % requestName )
          res = self.onlineRequestMgr.setRequest( requestName, requestString )
          if not res['OK']:
            self.log.error( "Failed to set removal request to gateway requestDB.", res['Message'] )
          else:
            self.log.info( "Successfully put %s to gateway requestDB." % requestName )
            ############################################################
            #
            # Remove the file from the list of files awaiting migration in database
            #
            self.log.info( "Updating status of %s in raw integrity database." % lfn )
            res = self.rawIntegrityDB.setFileStatus( lfn, 'Done' )
            if not res['OK']:
              self.log.error( "Failed to update status in raw integrity database.", res['Message'] )
            else:
              self.log.info( "Successfully updated status in raw integrity database." )
              gMonitor.addMark( "MigrationTime", activeFiles[lfn]['WaitTime'] )

    if len( filesToTransfer ) > 0:
      ############################################################
      #
      # Remove the incorrectly migrated files from the storage element (will be over written anyways)
      #
      self.log.info( "Removing incorrectly migrated files from Storage Element." )
      for lfn in filesToTransfer:
        pfn = activeFiles[lfn]['PFN']
        size = activeFiles[lfn]['Size']
        se = activeFiles[lfn]['SE']
        guid = activeFiles[lfn]['GUID']
        res = self.replicaManager.removeStorageFile( pfn, se )
        if not res['OK']:
          self.dataLoggingClient.addFileRecord( lfn, 'RemoveReplicaFailed', se, '', 'RAWIntegrityAgent' )
          self.log.error( "Completely failed to remove pfn from the storage element.", res['Message'] )
        elif pfn not in res['Value']['Successful']:
          self.dataLoggingClient.addFileRecord( lfn, 'RemoveReplicaFailed', se, '', 'RAWIntegrityAgent' )
          self.log.error( "Failed to remove pfn from the storage element.", res['Value']['Failed'][pfn] )
        else:
          self.dataLoggingClient.addFileRecord( lfn, 'RemoveReplica', se, '', 'RAWIntegrityAgent' )
          self.log.info( "Successfully removed pfn from the storage element." )
          ############################################################
          #
          # Create a transfer request for the files incorrectly migrated
          #
          self.log.info( "Creating (re)transfer request for incorrectly migrated files." )
          oRequest = RequestContainer()
          subRequestIndex = oRequest.initiateSubRequest( 'removal' )['Value']
          attributeDict = { 'Operation' : 'reTransfer', 'TargetSE' : 'OnlineRunDB' }
          oRequest.setSubRequestAttributes( subRequestIndex, 'removal', attributeDict )
          fileName = os.path.basename( lfn )
          filesDict = [ { 'LFN' : lfn, 'PFN' : fileName } ]
          oRequest.setSubRequestFiles( subRequestIndex, 'removal', filesDict )
          requestName = 'retransfer_%s.xml' % fileName
          requestString = oRequest.toXML()['Value']
          self.log.info( "Attempting to put %s to gateway requestDB." % requestName )
          res = self.onlineRequestMgr.setRequest( requestName, requestString )
          if not res['OK']:
            self.log.error( "Failed to set removal request to gateway requestDB.", res['Message'] )
          else:
            self.log.info( "Successfully put %s to gateway requestDB." % requestName )
            ############################################################
            #
            # Remove the file from the list of files awaiting migration in database
            #
            self.log.info( "Updating status of %s in raw integrity database." % lfn )
            res = self.rawIntegrityDB.setFileStatus( lfn, 'Failed' )
            if not res['OK']:
              self.log.error( "Failed to update status in raw integrity database.", res['Message'] )
            else:
              self.log.info( "Successfully updated status in raw integrity database." )

    return S_OK()
