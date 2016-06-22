########################################################################
# File: RAWIntegrityAgent.py
########################################################################
""" :mod: RAWIntegrityAgent
    =======================

    .. module: RAWIntegrityAgent
    :synopsis: RAWIntegrityAgent determines whether RAW files in CASTOR were migrated correctly.
"""
# # imports
import os
# # from DIRAC
from DIRAC import S_OK
# # from Core
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.Subprocess import shellCall
# # from DMS
from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
from DIRAC.Resources.Storage.StorageElement import StorageElement
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient
from LHCbDIRAC.DataManagementSystem.DB.RAWIntegrityDB import RAWIntegrityDB
# # from RMS
from DIRAC.RequestManagementSystem.Client.Request import Request
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.File import File

from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.FrameworkSystem.Client.MonitoringClient import gMonitor

__RCSID__ = "$Id$"


AGENT_NAME = 'DataManagement/RAWIntegrityAgent'

class RAWIntegrityAgent( AgentModule ):
  """
  .. class:: RAWIntegirtyAgent

  :param RAWIntegrityDB rawIntegrityDB: RAWIntegrityDB instance
  :param str gatewayUrl: URL to online RequestClient
  """

  def __init__( self, *args, **kwargs ):
    """ c'tor
    """

    AgentModule.__init__( self, *args, **kwargs )

    self.rawIntegrityDB    = None
    self.fileCatalog = None
    self.onlineRequestMgr  = None


  def initialize( self ):
    """ agent initialisation """

    self.rawIntegrityDB = RAWIntegrityDB()

    # The file catalog is used to register file once it has been transfered
    # But we want to register it in all the catalogs except the RAWIntegrityDB
    # otherwise it is register twice
    # We also remove the BK catalog because some files are not registered there
    # (detector calibration files for example). The real data are registered in
    # the bookeeping by the DataMover
    self.fileCatalog = FileCatalog()
    self.fileCatalog.removeCatalog( 'RAWIntegrity' )
    self.fileCatalog.removeCatalog( 'BookkeepingDB' )


    self.onlineRequestMgr = ReqClient()
    self.onlineRequestMgr.setServer( 'RequestManagement/onlineGateway' )


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
    res = self.am_setOption( 'shifterProxy', 'DataProcessing' )

    if not res['OK']:
      return res

    return S_OK()

  def execute( self ):
    """ execution in one cycle

    TODO: needs some refactoring and splitting, it's just way too long
    """

    # Don't use the server certificate otherwise the DFC wont let us write
    gConfigurationData.setOptionInCFG( '/DIRAC/Security/UseServerCertificate', 'false' )

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
    for lfn, fileDict in activeFiles.iteritems():
      totalSize += int( fileDict['Size'] )
      gMonitor.addMark( "TimeInQueue", ( fileDict['WaitTime'] / 60 ) )
    gMonitor.addMark( "WaitSize", ( totalSize / ( 1024 * 1024 * 1024.0 ) ) )

    ############################################################
    #
    # Obtain the physical file metadata for the files awating migration
    #
    self.log.info( "Obtaining physical file metadata." )
    seLfns = {}
    for lfn, metadataDict in activeFiles.iteritems():
      se = metadataDict['SE']
      seLfns.setdefault( se, [] ).append( lfn )

    lfnMetadata = { 'Successful':{}, 'Failed':{} }
    for se, lfnList in seLfns.iteritems():
      res = StorageElement( se ).getFileMetadata( lfnList )
      if not res['OK']:
        errStr = "Failed to obtain physical file metadata."
        self.log.error( errStr, res['Message'] )
        for lfn in lfnList:
          lfnMetadata['Failed'][lfn] = errStr
      else:
        lfnMetadata['Successful'].update( res['Value']['Successful'] )
        lfnMetadata['Failed'].update( res['Value']['Failed'] )
    if len( lfnMetadata['Failed'] ) > 0:
      self.log.info( "Failed to obtain physical file metadata for %s files." % len( lfnMetadata['Failed'] ) )
    self.log.info( "Obtained physical file metadata for %s files." % len( lfnMetadata['Successful'] ) )
    if not len( lfnMetadata['Successful'] ) > 0:
      return S_OK()

    ############################################################
    #
    # Determine the files that have been newly migrated and their success
    #
    filesToRemove = []
    filesToTransfer = []
    filesMigrated = []
    for lfn, lfnMetadataDict in lfnMetadata['Successful'].iteritems():
      if lfnMetadataDict.get( 'Migrated', False ):
        filesMigrated.append( lfn )
        self.log.info( "%s is newly migrated." % lfn )
        if "Checksum" not in lfnMetadataDict:
          self.log.error( "No checksum information available.", lfn )
          comm = "nsls -lT --checksum /castor/%s" % lfn
          res = shellCall( 180, comm )
          returnCode, stdOut, _stdErr = res[ 'Value']
          if not returnCode:
            lfnMetadataDict['Checksum'] = stdOut.split()[9]
          else:
            lfnMetadataDict['Checksum'] = 'Not available'
        castorChecksum = lfnMetadataDict['Checksum']
        onlineChecksum = activeFiles[lfn]['Checksum']
        if castorChecksum.lower().lstrip( '0' ) == onlineChecksum.lower().lstrip( '0' ).lstrip( 'x' ):
          self.log.info( "%s migrated checksum match." % lfn )
          filesToRemove.append( lfn )
          activeFiles[lfn]['Checksum'] = castorChecksum
        elif lfnMetadataDict['Checksum'] == 'Not available':
          self.log.info( "Unable to determine checksum.", lfn )
        else:
          self.log.error( "Migrated checksum mis-match.", "%s %s %s" % ( lfn, castorChecksum.lstrip( '0' ),
                                                                         onlineChecksum.lstrip( '0' ).lstrip( 'x' ) ) )

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

        fileDict = {}
        fileDict[lfn] = {'PFN':pfn, 'Size':size, 'SE':se, 'GUID':guid, 'Checksum':checksum}
        res = self.fileCatalog.addFile( fileDict )

        if not res['OK']:
          self.log.error( "Completely failed to register successfully migrated file.", res['Message'] )
          continue
        else:
          if lfn in res['Value']['Failed']:

            if lfn in res['Value']['Successful']:
              self.log.error( "Only partially registered lfn in the File Catalog.", res['Value']['Failed'][lfn] )
            else:
              self.log.error( "Completely failed to register lfn in the File Catalog.", res['Value']['Failed'][lfn] )

            continue

          else:
            self.log.info( "Successfully registered %s in the File Catalog." % lfn )
          ############################################################
          #
          # Create a removal request and set it to the gateway request DB
          #
          self.log.info( "Creating removal request for correctly migrated files." )
          oRequest = Request()
          oRequest.RequestName = "remove_%s" % os.path.basename( lfn )
          physRemoval = Operation()
          physRemoval.Type = "PhysicalRemoval"
          physRemoval.TargetSE = "OnlineRunDB"
          fileToRemove = File()
          fileToRemove.LFN = lfn
          fileToRemove.PFN = pfn
          physRemoval.addFile( fileToRemove )
          oRequest.addOperation( physRemoval )

          self.log.info( "Attempting to put %s to gateway requestDB." % oRequest.RequestName )
          res = self.onlineRequestMgr.putRequest( oRequest )
          if not res['OK']:
            self.log.error( "Failed to set removal request to gateway requestDB.", res['Message'] )
          else:
            self.log.info( "Successfully put %s to gateway requestDB." % oRequest.RequestName )
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
        res = StorageElement( se ).removeFile( lfn )
        if not res['OK']:
          self.log.error( "Completely failed to remove lfn from the storage element.", res['Message'] )
        elif lfn not in res['Value']['Successful']:
          self.log.error( "Failed to remove lfn from the storage element.", res['Value']['Failed'][lfn] )
        else:
          self.log.info( "Successfully removed lfn from the storage element." )
          ############################################################
          #
          # Create a transfer request for the files incorrectly migrated
          #
          self.log.info( "Creating (re)transfer request for incorrectly migrated files." )

          oRequest = Request()
          oRequest.RequestName = "retransfer_%s" % os.path.basename( lfn )

          reTransfer = Operation()
          reTransfer.Type = "ReTransfer"
          reTransfer.targetSE = "OnlineRunDB"

          reTransferFile = File()
          reTransferFile.LFN = lfn
          reTransferFile.PFN = os.path.basename( lfn )

          reTransfer.addFile( reTransferFile )
          oRequest.addOperation( reTransfer )


          self.log.info( "Attempting to put %s to gateway requestDB." % oRequest.RequestName )
          res = self.onlineRequestMgr.putRequest( oRequest )
          if not res['OK']:
            self.log.error( "Failed to set removal request to gateway requestDB.", res['Message'] )
          else:
            self.log.info( "Successfully put %s to gateway requestDB." % oRequest.RequestName )
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
