#! /usr/bin/env python
"""
   Get a list of failover directories with logfiles , and clean them
"""
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import gLogger, gConfig
import sys, os

if __name__ == "__main__":

  Script.setUsageMessage( '\n'.join( [ __doc__,
                                       'Usage:',
                                       '  %s [option|cfgfile] <transList>' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  args = Script.getPositionalArgs()

  start = 0
  nbDirs = 0
  if not len( args ) or args[0].lower() == 'all':
    ses = gConfig.getValue( 'Resources/StorageElementGroups/Tier1-Failover', [] )
    if not ses:
      print "No SE found"
      DIRAC.exit( 1 )
  else:
    ses = args[0].split( ',' )
  if len( args ) > 1:
    start = int( args[1] )
  else:
    start = 0
  if len( args ) > 2:
    nbDirs = int( args[2] )
  else:
    nbDirs = 1000


  from DIRAC.Core.DISET.RPCClient import RPCClient
  from DIRAC.Core.Utilities.List                                         import breakListIntoChunks

  from DIRAC.DataManagementSystem.Client.ReplicaManager                     import ReplicaManager
  from types import StringTypes, ListType, DictType, StringType, TupleType
  from DIRAC import S_OK, S_ERROR, gLogger, gConfig
  from DIRAC.AccountingSystem.Client.Types.DataOperation import DataOperation
  from DIRAC.AccountingSystem.Client.DataStoreClient import gDataStoreClient
  from DIRAC.Resources.Storage.StorageElement import StorageElement
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  import time
  class MyReplicaManager( ReplicaManager ):
    def __init( self ):
      ReplicaManager.__init__( self )

    def removeDirectory( self, dir, seList = [] ):
      if not seList:
        seList = gConfig.getValue( 'Resources/StorageElementGroups/SE_Cleaning_List', [] )
      failed = []
      print dir, seList
      for se in sorted( seList ):
        res = self.__removeStorageDirectory( dir, se )
        if not res['OK']:
          print res
          failed.append( se )
      if failed:
        return DIRAC.S_ERROR( "Failed to remove directory at %s" % failed )
      res = self.removeCatalogDirectory( dir, recursive = True, singleFile = True )
      if not res['OK']:
        return res
      return DIRAC.S_OK()

    def __verifyOperationPermission( self, path ):
      """  Check if we have write permission to the given directory
      """
      fc = FileCatalog()
      res = fc.getPathPermissions( path )
      if not res['OK']:
        return res
      paths = path
      if type( path ) in StringTypes:
        paths = [ path ]
      for path in paths:
        if path not in res['Value']['Successful']:
          return S_OK( False )
        catalogPerm = res['Value']['Successful'][path]
        if not ( "Write" in catalogPerm and catalogPerm['Write'] ):
          return S_OK( False )
      return S_OK( True )
    def __initialiseAccountingObject( self, operation, se, files ):
      import DIRAC
      accountingDict = {}
      accountingDict['OperationType'] = operation
      accountingDict['User'] = 'acsmith'
      accountingDict['Protocol'] = 'ReplicaManager'
      accountingDict['RegistrationTime'] = 0.0
      accountingDict['RegistrationOK'] = 0
      accountingDict['RegistrationTotal'] = 0
      accountingDict['Destination'] = se
      accountingDict['TransferTotal'] = files
      accountingDict['TransferOK'] = files
      accountingDict['TransferSize'] = files
      accountingDict['TransferTime'] = 0.0
      accountingDict['FinalStatus'] = 'Successful'
      accountingDict['Source'] = DIRAC.siteName()
      oDataOperation = DataOperation()
      oDataOperation.setValuesFromDict( accountingDict )
      return oDataOperation
    def __removePhysicalReplica( self, storageElementName, pfnsToRemove ):
      gLogger.verbose( "ReplicaManager.__removePhysicalReplica: Attempting to remove %s pfns at %s." % ( len( pfnsToRemove ),
                                                                                                         storageElementName ) )
      storageElement = StorageElement( storageElementName )
      res = storageElement.isValid()
      if not res['OK']:
        errStr = "ReplicaManager.__removePhysicalReplica: The storage element is not currently valid."
        gLogger.error( errStr, "%s %s" % ( storageElementName, res['Message'] ) )
        return S_ERROR( errStr )
      oDataOperation = self.__initialiseAccountingObject( 'removePhysicalReplica', storageElementName, len( pfnsToRemove ) )
      oDataOperation.setStartTime()
      startTime = time.time()
      res = storageElement.removeFile( pfnsToRemove )
      oDataOperation.setEndTime()
      oDataOperation.setValueByKey( 'TransferTime', time.time() - startTime )
      if not res['OK']:
        oDataOperation.setValueByKey( 'TransferOK', 0 )
        oDataOperation.setValueByKey( 'FinalStatus', 'Failed' )
        gDataStoreClient.addRegister( oDataOperation )
        errStr = "ReplicaManager.__removePhysicalReplica: Failed to remove replicas."
        gLogger.error( errStr, res['Message'] )
        return S_ERROR( errStr )
      else:
        oDataOperation.setValueByKey( 'TransferOK', len( res['Value']['Successful'].keys() ) )
        gDataStoreClient.addRegister( oDataOperation )
        infoStr = "ReplicaManager.__removePhysicalReplica: Successfully issued accounting removal request."
        gLogger.info( infoStr )
        for surl, value in res['Value']['Successful'].items():
          ret = storageElement.getPfnForProtocol( surl, self.registrationProtocol, withPort = False )
          if not ret['OK']:
            res['Value']['Successful'][surl] = surl
          else:
            res['Value']['Successful'][surl] = ret['Value']
        return res
    def removePhysicalReplica( self, storageElementName, lfn ):
      """ Remove replica from Storage Element.

         'lfn' are the files to be removed
         'storageElementName' is the storage where the file is to be removed
      """
      if type( lfn ) == ListType:
        lfns = lfn
      elif type( lfn ) == StringType:
        lfns = [lfn]
      else:
        errStr = "ReplicaManager.removePhysicalReplica: Supplied lfns must be string or list of strings."
        gLogger.error( errStr )
        return S_ERROR( errStr )
      # Check that we have write permissions to this directory.
      res = self.__verifyOperationPermission( lfns )
      if not res['OK']:
        return res
      if not res['Value']:
        errStr = "ReplicaManager.__replicate: Write access not permitted for this credential."
        gLogger.error( errStr, lfns )
        return S_ERROR( errStr )
      gLogger.verbose( "ReplicaManager.removePhysicalReplica: Attempting to remove %s lfns at %s." % ( len( lfns ),
                                                                                                       storageElementName ) )
      gLogger.verbose( "ReplicaManager.removePhysicalReplica: Attempting to resolve replicas." )
      res = self.fileCatalogue.getReplicas( lfns )
      if not res['OK']:
        errStr = "ReplicaManager.removePhysicalReplica: Completely failed to get replicas for lfns."
        gLogger.error( errStr, res['Message'] )
        return res
      failed = res['Value']['Failed']
      successful = {}
      pfnDict = {}
      for lfn, repDict in res['Value']['Successful'].items():
        if not repDict.has_key( storageElementName ):
          # The file doesn't exist at the storage element so don't have to remove it
          successful[lfn] = True
        else:
          sePfn = repDict[storageElementName]
          pfnDict[sePfn] = lfn
      gLogger.verbose( "ReplicaManager.removePhysicalReplica: Resolved %s pfns for removal at %s." % ( len( pfnDict.keys() ),
                                                                                                       storageElementName ) )
      res = self.__removePhysicalReplica( storageElementName, pfnDict.keys() )
      if not res['OK']:
        gLogger.error( "Error removing %s from %s" % ( pfnDict, storageElementName ), res['Message'] )
        return res
      for pfn, error in res['Value']['Failed'].items():
        failed[pfnDict[pfn]] = error
      for pfn in res['Value']['Successful'].keys():
        successful[pfnDict[pfn]] = True
      resDict = {'Successful':successful, 'Failed':failed}
      return S_OK( resDict )


    def __removeStorageDirectory( self, directory, storageElement ):
      """ delete SE directory

      :param self: self reference
      :param str directory: folder to be removed
      :param str storageElement: DIRAC SE name
      """
      gLogger.info( 'Removing the contents of %s at %s' % ( directory, storageElement ) )
      res = self.getPfnForLfn( [directory], storageElement )
      if not res['OK']:
        gLogger.error( "Failed to get PFN for directory", res['Message'] )
        return res
      for directory, error in res['Value']['Failed'].items():
        gLogger.error( 'Failed to obtain directory PFN from LFN', '%s %s' % ( directory, error ) )
      if res['Value']['Failed']:
        return S_ERROR( 'Failed to obtain directory PFN from LFNs' )
      storageDirectory = res['Value']['Successful'].values()[0]
      print storageDirectory, storageElement
      res = self.getStorageFileExists( storageDirectory, storageElement, singleFile = True )
      if not res['OK']:
        gLogger.error( "Failed to obtain existance of directory", res['Message'] )
        return res
      exists = res['Value']
      if not exists:
        gLogger.info( "The directory %s does not exist at %s " % ( directory, storageElement ) )
        return S_OK()
      res = self.removeStorageDirectory( storageDirectory, storageElement, recursive = True, singleDirectory = True )
      if not res['OK']:
        gLogger.error( "Failed to remove storage directory", res['Message'] )
        return res
      gLogger.info( "Successfully removed %d files from %s at %s" % ( res['Value']['FilesRemoved'],
                                                                      directory,
                                                                      storageElement ) )
      return S_OK()

  rpc = RPCClient( 'DataManagement/StorageUsage' )
  rm = MyReplicaManager()

  res = rpc.getStorageDirectoryData( '', 'LOG', '', ses )
  if not res['OK']:
    print 'Failed to get directories', res['Message']
    DIRAC.exit( 2 )

  dirInfo = res['Value']
  dirs = sorted( dirInfo )

  if nbDirs:
    dirs = dirs[start:start + nbDirs]

  removedFiles = 0
  removedDirs = 0
  failedFiles = 0
  failedDirs = 0
  chunkSize = 1000
  print 'Removing now files in %d directories at %s: ' % ( len( dirs ), ses )
  print dirs
  for dir in dirs:
    sys.stdout.write( '.' )
    sys.stdout.flush()
    gLogger.setLevel( 'FATAL' )
    res = rm.getFilesFromDirectory( dir )
    gLogger.setLevel( 'WARNING' )
    if not res['OK']:
      print "\nError getting files from directory %s" % dir, res['Message']
      continue

    filesInDir = -1
    notOKFiles = []
    if res['Value']:
      filesInDir = len( res['Value'] )
      sys.stdout.write( '+' )
      sys.stdout.flush()
      res = rm.getReplicas( res['Value'] )
      if not res['OK']:
        print "\nError getting replicas of %d files" % len( res['Value'] ), res['Message']
        continue
      replicas = res['Value']['Successful']
      files = [f for f in replicas if [se for se in replicas[f] if se in ses]]
      seGroup = {}
      for f in files:
        for se in [se for se in replicas[f] if se in ses]:
          seGroup.setdefault( se, [] ).append( f )
      for se, lfns in seGroup.items():
        sys.stdout.write( '-' )
        sys.stdout.flush()
        res = rm.removePhysicalReplica( se, lfns )
        # print se, lfns, res['Value']
        if not res['OK']:
          print "\nError removing %d files from %s" % ( len( lfns ), se ), res['Message']
          notOKFiles += [f for f in lfns if f not in notOKFiles]
        else:
          notOKFiles += [f for f in res['Value']['Failed'] if f not in notOKFiles]
          replicaDict = {}
          for f in res['Value']['Successful']:
            pfn = replicas[f][se]
            replicaDict[f] = {'SE':se, 'PFN':pfn}
          # print replicaDict
          res = rm.fileCatalogue.removeReplica( replicaDict )
          if not res['OK']:
            print '\nError removing %d replicas from catalog' % len( replicaDict ), res['Message']
          else:
            notOKFiles += [f for f in replicaDict if f in res['Value']['Failed']]

      okFiles = [f for f in files if f not in notOKFiles]
      # print okFiles
      removedFiles += len( okFiles )
      failedFiles += len( notOKFiles )
      # Now remove the files from catalog
      res = rm.fileCatalogue.removeFile( okFiles )
      # print res
      if not res['OK']:
        print "\nError removing %d files from catalog:" % len( okFiles ), res['Message']
      else:
        filesInDir -= len( okFiles )
    # print filesInDir, 'files remaining in', dir
    if filesInDir == 0:
      sys.stdout.write( '_' )
      sys.stdout.flush()
      # res = rm.removeDirectory( dir, ses )
      res = rm.cleanLogicalDirectory( dir )
      if not res['OK']:
        print "\nError removing directory %s" % dir, res['Message']
        failedDirs += 1
      else:
        removedDirs += 1

  print '\n'
  if failedFiles:
    print "Failed to remove %d files" % failedFiles
  if failedDirs:
    print "Failed to remove %d directories" % failedDirs
  print '%d files were removed, and %d directories' % ( removedFiles, removedDirs )


