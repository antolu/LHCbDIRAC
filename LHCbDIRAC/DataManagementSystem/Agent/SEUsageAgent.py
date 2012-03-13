"""  SEUsageAgent browses the SEs to determine their content and store it into a DB.
"""
__RCSID__ = "$Id$"

from DIRAC  import gLogger, gMonitor, S_OK, S_ERROR, rootPath, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv
from DIRAC.DataManagementSystem.Agent.NamespaceBrowser import NamespaceBrowser
from DIRAC.Core.Utilities.SiteSEMapping                import getSEsForSite
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
from DIRAC.Core.Utilities.List import sortList
from DIRAC.Resources.Storage.StorageElement         import StorageElement
import time, os, urllib2, tarfile, signal
from types import *

def alarmTimeoutHandler( *args ):
  raise Exception( 'Timeout' )

AGENT_NAME = 'DataManagement/SEUsageAgent'

class SEUsageAgent( AgentModule ):
  def initialize( self ):

    if self.am_getOption( 'DirectDB', False ):
      from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB
      self.__storageUsage = StorageUsageDB()
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.__storageUsage = RPCClient( 'DataManagement/StorageUsage' )

    self.__replicaManager = ReplicaManager()

    #self.am_setModuleParam( "shifterProxy", "DataManager" )
    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/DataManager
    # the shifterProxy option in the Configsorteduration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'DataManager' )
    self.am_setModuleParam( "shifterProxyLocation", "%s/runit/%s/proxy" % ( rootPath, AGENT_NAME ) )
    self.activeSites = self.am_getOption( 'ActiveSites' )
    # maximum delay after storage dump creation
    self.maximumDelaySinceSD = self.am_getOption( 'MaximumDelaySinceStorageDump' , 43200 )
    self.__workDirectory = self.am_getOption( "WorkDirectory" )
    if not os.path.isdir( self.__workDirectory ):
      os.makedirs( self.__workDirectory )
    self.log.info( "Working directory is %s" % self.__workDirectory )
    self.__InputFilesLocation = self.am_getOption( 'InputFilesLocation' )
    if not os.path.isdir( self.__InputFilesLocation ):
      os.makedirs( self.__InputFilesLocation )
    if os.path.isdir( self.__InputFilesLocation ):
      gLogger.info( "Input files will be downloaded to: %s" % self.__InputFilesLocation )
    else:
      gLogger.info( "ERROR! could not create directory %s" % self.__InputFilesLocation )
      return S_ERROR()
    self.__spaceTokToIgnore = self.am_getOption( 'SpaceTokenToIgnore' )# STs to skip during checks
    return S_OK()



  def execute( self ):
    """ Loops on the input files to read the content of Storage Elements, process them, and store the result into the DB.
    It reads directory by directory (every row of the input file being a directory).
    If the directory exists in the StorageUsage su_Directory table, and if a replica also exists for the given SE in the su_SEUsage table, then the directory and
    its usage are stored in the replica table (the se_Usage table) together with the insertion time, otherwise it is added to the problematic data table (problematicDirs) """
    gLogger.info( "SEUsageAgent: start the execute method\n" )
    gLogger.info( "Sites active for checks: %s " % self.activeSites )
    siteList = self.activeSites.split( ',' )
    timingPerSite = {}
    self.spaceTokens = {}
    self.siteConfig = {}
    self.specialReplicas = ['archive', 'freezer', 'failover']
    siteList.sort()
    for LcgSite in siteList:
      site = LcgSite.split( '.' )[1]
      timingPerSite[ site ] = {}
      start = time.time()
      res = self.setupSiteConfig( LcgSite )
      if not res[ 'OK' ]:
        gLogger.warn( "Error during site configuration %s " % res['Message'] )
        continue

      gLogger.info( "Parse and read input files for site: %s" % site )
      res = self.readInputFile( site )
      if not res['OK']:
        gLogger.error( "Failed to read input files for site %s " % site )
        continue
      if res['Value'] < 0:
        gLogger.warn( "Input files successfully, but not valid for checks (probably too old ). Skip site %s " % site )
        continue

      # ONLY FOR DEBUGGING
      DEBUG = False
      if DEBUG: # get the problematic summary at the beginning (to debug this part without waiting until the end)
        res = self.getProblematicDirsSummary( site )
        if not res[ 'OK' ]:
          return S_ERROR( res['Message'] )
      else: # follow the usual work flow
      # Flush the problematicDirs table
        gLogger.info( "Flushing the problematic directories table for site %s..." % site )
        res = self.__storageUsage.removeAllFromProblematicDirs( site )
        if not res['OK']:
          gLogger.error( "Error for site %s: Could not remove old entries from the problematicDirs table! %s " % ( site, res['Message'] ) )
          continue
        gLogger.info( "Removed %d entries from the problematic directories table for site %s" % ( res['Value'], site ) )

     # END OF DEBUGGING MODIFICATION ###########


      # Start the main loop:
      # read all file of type directory summary for the given site:
      pathToSummary = self.siteConfig[ site ][ 'pathToInputFiles' ]
      for fileP3 in os.listdir( pathToSummary ):
        fullPathFileP3 = os.path.join( pathToSummary, fileP3 )
        if 'dirSummary' not in fileP3:
          continue
        gLogger.info( "Start to scan file %s" % fullPathFileP3 )
        for line in open( fullPathFileP3, "r" ).readlines():
          splitLine = line.split()
          try:
            spaceToken = splitLine[ 0 ]
            StorageDirPath = splitLine[ 1 ]
            files = splitLine[ 2 ]
            size = splitLine[ 3 ]
          except:
            gLogger.error( "ERROR in input data format. Line is: %s" % line )
            continue
          oneDirDict = {}
          dirPath = StorageDirPath
          #specialRep = False
          replicaType = 'normal'
          for sr in self.specialReplicas:
            prefix = '/lhcb/' + sr
            if prefix in dirPath:
              dirPath = StorageDirPath.split( prefix )[1] # strip the initial prefix, to get the LFN as registered in the LFC
              replicaType = sr
              gLogger.info( "prefix: %s \n StorageDirPath: %s\n dirPath: %s" % ( prefix, StorageDirPath, dirPath ) )
          oneDirDict[ dirPath ] = { 'SpaceToken': spaceToken, 'Files': files, 'Size': size , 'Updated': 'na', 'Site': site, 'ReplicaType': replicaType }
          # the format of the entry to be processed must be a dictionary with LFN path as key
          # use this format for consistency with already existing methods of StorageUsageDB which take in input a dictionary like this
          gLogger.info( "Processing directory: %s" % ( oneDirDict ) )
          # initialize the isRegistered flag. Change it according to the checks SE vs LFC
          # possible values of isRegistered flag are:
          # NotRegisteredInFC: data not registered in FC
          # RegisteredInFC: data correctly registered in FC
          # MissingDataFromSE: the directory exists in the LFC for that SE, but there is less data on the SE than what reported in the FC
          isRegistered = False
          LFCFiles = -1
          LFCSize = -1
          gLogger.verbose( "SEUsageAgent: check if dirName exists in su_Directory: %s" % dirPath )
          # select entries with that LFN path (no reference to a particular site in this query)
          res = self.__storageUsage.getIDs( oneDirDict )
          if not res['OK']:
            gLogger.info( "SEUsageAgent: ERROR failed to get DID from StorageUsageDB.su_Directory table for dir: %s " % dirPath )
            continue
          elif not res['Value']:
            gLogger.info( "SEUsageAgent: NO LFN registered in the LFC for the given path %s => insert the entry in the problematicDirs table and delete this entry from the replicas table" % dirPath )
            isRegistered = 'NotRegisteredInFC'
          else:
            value = res['Value']
            gLogger.info( "SEUsageAgent: directory LFN  is registered in the LFC, output of getIDs is %s" % value )
            for dir in value:
              gLogger.verbose( "SEUsageAgent: Path: %s su_Directory.DID %d" % ( dir, value[dir] ) )
            gLogger.verbose( "SEUsageAgent: check if this particular replica is registered in the LFC." )
            res = self.__storageUsage.getAllReplicasInFC( dirPath )
            if not res['OK']:
              gLogger.error( "SEUsageAgent: ERROR failed to get replicas for %s directory " % dirPath )
              continue
            elif not res['Value']:
              gLogger.warn( "SEUsageAgent: NO replica found for %s on any SE! Anomalous case: the LFN is registered in the FC but with NO replica! For the time being, insert it into problematicDirs table " % dir )
              # we should decide what to do in this case. This might happen, but it is a problem at FC level... TBD!
              isRegistered = 'NotRegisteredInFC'
            else: # got some replicas! let's see if there is one for this SE
              associatedDiracSEs = self.spaceTokens[ site ][spaceToken]['DiracSEs']
              gLogger.verbose( "SpaceToken: %s list of its DiracSEs: %s" % ( spaceToken, associatedDiracSEs ) )
              LFCFiles = 0
              LFCSize = 0
              for lfn in res['Value'].keys():
                matchedSE = ''
                for se in res['Value'][lfn].keys():
                  gLogger.verbose( "SpaceToken: %s -- se: %s" % ( spaceToken, se ) )
                  if se in associatedDiracSEs:
                    if oneDirDict[ dirPath ][ 'ReplicaType' ] in self.specialReplicas:# consider only the LFC replicas of the corresponding Dirac SE
                      gLogger.info( "SpecialReplica: %s" % oneDirDict[ dirPath ][ 'ReplicaType' ] )
                      SESuffix = oneDirDict[ dirPath ][ 'ReplicaType' ].upper()
                      if SESuffix not in se:
                        gLogger.info( "Se does not contain the suffix: %s. Skip it" % SESuffix )
                        continue
                    LFCFiles += int( res['Value'][lfn][ se ]['Files'] )
                    LFCSize += int( res['Value'][lfn][ se ]['Size'] )
                    gLogger.info( "==> the replica is registered in the FC with DiracSE= %s" % se )
                    # this is because in the same directory there can be files belonging to (e.g.) DST and M-DST
                    if not matchedSE:
                      matchedSE = se
                    else:
                      matchedSE = matchedSE + '.and.' + se
              # check also whether the number of files and total directory size match:
              SESize = int( oneDirDict[ dirPath ]['Size'] )
              SEFiles = int( oneDirDict[ dirPath ]['Files'] )
              gLogger.info( "Matched SE: %s" % matchedSE )
              oneDirDict[ dirPath ]['SEName'] = matchedSE
              gLogger.info( "LFCFiles = %d LFCSize = %d SEFiles = %d SESize = %d" % ( LFCFiles, LFCSize, SEFiles, SESize ) )
              if SESize > LFCSize:
                gLogger.info( "Data on SE exceeds what reported in LFC! some data not registered in LFC" )
                isRegistered = 'NotRegisteredInFC'
              elif SESize < LFCSize:
                gLogger.info( "Data on LFC exceeds what reported by SE dump! missing data from storage" )
                isRegistered = 'MissingDataFromSE'
              elif LFCFiles == SEFiles and LFCSize == SESize:
                gLogger.info( "Number of files and total size matches with what reported by LFC" )
                isRegistered = 'RegisteredInFC'
              else:
                gLogger.info( "Unexpected case: SESize = LFCSize but SEFiles != LFCFiles" )
                isRegistered = 'InconsistentFilesSize'

          gLogger.info( "SEUsageAgent: isRegistered flag is %s" % isRegistered )
        # now insert the entry into the problematicDirs table, or the replicas table according the isRegistered flag.
          if not isRegistered:
            gLogger.error( "ERROR: the isRegistered flag could not be updated for this directory: %s " % oneDirDict )
            continue
          if isRegistered == 'NotRegisteredInFC' or isRegistered == 'MissingDataFromSE' or isRegistered == 'InconsistentFilesSize':
            gLogger.info( "SEUsageAgent: problematic directory! Add the entry %s to problematicDirs table. Before (if necessary) remove it from se_Usage table" % ( dirPath ) )
            res = self.__storageUsage.removeDirFromSe_Usage( oneDirDict )
            if not res[ 'OK' ]:
              gLogger.error( "SEUsageAgent: ERROR failed to remove from se_Usage table: %s" % oneDirDict )
              continue
            else:
              removedDirs = res[ 'Value' ]
              if removedDirs:
                gLogger.verbose( "SEUsageAgent: entry %s correctly removed from se_Usage table" % oneDirDict )
              else:
                gLogger.verbose( "SEUsageAgent: entry %s was NOT in se_Usage table" % oneDirDict )
              #update the oneDirDict and insert into problematicDirs
              oneDirDict[ dirPath ][ 'Problem' ] = isRegistered
              oneDirDict[ dirPath ][ 'LFCFiles'] = LFCFiles
              oneDirDict[ dirPath ][ 'LFCSize' ] = LFCSize
              res = self.__storageUsage.publishToProblematicDirs( oneDirDict )
              if not res['OK']:
                gLogger.error( "SEUsageAgent: failed to publish entry %s to problematicDirs table" % dirPath )
              else:
                gLogger.info( "SEUsageAgent: entry %s correctly published to problematicDirs table" % oneDirDict )
          elif isRegistered == 'RegisteredInFC': # publish to se_Usage table!
            gLogger.verbose( "SEUsageAgent: replica %s is registered! remove from problematicDirs (if necessary) and  publish to se_Usage table" % dirPath )
            res = self.__storageUsage.removeDirFromProblematicDirs( oneDirDict )
            if not res[ 'OK' ]:
              gLogger.error( "SEUsageAgent: ERROR failed to remove from problematicDirs: %s" % oneDirDict )
              continue
            else:
              removedDirs = res[ 'Value' ]
              if removedDirs:
                gLogger.verbose( "SEUsageAgent: entry %s correctly removed from problematicDirs" % oneDirDict )
              else:
                gLogger.verbose( "SEUsageAgent: entry %s was NOT in problematicDirs" % oneDirDict )
              res = self.__storageUsage.publishToSEReplicas( oneDirDict )
              if not res['OK']:
                gLogger.warn( "SEUsageAgent: failed to publish %s entry to se_Usage table" % oneDirDict )
              else:
                gLogger.info( "SEUsageAgent: entry %s correctly published to se_Usage" % oneDirDict )
          else:
            gLogger.error( "Unknown value of isRegistered flag: %s " % isRegistered )

        gLogger.info( "Finished loop on file: %s " % fileP3 )

      gLogger.info( "Finished loop for site: %s " % site )
      # query problematicDirs table to get a summary of directories with the flag: NotRegisteredInFC
      gLogger.info( "Get the summary of problematic directories.." )
      res = self.getProblematicDirsSummary( site )
      if not res[ 'OK' ]:
        return S_ERROR( res['Message'] )
        continue

      end = time.time()
      timingPerSite[ site ]['timePerCycle'] = end - start

    gLogger.info( "--------- End of cycle ------------------" )
    gLogger.info( "checked sites:" )
    for site in timingPerSite.keys():
      gLogger.info( "Site: %s -  total time %s" % ( site, timingPerSite[ site ] ) )
    return S_OK()


  def setupSiteConfig( self, LcgSite ):
    """ Setup the configuration for the site
    """
    site = LcgSite.split( '.' )[1]
    self.spaceTokens[ site ] = { 'LHCb-Tape' : {'year' : '2011', 'DiracSEs':[ site + '-RAW', site + '-RDST', site + '-ARCHIVE']},
                                 'LHCb-Disk' : {'year': '2011', 'DiracSEs':[ site + '-DST', site + '_M-DST', site + '_MC_M-DST', site + '_MC-DST', site + '-FAILOVER']},
                                 'LHCb_USER' : {'year': '2011', 'DiracSEs':[ site + '-USER']},
                                 'LHCb_RAW' : {'year': '2010', 'DiracSEs':[ site + '-RAW']},
                                 'LHCb_RDST' : {'year': '2010', 'DiracSEs':[ site + '-RDST']},
                                 'LHCb_M-DST': {'year': '2010', 'DiracSEs':[ site + '_M-DST']},
                                 'LHCb_DST'  : {'year': '2010', 'DiracSEs':[ site + '-DST']},
                                 'LHCb_MC_M-DST': {'year': '2010', 'DiracSEs':[ site + '_MC_M-DST']},
                                 'LHCb_MC_DST'  : {'year': '2010', 'DiracSEs': [ site + '_MC-DST']},
                                 'LHCb_FAILOVER' : {'year': '2010', 'DiracSEs' : [ site + '-FAILOVER']}
                                 }
    # for CERN: two more SEs: FREEZER AND HISTO

    for st in self.spaceTokens[ site ].keys():
      if st in self.__spaceTokToIgnore:
        self.spaceTokens[ site ][ st ]['Check'] = False
      else:
        self.spaceTokens[ site ][ st ]['Check'] = True


    rootConfigPath = '/Operations/DataConsistency'
    res = gConfig.getOptionsDict( "%s/%s" % ( rootConfigPath, site ) )
    if not res[ 'OK' ]:
      gLogger.error( "could not get configuration for site %s : %s " % ( site, res['Message'] ) )
      return S_ERROR( res['Message'] )
    StorageDumpFileName = res[ 'Value' ]['StorageDumpFileName']
    StorageDumpURL = res[ 'Value' ]['StorageDumpURL']
    fileType = res[ 'Value' ]['FileType']
    pathInsideTar = res[ 'Value' ]['PathInsideTar']
    #res = getSEsForSite( site )
    res = getSEsForSite( LcgSite )
    if not res[ 'OK' ]:
      gLogger.error( 'could not get SEs for site %s ' % site )
      return S_ERROR( res['Message'] )
    SEs = res['Value']
    gLogger.info( "SEs: %s" % SEs )
    SeName = site + '-RAW'
    res = gConfig.getOption( '/Resources/StorageElements/%s/AccessProtocol.1/Path' % ( SeName ) )
    if not res[ 'OK' ]:
      gLogger.error( "could not get configuration for SE %s " % res['Message'] )
      return S_ERROR( res['Message'] )
    SAPath = res['Value']

    self.siteConfig[ site ] = { 'originFileName': StorageDumpFileName,
                                'pathInsideTar': pathInsideTar,
                                'originURL': StorageDumpURL,
                                'targetPath': os.path.join( self.__InputFilesLocation, 'downloadedFiles', site ),
                                'pathToInputFiles': os.path.join( self.__InputFilesLocation, 'goodFormat', site ),
                                'DiracName': LcgSite,
                                'FileNameType': fileType,
                                'SEs' : SEs,
                                'SAPath' : SAPath
                               }
    gLogger.info( "Configuration for site %s : %s " % ( site, self.siteConfig[ site ] ) )
    return S_OK()


  def readInputFile( self, site ):
    """ Download, read and parse input files with SEs content.
        Write down the results to  ASCII files.
        There are 3 phases in the manipulation of input files:
        phase 1- it is directly the format of the DB query output, right after uncompressing the tar file provided by the site:
        PFN | size | update (ms)
        phase 2- one row per file, with format:  LFN size update
        phase 3- directory summary files: one row per directory, with format:
        SE DirectoryLFN NumOfFiles TotalSize Update(actually, not used)

         """

    retCode = 0
    originFileName = self.siteConfig[ site ][ 'originFileName' ]
    originURL = self.siteConfig[ site ][ 'originURL' ]
    targetPath = self.siteConfig[ site ][ 'targetPath' ]
    if not os.path.isdir( targetPath ):
      os.makedirs( targetPath )
    pathToInputFiles = self.siteConfig[ site ][ 'pathToInputFiles' ]
    if not os.path.isdir( pathToInputFiles ):
      os.makedirs( pathToInputFiles )
    if pathToInputFiles[-1] != "/":
      pathToInputFiles = "%s/" % pathToInputFiles
    gLogger.info( "Reading input files for site %s " % site )
    gLogger.info( "originFileName: %s , originURL: %s ,targetPath: %s , pathToInputFiles: %s " % ( originFileName, originURL, targetPath, pathToInputFiles ) )

    if targetPath[-1] != "/":
      targetPath = "%s/" % targetPath
    targetPathForDownload = targetPath + self.siteConfig[ site ]['pathInsideTar']
    gLogger.info( "Target path to download input files: %s" % targetPathForDownload )
    # delete all existing files in the target directory if necessary:
    previousFiles = []
    try:
      previousFiles = os.listdir( targetPathForDownload )
      gLogger.info( "In %s found these files: %s" % ( targetPathForDownload, previousFiles ) )
    except:
      gLogger.info( "no leftover to remove from %s. Proceed downloading input files..." % targetPathForDownload )
    if previousFiles:
      gLogger.info( "delete these files: %s " % previousFiles )
      for file in previousFiles:
        fullPath = os.path.join( targetPathForDownload , file )
        gLogger.info( "removing %s" % fullPath )
        os.remove( fullPath )

    # Download input data made available by the sites. Reuse the code of dirac-install: urlretrieveTimeout, downloadAndExtractTarball
    res = self.downloadAndExtractTarball( originFileName, originURL, targetPath )
    if not res['OK']:
       return S_ERROR( res['Message'] )

    defaultDate = 'na'
    defaultSize = 0

    gLogger.info( "Check storage dump creation time.." )
    res = self.checkCreationDate( targetPathForDownload )
    if not res[ 'OK' ]:
      gLogger.error( "Failed to check storage dump date for site %s " % site )
      return S_ERROR( res )
    if res['Value'] != 0:
      gLogger.warn( "Stale storage dump for site %s " % site )
      retCode = -1
      return S_OK( retCode )
    gLogger.info( "Creation date check is fine!" )

    InputFilesListP1 = os.listdir( targetPathForDownload )
    gLogger.info( "List of raw input files in %s: %s " % ( targetPathForDownload, InputFilesListP1 ) )

    # delete all leftovers of previous runs from the pathToInputFiles
    try:
      previousParsedFiles = os.listdir( pathToInputFiles )
      gLogger.info( "In %s found these files: %s" % ( pathToInputFiles, previousParsedFiles ) )
    except:
      gLogger.info( "no leftover to remove from %s . Proceed to parse the input files..." % pathToInputFiles )
    if previousParsedFiles:
      gLogger.info( "delete these files: %s " % previousParsedFiles )
      for file in previousParsedFiles:
        fullPath = os.path.join( pathToInputFiles, file )
        gLogger.info( "removing %s" % fullPath )
        os.remove( fullPath )

    # if necessary, call the special Castor pre-parser which analyse the name server dump and creates one file per space token.
    # the space token attribution is totally fake! it's based on the namespace. For Castor, no other way to do it
    if site in ['RAL']:
      gLogger.info( "Calling special parser for Castor" )
      res = self.castorPreParser( site, targetPathForDownload )
      if not res['OK']:
        gLogger.error( "Error parsing storage dump" )
      else:
        # recompute the list of input files
        InputFilesListP1 = os.listdir( targetPathForDownload )
        gLogger.info( "List of raw input files in %s: %s " % ( targetPathForDownload, InputFilesListP1 ) )


    # if necessary, merge the files in order to have one file per space token: LHCb-Tape, LHCb-Disk, LHCb_USER
    # this is necessary in the transition phase while there are still some data on the old space tokens of 2010
    gLogger.info( "Merge the input files to have one file per space token" )
    # Loop on N files relative to the site (one file for each space token)
    # previously open files for writing
    # every input file corresponds to one space token
    # whereas for the output, there is one file per NEW space token, so a merging is done

    diskST = ['LHCb-Disk', 'LHCb_M-DST', 'LHCb_DST', 'LHCb_MC_M-DST', 'LHCb_MC_DST', 'LHCb_FAILOVER']
    tapeST = ['LHCb-Tape', 'LHCb_RAW', 'LHCb_RDST']
    outputFileMerged = {}
    for st in self.spaceTokens[ site ].keys():
      outputFileMerged[ st ] = {}
      if self.spaceTokens[ site ][ st ][ 'year'] == '2011':
        gLogger.info( "Preparing output files for space tokens: %s" % st )
        # merged file:
        fileP2Merged = pathToInputFiles + st + '.Merged.txt'
        outputFileMerged[ st ]['MergedFileName'] = fileP2Merged
        gLogger.info( "Opening file %s in w mode" % fileP2Merged )
        fP2Merged = open( fileP2Merged, "w" )
        outputFileMerged[ st ]['pointerToMergedFile' ] = fP2Merged
        # directory summary file:
        fileP3DirSummary = pathToInputFiles + st + '.dirSummary.txt'
        outputFileMerged[ st ]['DirSummaryFileName'] = fileP3DirSummary
        gLogger.info( "Opening file %s in w mode" % fileP3DirSummary )
        fP3DirSummary = open( fileP3DirSummary, "w" )
        outputFileMerged[ st ]['pointerToDirSummaryFile' ] = fP3DirSummary
    for st in diskST:
      try:
        outputFileMerged[ st ] = outputFileMerged[ 'LHCb-Disk']
      except:
        gLogger.error( "no pointer to file for st=%s " % st )
    for st in tapeST:
      try:
        outputFileMerged[ st ] = outputFileMerged['LHCb-Tape']
      except:
        gLogger.error( "no pointer to file for st=%s " % st )
    gLogger.info( "Parsed output files : " )
    for st in outputFileMerged.keys():
      gLogger.info( "space token: %s -> \n  %s\n %s  " % ( st, outputFileMerged[ st ]['MergedFileName'], outputFileMerged[ st ]['DirSummaryFileName'] ) )

    for inputFileP1 in InputFilesListP1:
      gLogger.info( "+++++ input file: %s ++++++" % inputFileP1 )
      # the expected input file line is: pfn | size | date
      # manipulate the input file to create a directory summary file (one row per directory)
      # Check the validity of the input file name:
      splitFile = inputFileP1.split( '.' )
      st = splitFile[ 0 ]
      if st not in self.spaceTokens[ site ].keys():
        gLogger.warn( "Not a  valid space token in the file name: %s " % inputFileP1 )
        continue

      completeSTId = st
      if len( splitFile ) > 3:
        # file with format provided by SARA e.g. LHCb_RAW.31455.INACTIVE.txt
        completeSTId = splitFile[0] + '.' + splitFile[1] + '.' + splitFile[2]
      fullPathFileP1 = os.path.join( targetPathForDownload, inputFileP1 )
      if not os.path.getsize( fullPathFileP1 ):
        gLogger.info( "%s file has zero size, will be skipped " % inputFileP1 )
        continue
      if 'INACTIVE' in inputFileP1:
        gLogger.info( "WARNING: File %s will be analysed, even if space token is inactive" % inputFileP1 )
      gLogger.info( "processing input file for space token: %s " % st )
      fP2 = outputFileMerged[ st ]['pointerToMergedFile' ]
      fileP2 = outputFileMerged[ st ]['MergedFileName']
      gLogger.info( "Reading from file %s\n and writing to: %s" % ( fullPathFileP1, fileP2 ) )
      totalLines = 0 # counts all lines in input
      processedLines = 0 # counts all processed lines
      dirac_dir_lines = 0
      totalSize = 0
      totalFiles = 0
      for line in open( fullPathFileP1, "r" ).readlines():
        totalLines += 1
        try:
          splitLine = line.split( '|' )
        except:
          gLogger.error( "Error: impossible to split line : %s" % line )
          continue

        providedFilePath = splitLine[0].rstrip()
        if 'dirac_directory' in providedFilePath:
          dirac_dir_lines += 1
          continue
        # get LFN from PFN if necessary:
        filePath = ''
        if self.siteConfig[ site ]['FileNameType'] == 'LFN':
          filePath = providedFilePath
        elif self.siteConfig[ site ]['FileNameType'] == 'PFN':
          res = self.getLFNPath( site, providedFilePath )
          if not res[ 'OK' ]:
            gLogger.error( "ERROR getLFNPath returned: %s " % res )
            continue
          filePath = res[ 'Value' ]
        gLogger.debug( "filePath: %s" % filePath )
        if not filePath:
          gLogger.info( "SEUsageAgent: it was not possible to get the LFN for PFN=%s, skip this line" % PFNfilePath )
          continue

        gLogger.debug( "splitLine: %s " % splitLine )
        size = splitLine[1].lstrip()
        totalSize += int( size )
        totalFiles += 1
        updated = splitLine[2].lstrip()
        newLine = filePath + ' ' + size + ' ' + updated
        if newLine[-1] != "\n":
          newLine = "%s\n" % newLine
        fP2.write( "%s" % newLine )
        processedLines += 1
#        except:
#          gLogger.error( "Error in input line format! Line is: %s" % line ) # the last line of these files is empty, so it will give this exception
#          continue
      fP2.flush()

      gLogger.info( "Cleaning the STSummary table entries for site %s and space token %s ..." % ( site, completeSTId ) )
      res = self.__storageUsage.removeSTSummary( site, completeSTId )
      if not res['OK']:
        gLogger.error( "Error for site %s: Could not remove old entries from the STSummary table! %s " % ( site, res['Message'] ) )
        continue
      gLogger.info( "Removed %d entries from the STSummary table for site %s" % ( res['Value'], site ) )

      gLogger.info( "%s - %s Total size: %d , total files: %d : publish to STSummary" % ( site, completeSTId, totalSize, totalFiles ) )
      res = self.__storageUsage.publishTose_STSummary( site, completeSTId, totalSize, totalFiles )
      if not res['OK']:
        gLogger.info( "ERROR: failed to publish %s - %s summary " % ( site, st ) )
        return S_ERROR( res )
      gLogger.info( "Total lines: %d , correctly processed: %d, dirac_directory found %d " % ( totalLines, processedLines, dirac_dir_lines ) )
    # close output files:
    for st in outputFileMerged.keys():
      p2 = outputFileMerged[ st ]['pointerToMergedFile' ]
      p2.close()

    # produce directory summaries:
    mergedFilesList = os.listdir( pathToInputFiles )
    for file in mergedFilesList:
      if 'Merged' not in file:
        continue
      fileP2 = os.path.join( pathToInputFiles, file )
      gLogger.info( "Reading from Merged file fileP2 %s " % fileP2 )
      for spaceTok in self.spaceTokens[ site ].keys():
        gLogger.debug( "..check for space token: %s" % spaceTok )
        if spaceTok in fileP2:
          st = spaceTok
          break

      if not self.spaceTokens[ site ][ st ][ 'Check']:
        gLogger.info( "Skip this space token: %s" % st )
        continue
      gLogger.info( "Space token: %s" % st )
      totalLines = 0 # counts all lines in input
      processedLines = 0 # counts all processed lines
      self.dirDict = {}
      for line in open( fileP2, "r" ).readlines():
        gLogger.debug( "..processing line: %s" % line )
        totalLines += 1
        splitLine = line.split()
        filePath = splitLine[ 0 ]
        updated = defaultDate
        size = int( splitLine[ 1 ] )
        # currently the creation data is NOT stored in the DB. The time stamp stored for every entry is the insertion time
        #updatedMS = int( splitLine[3] )*1./1000
        #updatedTuple = time.gmtime( updatedMS ) # convert to tuple format in UTC. Still to be checked which format the DB requires
        #updated = time.asctime( updatedTuple )
        directories = filePath.split( '/' )
        fileName = directories[len( directories ) - 1 ]
        basePath = ''
        for segment in directories[0:-1]:
          basePath = basePath + segment + '/'
        if basePath not in self.dirDict.keys():
          self.dirDict[ basePath ] = {}
          self.dirDict[ basePath ][ 'Files' ] = 0
          self.dirDict[ basePath ][ 'Size' ] = 0
          self.dirDict[ basePath ][ 'Updated'] = updated
        self.dirDict[ basePath ][ 'Files' ] += 1
        self.dirDict[ basePath ][ 'Size' ] += size
        processedLines += 1

      gLogger.info( "total lines: %d,  correctly processed: %d " % ( totalLines, processedLines ) )
      # write to directory summary file
      fileP3 = outputFileMerged[ st ]['DirSummaryFileName']
      fP3 = outputFileMerged[ st ]['pointerToDirSummaryFile' ]
      gLogger.info( "Writing to file %s" % fileP3 )
      for basePath in self.dirDict.keys():
        summaryLine = st + ' ' + basePath + ' ' + str( self.dirDict[ basePath ][ 'Files' ] ) + ' ' + str( self.dirDict[ basePath ][ 'Size' ] )
        gLogger.debug( "Writing summaryLine %s" % summaryLine )
        fP3.write( "%s\n" % summaryLine )
      fP3.flush()
      fP3.close()

      gLogger.debug( "SEUsageAgent: ----------------summary of ReadInputFile-------------------------" )
      for k in self.dirDict.keys():
        gLogger.debug( "SEUsageAgent: (lfn,st): %s-%s files=%d size=%d updated=%s" % ( k, st , self.dirDict[ k ][ 'Files' ], self.dirDict[ k ][ 'Size' ], self.dirDict[ k ][ 'Updated' ] ) )

    return S_OK( retCode )


  def getLFNPath( self, site, PFNfilePath ):
    """ Given a PFN returns the LFN, stripping the suffix relative to the particular site.
        Important: usually the transformation is done simply removing the SApath of the site. So for ARCHIVE and FREEZER and FAILOVER data:
        the LFN will be: /lhcb/archive/<LFN> etc...
        even if LHCb register those replicas in the LFC with the LFN: <LFN>, stripping the initial '/lhcb/archive'
        this is taken into account by the main method of the agent when it queries for replicas in the LFC
          """

    outputFile = os.path.join( self.__workDirectory, site + ".UnresolvedPFNs.txt" )
    # this should be done with the replicaManager, but it does not work for archive files . tbu why
    #SeName = site + '-RAW'
    #res = self.__replicaManager.getLfnForPfn( PFNfilePath, SeName )
    SEPath = self.siteConfig[ site ][ 'SAPath']
    LFN = 'None'
    try:
      LFN = PFNfilePath.split( SEPath )[1]
    except:
      gLogger.error( "ERROR retrieving LFN from PFN = %s, SEPath = %s " % ( PFNfilePath, SEPath ) )
      if not os.path.exists( outputFile ):
        of = open( outputFile , "w" )
      else:
        of = open( outputFile , "a" )
      of.write( "%s\n" % PFNfilePath )
      of.close()
      return S_ERROR( "Could not retrieve LFN" )
    # additional check on the LFN format:
    if not LFN.startswith( '/lhcb' ):
      gLogger.info( "SEUsageAgent: ERROR! LFN should start with /lhcb: PFN=%s LFN=%s. Skip this file." % ( PFNfilePath, LFN ) )
      if not os.listdir( outputFile ):
        of = open( outputFile , "w" )
      else:
        of = open( outputFile , "a" )
      os.write( "%s\n" % PFNfilePath )
      os.close()
      return S_ERROR( "Anomalous LFN does not start with '/lhcb' string" )

    return  S_OK( LFN )



  def urlretrieveTimeout( self, url, fileName, timeout = 0 ):
    """
     Borrowed from dirac-install (and slightly modified to fit in this agent).
     Retrieve remote url to local file (fileName), with timeout wrapper
    """
  # NOTE: Not thread-safe, since all threads will catch same alarm.
  #       This is OK for dirac-install, since there are no threads.
    gLogger.info( 'Retrieving remote file "%s"' % url )

    if timeout:
      signal.signal( signal.SIGALRM, alarmTimeoutHandler )
      # set timeout alarm
      signal.alarm( timeout )
    try:
      remoteFD = urllib2.urlopen( url )
      expectedBytes = long( remoteFD.info()[ 'Content-Length' ] )
      localFD = open( fileName, "wb" )
      receivedBytes = 0L
      data = remoteFD.read( 16384 )
      while data:
        receivedBytes += len( data )
        localFD.write( data )
        data = remoteFD.read( 16384 )
      localFD.close()
      remoteFD.close()
      if receivedBytes != expectedBytes:
        gLogger.info( "File should be %s bytes but received %s" % ( expectedBytes, receivedBytes ) )
        return False
    except urllib2.HTTPError, x:
      if x.code == 404:
        gLogger.info( "%s does not exist" % url )
        return False
    except Exception, x:
      if x == 'TimeOut':
        gLogger.info( 'Timeout after %s seconds on transfer request for "%s"' % ( str( timeout ), url ) )
      if timeout:
        signal.alarm( 0 )
      raise x

    if timeout:
      signal.alarm( 0 )
    return True



  def downloadAndExtractTarball( self, originFileName, originURL, targetPath ):
    """ Borrowed from dirac-install ( slightly modified to fit in this agent).
    It download a tar archive and extract the content, using the method urlretrieveTimeout """

    tarName = "%s" % ( originFileName )
    # destination file:
    tarPath = os.path.join( targetPath, tarName )
    try:
      if not self.urlretrieveTimeout( "%s/%s" % ( originURL, tarName ), tarPath, 300 ):
        gLogger.error( "Cannot download %s" % tarName )
        return S_ERROR( "Cannot download file" )
    except Exception, e:
      gLogger.error( "Cannot download %s: %s" % ( tarName, str( e ) ) )
      return S_ERROR( "Cannot download file" )
    # check if the file has to be uncompressed
    gLogger.info( "The downloaded file is: %s " % tarPath )
    if tarPath[-3:] not in ['bz2', 'tgz', 'tar']:
      gLogger.info( "File ready to be read (no need to uncompress) " )
      return S_OK()
    #Extract
    cwd = os.getcwd()
    os.chdir( targetPath )
    tf = tarfile.open( tarPath, "r" )
    for member in tf.getmembers():
      tf.extract( member )
    os.chdir( cwd )
    #Delete tar
    os.unlink( tarPath )
    return S_OK()

  def downloadFiles( self, originFileNames, originURL, targetPath ):
    """ Downloads a list of files from originURL locally to targetPath """
    if type( originFileNames ) != type( [] ):
      gLogge.error( "first argument for downloadFiles method should be a list! " )
      return False

    for file in originFileNames:
      destinationPath = os.path.join( targetPath, file )
      try:
        if not self.urlretrieveTimeout( "%s/%s" % ( originURL, file ), destinationPath, 300 ):
          gLogger.error( "Cannot download %s" % file )
          return False
      except Exception, e:
        gLogger.error( "Cannot download %s: %s" % ( file, str( e ) ) )
        return False
    return True


  def PathInLFC( self, dirName ):
    """ get the path as registered in the LFC. Different from the path that is used to build the pfn only for the special replicas (failover, archive, freezer)
    """
    LFCDirName = dirName
    for specialReplica in self.specialReplicas:
      prefix = '/lhcb/' + specialReplica
      if prefix in dirName:
        LFCDirName = dirName.split( prefix )[1]
        gLogger.verbose( "special replica! dirname = %s -- LFCDirName = %s" % ( dirName, LFCDirName ) )
        return LFCDirName
    return LFCDirName

  def PathWithSuffix( self, dirName, replicaType ):
    """ Takes in input the path as registered in LFC and
        returns the path with the initial suffix for the special replicas
    """
    pathWithSuffix = dirName
    if replicaType in self.specialReplicas:
      pathWithSuffix = '/lhcb/' + replicaType + dirName
    return pathWithSuffix


  def getProblematicDirsSummary( self, site ):
    """ Produce a list of files that are not registered in the File Catalog.
        1- queries the problematicDirs table to get all directories for a given site that have more data on SE than in LFC
        for each replica type: (normal, archive, failover, freezer )
        2- scan the input files (from the sites storage dumps) to get all the files belonging to the problematic directories
        3- lookup in in FC file by file to check if they have a replica registered at the site
        4- the files that are found not to have a replica registered for the site, are written down to a file
    """
    gLogger.info( "*** Execute getProblematicDirsSummary method for site: %s " % site )
    problem = 'NotRegisteredInFC'
    res = self.__storageUsage.getProblematicDirsSummary( site, problem )
    if not res['OK']:
      gLogger.error( "ERROR! %s" % res )
      return S_ERROR( res )
    val = res[ 'Value' ]
    problematicDirectories = {} # store a list of directories for each replica type
    gLogger.verbose( "List of problematic directories: " )
    for row in val:
      #('SARA', 'LHCb-Disk', 0L, 43L, '/lhcb/MC/2010/DST/00007332/0000/', 'NotRegisteredInFC','failover')
      site, spaceToken, LFCFiles, SDFiles, LFCPath, problem, replicaType = row
      pathWithSuffix = self.PathWithSuffix( LFCPath, replicaType )
      gLogger.info( "%s %s - %s" % ( LFCPath, replicaType, pathWithSuffix ) )
      if replicaType not in problematicDirectories.keys():
        problematicDirectories[ replicaType ] = []
      if pathWithSuffix not in problematicDirectories[ replicaType ]:
        problematicDirectories[ replicaType ].append( pathWithSuffix ) #fix 13.03.2012
      else:
        gLogger.error( "ERROR: the directory should be listed only once for a given site and type of replica! site=%s, path= %s, type of replica =%s  " % ( site, LFCPath, replicaType ) )
        continue
    gLogger.info( "Found the following problematic directories:" )
    for replicaType in problematicDirectories.keys():
      gLogger.info( "replica type: %s , directories: %s " % ( replicaType, problematicDirectories[replicaType] ) )
    # retrieve the list of files belonging to problematic directories from the merged files:
    filesInProblematicDirs = {}
    # read the files from the Merged files
    pathToMergedFiles = self.siteConfig[ site ][ 'pathToInputFiles' ]
    if pathToMergedFiles[:-1] != '/':
      pathToMergedFiles = pathToMergedFiles + '/'
    for mergedFile in os.listdir( pathToMergedFiles ):
      if 'Merge' not in mergedFile:
        continue
      fullFilePath = os.path.join( pathToMergedFiles, mergedFile )
      gLogger.info( "Scanning file: %s ... " % fullFilePath )
      for line in open( fullFilePath, "r" ).readlines():
        lfn, size, creationdate = line.split() # this LFN includes the initial suffix (e.g./lhcb/archive/)
        directories = lfn.split( '/' )
        basePath = ''
        for segment in directories[0:-1]:
          basePath = basePath + segment + '/' # basepath is the directory including the initial suffix
        for replicaType in problematicDirectories.keys():
          if basePath in problematicDirectories[ replicaType ]: # these paths do include initial suffix
            if replicaType not in filesInProblematicDirs.keys():
              filesInProblematicDirs[ replicaType ] = []
            if lfn not in filesInProblematicDirs[ replicaType ]: # lfn including suffix
              filesInProblematicDirs[ replicaType ].append( lfn )

    gLogger.info( "Files in problematic directories:" )
    for replicaType in filesInProblematicDirs.keys():
      gLogger.info( "replica type: %s files: %d " % ( replicaType, len( filesInProblematicDirs[replicaType] ) ) )
      for fil in filesInProblematicDirs[ replicaType ]:
        gLogger.verbose( "file in probl Dir %s %s" % ( fil, replicaType ) )

    for replicaType in filesInProblematicDirs.keys():
      res = self.checkReplicasInFC( replicaType, filesInProblematicDirs[ replicaType ] , site )
    return S_OK()

#...............................................................................................................
  def checkReplicasInFC( self, replicaType, filesToBeChecked, site ):
    """ Check the existance of the replicas for the given site and replica type in the FC
    """
    gLogger.info( "*** Execute checkReplicasInFC for replicaType=%s, site=%s " % ( replicaType, site ) )
    filesMissingFromFC = []
    replicasMissingFromSite = []
    #totalSizeMissingFromFC = 0
    #totalSizeReplicasMissingFromSite = 0
    # for files in problematic directories look up in the FC:
    specialReplicasSEs = []
    for sr in self.specialReplicas:
      se = site + '-' + sr.upper()
      specialReplicasSEs.append( se )
    gLogger.verbose( "SEs for special replicas: %s " % specialReplicasSEs )

    filesInProblematicDirs = []
    if replicaType in self.specialReplicas:
      for lfn in filesToBeChecked:
        filesInProblematicDirs.append( self.PathInLFC( lfn ) )
    else:
      filesInProblematicDirs = filesToBeChecked

    active = False # then this should be moved to a config parameter
    start = time.time()
    if not filesInProblematicDirs:
      gLogger.info( "No file to be checked in the FC for site %s " % site )
      return S_OK()
    if active:
      repsResult = self.__replicaManager.getActiveReplicas( filesInProblematicDirs )
    else:
      repsResult = self.__replicaManager.getReplicas( filesInProblematicDirs )
    timing = time.time() - start
    gLogger.info( ' %d replicas Lookup Time: %.2f s -> %.2f s/replica ' % ( len( filesInProblematicDirs ), timing, timing * 1. / len( filesInProblematicDirs ) ) )
    if not repsResult['OK']:
      return S_ERROR( repsResult['Message'] )
    goodFiles = repsResult['Value']['Successful']
    badFiles = repsResult['Value']['Failed']
    for lfn in badFiles.keys():
      if "No such file or directory" in badFiles[ lfn ]:
        gLogger.info( "missing from FC %s %s" % ( lfn, replicaType ) )
        # check if the storage files have been removed in the meanwhile after the storage dump creation and the check
        # to be done
        storageFileStatus = self.storageFileExists( lfn, replicaType, site )
        if storageFileStatus == 1:
          gLogger.info( "Inconsistent file! %s " % lfn )
          filesMissingFromFC.append( lfn )
        elif storageFileStatus == 0:
          gLogger.info( "storage file does not exist (temporary file) %s " % lfn )
        else:
          gLogger.warning( "Failed request for storage file %s " % lfn )
      else:
        gLogger.info( "Unknown message from Fc: %s - %s " % ( lfn, badFiles[ lfn ] ) )
    for lfn in goodFiles.keys():
      #check if the replica exists at the given site:
      replicaAtSite = False
      if replicaType in self.specialReplicas:
        specialReplicaSE = site + '-' + replicaType.upper()
        for se in goodFiles[lfn].keys():
          if se == specialReplicaSE:
            gLogger.verbose( "matching se: %s " % se )
            replicaAtSite = True
            break
      else:
        for se in goodFiles[lfn].keys():
          if se in specialReplicasSEs:
            gLogger.info( "Replica type is %s, skip this SE: %s " % ( replicaType, se ) )
            continue
          if site in se:
            gLogger.verbose( "matching se: %s " % se )
            replicaAtSite = True
            break
      if not replicaAtSite:
        replicasMissingFromSite.append( lfn )


    # write results of checks to files:
    fileName = os.path.join( self.__workDirectory, site + '.' + replicaType + ".replicasMissingFromSite.txt" )
    gLogger.info( "Writing list of replicas missing from site to file %s " % fileName )
    fp = open( fileName , "w" )
    for lfn in replicasMissingFromSite:
      fp.write( "%s\n" % lfn )
    fp.close()
    fileName = os.path.join( self.__workDirectory, site + '.' + replicaType + ".filesMissingFromFC.txt" )
    gLogger.info( "Writing list of files missing from FC to file %s " % fileName )
    fp = open( fileName , "w" )
    for lfn in filesMissingFromFC:
      fp.write( "%s\n" % lfn )
    fp.close()
    fileName = os.path.join( self.__workDirectory, site + '.' + replicaType + ".consistencyChecksSummary.txt" )
    gLogger.info( "Writing consistency checsk summary to file %s " % fileName )
    date = time.asctime()
    line = "Site: " + site + "  Date: " + date
    fp = open( fileName , "w" )
    fp.write( "%s\n" % line )
    totalSizeMissingFromFC = 0 # to be implemented!!!
    totalSizeReplicasMissingFromSite = 0 # to be implemented!!!
    fp.write( "Total number of LFN at site not registered in the FC: %d , total size: %.2f GB \n" % ( len( filesMissingFromFC ), totalSizeMissingFromFC / 1.0e9 ) )
    fp.write( "Total number of replicas  at site not registered in the FC: %d , total size: %.2f GB \n" % ( len( replicasMissingFromSite ), totalSizeReplicasMissingFromSite / 1.0e9 ) )
    fp.close()

    return S_OK()


  def storageFileExists( self, lfn, replicaType, site ):
    """ Check if the replica exists on storage. This is to filter many temporary files (e.g. un-merged..) that are
          removed in the while between storage dump and consistency check.
        Return values:
         -1 : request failed
          0 : storage file does not exist
          1 : storage file exists
    """
    storageFileExist = -1
    # get the PFN
    seList = []
    if replicaType in self.specialReplicas:
      seName = site + '-' + replicaType.upper()
      seList.append( seName )
    else:
      specialReplicasSEs = []
      for sr in self.specialReplicas:
        se = site + '-' + sr.upper()
        specialReplicasSEs.append( se )
      # seName is not known
      # try with all the SEs available for the site, except the ones for special replicas
      allSEs = self.siteConfig[ site ]['SEs']
      for se in allSEs:
        if se not in specialReplicasSEs:
          seList.append( se )
    gLogger.verbose( "list of SEs : %s" % seList )

    for seName in seList:
      storageElement = StorageElement( seName )
      res = storageElement.getPfnForLfn( lfn )
      if not res['OK']:
        gLogger.error( "Could not create storage element object %s " % res['Message'] )
        continue
      surl = res[ 'Value']
      gLogger.verbose( "checking existance for %s - %s" % ( surl, seName ) )
      res = self.__replicaManager.getStorageFileExists( surl, seName )
      gLogger.verbose( "result of getStorageFileExists: %s " % res )
      if not res['OK']:
        gLogger.error( "error executing replicaManager.getStorageFileExists %s " % res['Message'] )
        storageFileExist = -1
        continue
      if res['Value']['Failed']:
        gLogger.error( "error executing replicaManager.getStorageFileExists %s " % res )
        storageFileExist = -1
        continue
      elif res['Value']['Successful']:
        if res['Value']['Successful'][ surl ]:
          gLogger.info( "storage file exists: %s " % res['Value'] )
          storageFileExist = 1
          return storageFileExist
        else:
          gLogger.verbose( "storage file NOT found: %s " % res['Value'] )
          storageFileExist = 0

    return storageFileExist

  def castorPreParser( self, site, inputFilesDir ):
    """ Preliminary parsing for Castor nameserver dump
        Separates the files in 3 space tokens relying on the namespace
    """

    if inputFilesDir[-1:] != '/':
      inputFilesDir = inputFilesDir + '/'
    inputFile = os.listdir( inputFilesDir )
    if len( inputFile ) > 1:
      gLogger.error( "For this parser, there should be only one file. Found these files: %s " % inputFile )
      return S_ERROR()


    p1FilesDict = { 'LHCb_USER': {'fileName': inputFilesDir + 'LHCb_USER.txt'},
                    'LHCb-Disk': {'fileName': inputFilesDir + 'LHCb-Disk.txt'},
                    'LHCb-Tape': {'fileName': inputFilesDir + 'LHCb-Tape.txt'}
                  }
    for st in p1FilesDict.keys():
      fp = open( p1FilesDict[ st ]['fileName'], "w" )
      p1FilesDict[ st ][ 'filePointer' ] = fp

    fullPath = os.path.join( inputFilesDir, inputFile[ 0 ] )
    for line in open( fullPath, "r" ).readlines():
      splitLine = line.split()
      if len( splitLine ) < 2:
        gLogger.debug( "Empty directory. Skip line! %s " % line )
        continue
      size = splitLine[ 4 ]
      pfn = splitLine[ 10 ]
      #updated = should be reconstructed on the basis of the ascii date
      update = 'na'
      res = self.getLFNPath( site, pfn )
      if not res[ 'OK' ]:
        return S_ERROR()
        continue
      lfn = res['Value']
      probableSpaceToken = ''
      if lfn[:11] == '/lhcb/user/':
        probableSpaceToken = 'LHCb_USER'
      elif ( lfn[-3:] == 'raw' or lfn[-4:] == 'sdst' or lfn[:19] == '/lhcb/archive/lhcb/' ) and ( lfn[:20] != '/lhcb/failover/lhcb/' ):
        probableSpaceToken = 'LHCb-Tape'
      else:
        probableSpaceToken = 'LHCb-Disk'
      newLine = pfn + ' |  ' + size + ' |  ' + update
      fp = p1FilesDict[ probableSpaceToken ][ 'filePointer']
      fp.write( "%s\n" % newLine )

    for st in p1FilesDict.keys():
      fp = p1FilesDict[ st ][ 'filePointer']
      fp.close()
    return S_OK()


  def checkCreationDate( self, directory ):
    """ Check the storage dump creation date . Returns 0 if the creation date is more recent than a given time interval (set as
       configuration parameter), otherwise returns -1 """
    retCode = 0
    for file in os.listdir( directory ):
      fullPath = os.path.join( directory , file )
      ( mode, ino, dev, nlink, uid, gid, size, atime, mtime, ctime ) = os.stat( fullPath )
      now = time.time()
      elapsedTime = now - mtime
      gLogger.info( "Creation time: %s,  elapsed time: %d h (maximum delay allowed : %d h ) " % ( time.ctime( mtime ), elapsedTime / 3600, self.maximumDelaySinceSD / 3600 ) )

      if elapsedTime > self.maximumDelaySinceSD:
        gLogger.warn( "WARNING: storage dump creation date is older than maximum limit! %s s ( %d h )  - %s " % ( self.maximumDelaySinceSD, self.maximumDelaySinceSD / 3600, fullPath ) )
        retCode = -1
        return S_OK( retCode )
    return S_OK( retCode )
