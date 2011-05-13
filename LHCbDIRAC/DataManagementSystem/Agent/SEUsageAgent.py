"""  SEUsageAgent browses the SEs to determine their content and store it into a DB.
"""
__RCSID__ = "$Id:  $"

from DIRAC  import gLogger, gMonitor, S_OK, S_ERROR, rootPath, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

from DIRAC.DataManagementSystem.Agent.NamespaceBrowser import NamespaceBrowser
from DIRAC.DataManagementSystem.Client.ReplicaManager import CatalogDirectory
from DIRAC.Core.Utilities.List import sortList

import time, os, urllib2, tarfile, signal
from types import *

def alarmTimeoutHandler( *args ):
  raise Exception( 'Timeout' )

AGENT_NAME = 'DataManagement/SEUsageAgent'
#InputFilesLocation = '/home/dirac/SEUsageAgentInputFiles/'

class SEUsageAgent( AgentModule ):
  def initialize( self ):

    if self.am_getOption( 'DirectDB', False ):
      from LHCbDIRAC.DataManagementSystem.DB.StorageUsageDB import StorageUsageDB
      self.__storageUsage = StorageUsageDB()
    else:
      from DIRAC.Core.DISET.RPCClient import RPCClient
      self.__storageUsage = RPCClient( 'DataManagement/StorageUsage' )
    self.am_setModuleParam( "shifterProxy", "DataManager" )
    self.am_setModuleParam( "shifterProxyLocation", "%s/runit/%s/proxy" % ( rootPath, AGENT_NAME ) )
    InputFilesLocation = self.am_getOption( 'InputFilesLocation' )
    # check that this directory exists:
    if not os.path.isdir( InputFilesLocation ):
      gLogger.info( "Create the directory to download input files: %s" % InputFilesLocation )
      os.makedirs( InputFilesLocation )
    if os.path.isdir( InputFilesLocation ):
      gLogger.info( "Input files will be downloaded to: %s" % InputFilesLocation )
    else:
      gLogger.info( "ERROR! could not create directory %s" % InputFilesLocation )
      return S_ERROR()

    self.siteConfig = {}
    self.siteConfig['SARA'] = { 'originFileName': "LHCb.tar.bz2",
                                'originURL': "http://web.grid.sara.nl/space_tokens", # without final slash
                                'targetPath': InputFilesLocation + 'downloadedFiles/SARA/',
                                'pathToInputFiles': InputFilesLocation + 'goodFormat/SARA/',
                                'alternativeStorageName': 'NIKHEF' }


    return S_OK()

  def execute( self ):
    """ Loops on the input files to read the content of Storage Elements, process them, and store the result into the DB.
    It reads directory by directory (every row of the input file being a directory).
    If the directory exists in the StorageUsage su_Directory table, and if a replica also exists for the given SE in the su_SEUsage table, then the directory and
    its usage are stored in the replica table (the se_Usage table) together with the insertion time, otherwise it is added to the dark data table (se_DarkDirectories) """
    gLogger.info( "SEUsageAgent: start the execute method\n" )

    siteList = self.siteConfig.keys()
    siteList.sort()
    # Read the input files:
    for site in siteList:
      gLogger.info( "Reading input files for site: %s" % site )
      res = self.readInputFile( site )
      if not res:
        gLogger.debug( "SEUsageAgent: successfully read input file" )
      else:
        gLogger.error( "SEUsageAgent: failed to read input file" )
        return S_ERROR( "Failed to read input files" )

      # Start the main loop:
      # read all file of type directory summary for the given site:
      pathToSummary = self.siteConfig[ site ][ 'pathToInputFiles' ]
      for fileP3 in os.listdir( pathToSummary ):
        fullPathFileP3 = pathToSummary + fileP3
        if 'dirSummary' not in fileP3:
          continue
        for line in open( fullPathFileP3, "r" ).readlines():
          splitLine = line.split()
          try:
            SEName = splitLine[ 0 ]
            dirPath = splitLine[ 1 ]
            files = splitLine[ 2 ]
            size = splitLine[ 3 ]
          except:
            gLogger.error( "ERROR in input data format. Line is: %s" % line )
            continue
          #oneDirDict = {dirPath: self.dirDict[ t ]} # dict where the key is simply the LFN path.
          oneDirDict = {}
          oneDirDict[ dirPath ] = { 'SEName': SEName, 'Files': files, 'Size': size , 'Updated': 'na' }

          # the format of the entry to be processed must be:
          # oneDirDict = {LFNPath: {'SEName: se, 'Files':files, 'Size':size, 'Updated',updated }}
          # use this format for consistency with already existing methods of StorageUsageDB which take in input a dictionary like this
          gLogger.info( "SEUsageAgent: processing directory: %s" % ( oneDirDict ) )
          # initialize the isRegistered flag as False. Change it to true if a replica is found in the same SE.
          isRegistered = False
          gLogger.info( "SEUsageAgent: check if dirName exists in su_Directory: %s" % dirPath )
          res = self.__storageUsage.getIDs( oneDirDict )
          if not res['OK']:
            gLogger.info( "SEUsageAgent: ERROR failed to get DID from StorageUsageDB.su_Directory table for dir: %s " % dirPath )
            continue
          elif not res['Value']:
            gLogger.info( "SEUsageAgent: NO LFN registered in the LFC for the given path %s => insert the entry in the se_DarkDirectory table and delete this entry from the replicas table" % dirPath )
            isRegistered = False
          else:
            value = res['Value']
            gLogger.info( "SEUsageAgent: directory LFN  is registered in the LFC, output of getIDs is %s" % value )
            for dir in value:
              gLogger.debug( "SEUsageAgent: Path: %s su_Directory.DID %d" % ( dir, value[dir] ) )
            gLogger.debug( "SEUsageAgent: check if this particular replica is registered in the LFC." )
            res = self.__storageUsage.getAllReplicasInFC( dir )
            if not res['OK']:
              gLogger.info( "SEUsageAgent: ERROR failed to get replicas for %s directory " % dir )
              isRegistered = False
              continue
            elif not res['Value']:
              gLogger.info( "SEUsageAgent: NO replica found for %s on any SE! Anomalous case: the LFN is registered in the FC but with NO replica! For the time being, insert it intose_DarkDirectories table " % dir )
              # we should decide what to do in this case. This might happen, but it is a problem at FC level... TBD!
              isRegistered = False
            else: # got some replicas! let's see if there is one for this SE
              for lfn in res['Value'].keys():
                for se in res['Value'][lfn].keys():
                  if SEName in se:
                    gLogger.info( "SEUsageAgent: the replica at SE= %s is registered in the FC!" % SEName )
                    isRegistered = True
                    break

          gLogger.info( "SEUsageAgent: isRegistered flag is %s" % isRegistered )
        # now insert the entry into the dark data table, or the replicas table according the isRegistered flag.
          if not isRegistered:
            gLogger.info( "SEUsageAgent: dark data! Add the entry %s to se_DarkDirectories table. Before (if necessary) remove it from se_Usage table" % ( dirPath ) )
            res = self.__storageUsage.removeDirFromSe_Usage( oneDirDict )
            if not res[ 'OK' ]:
              gLogger.error( "SEUsageAgent: ERROR failed to remove from se_Usage table: %s" % oneDirDict )
              continue
            else:
              removedDirs = res[ 'Value' ]
              if removedDirs:
                gLogger.info( "SEUsageAgent: entry %s correctly removed from se_Usage table" % oneDirDict )
              else:
                gLogger.info( "SEUsageAgent: entry %s was NOT in se_Usage table" % oneDirDict )
              res = self.__storageUsage.publishToDarkDir( oneDirDict )
              if not res['OK']:
                gLogger.error( "SEUsageAgent: failed to publish entry %s to se_DarkDirectories table" % dirPath )
              else:
                gLogger.info( "SEUsageAgent: entry %s correctly published to Dark Directories table" % oneDirDict )
          else: # publish to se_Usage table!
            # before, remove it from dark data dir!!!
            gLogger.info( "SEUsageAgent: replica %s is registered! remove from se_DarkDirectories (if necessary) and  publish to se_Usage table" % dirPath )
            res = self.__storageUsage.removeDirFromSe_DarkDir( oneDirDict )
            if not res[ 'OK' ]:
              gLogger.error( "SEUsageAgent: ERROR failed to remove from se_DarkDirectories: %s" % oneDirDict )
              continue
            else:
              removedDirs = res[ 'Value' ]
              if removedDirs:
                gLogger.info( "SEUsageAgent: entry %s correctly removed from se_DarkDirectories" % oneDirDict )
              else:
                gLogger.info( "SEUsageAgent: entry %s was NOT in se_DarkDirectories" % oneDirDict )
              res = self.__storageUsage.publishToSEReplicas( oneDirDict )
              if not res['OK']:
                gLogger.info( "SEUsageAgent: failed to publish %s entry to se_Usage table" % oneDirDict )
              else:
                gLogger.info( "SEUsageAgent: entry %s correctly published to se_Usage" % oneDirDict )
      return S_OK()



  def readInputFile( self, site ):
    """ Download, read and parse input files with SEs content.
        Write down the results to  ASCII files.
        There are 2 phases in the manipulation of input files:
        phase 1- it is directly the format of the DB query output, right after uncompressing the tar file provided by the site:
        PFN | size | update (ms)
        phase 2- one row per file, with format: SE PFN size update
        phase 3- directory summary files: one row per directory, with format:
        SE DirectoryLFN NumOfFiles TotalSize Update(actually, not used)

         """

    originFileName = self.siteConfig[ site ][ 'originFileName' ]
    originURL = self.siteConfig[ site ][ 'originURL' ]
    targetPath = self.siteConfig[ site ][ 'targetPath' ]
    if not os.path.isdir( targetPath ):
      os.makedirs( targetPath )
    pathToInputFiles = self.siteConfig[ site ][ 'pathToInputFiles' ]
    if not os.path.isdir( pathToInputFiles ):
      os.makedirs( pathToInputFiles )
    alternativeStorageName = self.siteConfig[ site ][ 'alternativeStorageName' ] # i.e. for SARA it's NIKHEF
    if pathToInputFiles[-1] != "/":
      pathToInputFiles = "%s/" % pathToInputFiles
    gLogger.info( "Reading input files for site %s " % site )
    gLogger.info( "originFileName: %s , originURL: %s ,targetPath: %s , pathToInputFiles: %s " % ( originFileName, originURL, targetPath, pathToInputFiles ) )

    # Download input data made available by the sites. Reuse the code of dirac-install: urlretrieveTimeout, downloadAndExtractTarball
    res = self.downloadAndExtractTarball( originFileName, originURL, targetPath )
    if not res:
       return S_ERROR( "ERROR: Could not download input files" )

    defaultDate = 'na'
    defaultSize = 0

    if targetPath[-1] != "/":
      targetPath = "%s/" % targetPath
    InputFilesListP1 = os.listdir( targetPath + 'LHCb/' )
    gLogger.debug( "List of raw input files %s " % InputFilesListP1 )
    suffix = {'LHCb_FAILOVER': '-FAILOVER', 'LHCb_USER':'-USER', 'LHCb_MC_M-DST': '_MC_M-DST', 'LHCb_MC_DST': '_MC-DST', 'LHCb_M-DST':'_M-DST', 'LHCb_RDST':'-RDST', 'LHCb_RAW':'-RAW', 'LHCb_DST':'-DST', 'LHCb_HISTOS':'-HIST'}

    # Loop on N files relative to the site (one file for each space token)
    for inputFileP1 in InputFilesListP1:
      if 'USER' in inputFileP1:
        #### THIS IS ONLY A TEMPORARY TRICK TO SPEED UP DEBUGGING!!!!!
        gLogger.info( "WARNING!!!!! Skip users files for the time being (until in development)" )
        continue
      self.dirDict = {}

      # the expected input file line is: pfn | size | date
      #/pnfs/grid.sara.nl/data/lhcb/data/2010/CHARMCONTROL.DST/00009283/0000/00009283_00000384_1.charmcontrol.dst          |   831797381 | 1296022620555
      # manipulate the input file to create a directory summary file (one row per directory)
      for siteNaming in suffix.keys():
        if siteNaming in inputFileP1:
          SE = alternativeStorageName + suffix[siteNaming] # i.e. transforms LHCb_USER into NIKHEF-USER
          gLogger.info( "SE name in DIRAC terminology: %s " % SE )
          break
      fileP2 = pathToInputFiles + SE + '.txt'
      gLogger.debug( "Opening file %s in w mode" % fileP2 )
      fP2 = open( fileP2, "w" )
      fullPathFileP1 = targetPath + 'LHCb/' + inputFileP1
      gLogger.debug( "Reading from file %s" % fullPathFileP1 )
      totalLines = 0 # counts all lines in input
      processedLines = 0 # counts all processed lines
      for line in open( fullPathFileP1, "r" ).readlines():
        totalLines += 1
        try:
          splitLine = line.split( '|' )
          filePath = splitLine[0].rstrip()
          size = splitLine[1].lstrip()
          updated = splitLine[2].lstrip()
          newLine = SE + ' ' + filePath + ' ' + size + ' ' + updated
          fP2.write( "%s" % newLine )
          processedLines += 1
        except:
          gLogger.info( "Error in input line format! Line is: %s" % line ) # the last line of these files is empty, so it will give this exception
          continue
      fP2.close()
      gLogger.info( "File %s: total lines: %d correctly processed: %d " % ( fullPathFileP1, totalLines, processedLines ) )

      gLogger.debug( "Reading from fileP2 %s " % fileP2 )
      totalLines = 0 # counts all lines in input
      processedLines = 0 # counts all processed lines
      for line in open( fileP2, "r" ).readlines():
        totalLines += 1
        splitLine = line.split()
        SEName = splitLine[0]
        PFNfilePath = splitLine[1]
        res = self.getLFNPath( SEName, PFNfilePath )
        if not res[ 'OK' ]:
          gLogger.info( "ERROR: could not retrieve LFN for PFN %s" % PFNfilePath )
          continue
        filePath = res[ 'Value' ]
        #gLogger.debug("filePath: %s" %filePath)
        if not filePath:
          gLogger.info( "SEUsageAgent: it was not possible to get the LFN for PFN=%s, skip this line" % PFNfilePath )
          continue
        updated = defaultDate
        size = int( splitLine[2] )
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

      gLogger.info( "File %s: total lines: %d correctly processed: %d " % ( fileP2, totalLines, processedLines ) )

      # create the directory summary file
      fileP3 = pathToInputFiles + SE + '.dirSummary.txt'
      gLogger.debug( "Opening fileP3 %s in w mode" % fileP3 )
      fP3 = open( fileP3, "w" )
      for basePath in self.dirDict.keys():
        summaryLine = SEName + ' ' + basePath + ' ' + str( self.dirDict[ basePath ][ 'Files' ] ) + ' ' + str( self.dirDict[ basePath ][ 'Size' ] )
        gLogger.debug( "Writing summaryLine %s" % summaryLine )
        fP3.write( "%s\n" % summaryLine )
      fP3.close()

      gLogger.debug( "SEUsageAgent: ----------------summary of ReadInputFile-------------------------" )
      for k in self.dirDict.keys():
        gLogger.debug( "SEUsageAgent: (lfn,SE): %s-%s files=%d size=%d updated=%s" % ( k, SEName , self.dirDict[ k ][ 'Files' ], self.dirDict[ k ][ 'Size' ], self.dirDict[ k ][ 'Updated' ] ) )

    return 0


  def getLFNPath( self, ST, PFNfilePath ):
    """ Given a PFN returns the LFN, stripping the suffix relative to the particular site.
        Important: usually the transformation is done simply removing the SApath of the site, except for ARCHIVED and FREEZED
        datasets: in this case the PFN is: <old SAPath>/lhcb/archive/<LFN> and <old SAPath>/lhcb/freezer/<LFN>
        so, in order to get the lfn also the 'lhcb/archive/' or '/lhcb/freezer' string should be removed
          """
    #gLogger.info( "Get the path from the CS:" )
    res = gConfig.getOption( '/Resources/StorageElements/%s/AccessProtocol.1/Path' % ( ST ) )
    if not res[ 'OK' ]:
      return S_ERROR( "Could not retrieve path from CS" )
    SEPath = res[ 'Value' ]
    prefixes = ['/lhcb/archive', '/lhcb/freezer']
    LFN = 'None'
    try:
      LFN = PFNfilePath.split( SEPath )[1]
      for p in prefixes:
        if p in LFN:
          gLogger.info( "Remove the initial prefix %s from LFN: %s" % ( p, LFN ) )
          length = len( p )
          LFN = LFN[ length: ]
    except:
      gLogger.error( "ERROR retrieving LFN from PFN %s" % PFNfilePath )
      return S_ERROR( "Could not retrieve LFN" )
    # additional check on the LFN format:
    if not LFN.startswith( '/lhcb' ):
      gLogger.info( "SEUsageAgent: ERROR! LFN should start with /lhcb: PFN=%s LFN=%s. Skip this file." % ( PFNfilePath, LFN ) )
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



#  def downloadAndExtractTarball( pkgVer, targetPath, subDir = False, checkHash = True ):
  def downloadAndExtractTarball( self, originFileName, originURL, targetPath ):
    """ Borrowed from dirac-install ( slightly modified to fit in this agent).
    It download a tar archive and extract the content, using the method urlretrieveTimeout """

    #tarName = "%s.tar.gz" % ( originFileName )
    tarName = "%s" % ( originFileName )
    # destination file:
    tarPath = os.path.join( targetPath, tarName )
    try:
      #if not urlretrieveTimeout( "%s/%s/%s" % ( cliParams.downBaseURL, subDir, tarName ), tarPath, 300 ):
      if not self.urlretrieveTimeout( "%s/%s" % ( originURL, tarName ), tarPath, 300 ):
        gLogger.info( "Cannot download %s" % tarName )
        return False
    except Exception, e:
      gLogger.info( "Cannot download %s: %s" % ( tarName, str( e ) ) )
      return False
      #sys.exit( 1 )

    #Extract
    cwd = os.getcwd()
    os.chdir( targetPath )
    tf = tarfile.open( tarPath, "r" )
    for member in tf.getmembers():
      tf.extract( member )
    os.chdir( cwd )
    #tarCmd = "tar xzf '%s' -C '%s'" % ( tarPath, cliParams.targetPath )
    #os.system( tarCmd )
    #Delete tar
    os.unlink( tarPath )
    return True

