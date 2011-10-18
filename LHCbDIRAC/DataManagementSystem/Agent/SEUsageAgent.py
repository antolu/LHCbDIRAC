"""  SEUsageAgent browses the SEs to determine their content and store it into a DB.
"""
__RCSID__ = "$Id$"

from DIRAC  import gLogger, gMonitor, S_OK, S_ERROR, rootPath, gConfig
from DIRAC.Core.Base.AgentModule import AgentModule

from DIRAC.Core.Utilities.Shifter import setupShifterProxyInEnv

from DIRAC.DataManagementSystem.Agent.NamespaceBrowser import NamespaceBrowser
from DIRAC.Core.Utilities.SiteSEMapping                import getSEsForSite
from DIRAC.DataManagementSystem.Client.ReplicaManager import CatalogDirectory
from DIRAC.Core.Utilities.List import sortList

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
    self.am_setModuleParam( "shifterProxy", "DataManager" )
    self.am_setModuleParam( "shifterProxyLocation", "%s/runit/%s/proxy" % ( rootPath, AGENT_NAME ) )
    InputFilesLocation = self.am_getOption( 'InputFilesLocation' )
    self.activeSites = self.am_getOption( 'ActiveSites' )
    # maximum delay after storage dump creation (to be implemented) 
    self.maximumDelaySinceSD = 43200  
    # check that this directory exists:
    if not os.path.isdir( InputFilesLocation ):
      gLogger.info( "Create the directory to download input files: %s" % InputFilesLocation )
      os.makedirs( InputFilesLocation )
    if os.path.isdir( InputFilesLocation ):
      gLogger.info( "Input files will be downloaded to: %s" % InputFilesLocation )
    else:
      gLogger.info( "ERROR! could not create directory %s" % InputFilesLocation )
      return S_ERROR()

    spaceTokToIgnore = self.am_getOption( 'SpaceTokenToIgnore' )# STs to skip during checks
    self.siteConfig = {}
    self.spaceTokens = {}

    site = 'SARA'
    self.spaceTokens[ site ] = { 'LHCb-Tape' : {'year' : '2011', 'DiracSEs':['SARA-RAW', 'SARA-RDST', 'SARA-ARCHIVE']},
                                 'LHCb-Disk' : {'year': '2011', 'DiracSEs':['SARA-DST', 'SARA_M-DST', 'SARA_MC_M-DST', 'SARA_MC-DST', 'SARA-FAILOVER']},
                                 'LHCb_USER' : {'year': '2011', 'DiracSEs':['SARA-USER']},
                                 'LHCb_RAW' : {'year': '2010', 'DiracSEs':['SARA-RAW']},
                                 'LHCb_RDST' : {'year': '2010', 'DiracSEs':['SARA-RDST']},
                                 'LHCb_M-DST': {'year': '2010', 'DiracSEs':['SARA_M-DST']},
                                 'LHCb_DST'  : {'year': '2010', 'DiracSEs':['SARA-DST']},
                                 'LHCb_MC_M-DST': {'year': '2010', 'DiracSEs':['SARA_MC_M-DST']},
                                 'LHCb_MC_DST'  : {'year': '2010', 'DiracSEs': ['SARA_MC-DST']},
                                 'LHCb_FAILOVER' : {'year': '2010', 'DiracSEs' : ['SARA-FAILOVER']}
                                 }

    self.siteConfig[ site ] = { 'originFileName': "LHCb.tar.bz2",
                                'originURL': "http://web.grid.sara.nl/space_tokens", # without final slash
                                'pathInsideTar': "LHCb/",
                                'targetPath': InputFilesLocation + 'downloadedFiles/SARA/',
                                'pathToInputFiles': InputFilesLocation + 'goodFormat/SARA/',
                                'StorageName': 'SARA' ,
                                'DiracName': 'LCG.SARA.nl',
                                'FileNameType': 'PFN' 
                               }
    res = getSEsForSite( site )
    if not res[ 'OK' ]:
      gLogger.error( 'could not get SEs for site %s ' % site )
      return S_ERROR()
    SEs = res['Value']
    gLogger.info( "SEs: %s" % SEs )

    res = gConfig.getOption( '/Resources/StorageElements/%s/AccessProtocol.1/Path' % ( 'SARA-RAW' ) )
    if not res[ 'OK' ]:
      gLogger.error( 'could not get configuration for SE SARA-RAW' )
    self.siteConfig[ site ][ 'SEs'] = SEs
    self.siteConfig[ site ][ 'SAPath'] = res['Value']
    gLogger.info( "Configuration for site: %s : %s " % ( site, self.siteConfig[ site ] ) )
    #......................................
    site = 'CERN'
    self.spaceTokens[ site ] = { 'LHCb-Tape' : {'year' : '2011', 'DiracSEs':['CERN-RAW', 'CERN-RDST', 'CERN-ARCHIVE'], 'ServiceClass': 'lhcbtape'},
                                 'LHCb-Disk' : {'year': '2011', 'DiracSEs':['CERN-DST', 'CERN_M-DST', 'CERN_MC_M-DST', 'CERN_MC-DST', 'CERN-FAILOVER', 'CERN-FREEZER', 'CERN-HIST'], 'ServiceClass': 'lhcbdisk'},
                                 'LHCb_USER' : {'year': '2011', 'DiracSEs':['CERN-USER'], 'ServiceClass':'lhcbuser'},
                                }

    self.siteConfig[ site ] = {'originFileName': ['lhcb.lhcbtape.last', 'lhcb.lhcbdisk.last', 'lhcb.lhcbuser.last'],
                               'originURL': 'http://castorold.web.cern.ch/castorold/DiskPoolDump/',
                               'targetPath': InputFilesLocation + 'downloadedFiles/CERN/',
                               'pathToInputFiles': InputFilesLocation + 'goodFormat/CERN/',
                               'StorageName' : 'CERN',
                               'DiracName': 'LCG.CERN.ch',
                                'FileNameType': 'PFN' 
                               }
    res = gConfig.getOption( '/Resources/StorageElements/%s/AccessProtocol.1/Path' % ( 'CERN-RAW' ) )
    if not res[ 'OK' ]:
      gLogger.error( 'could not get configuration for SE CERN-RAW' )
    self.siteConfig[ site ][ 'SAPath'] = res['Value']
    gLogger.info( "Configuration for site: %s : %s " % ( site, self.siteConfig[ site ] ) )

    
    #......................................
    site = 'CNAF'
    self.spaceTokens[ site ] = { 'LHCb-Tape' : {'year' : '2011', 'DiracSEs':['CNAF-RAW', 'CNAF-RDST', 'CNAF-ARCHIVE']},
                                 'LHCb-Disk' : {'year': '2011', 'DiracSEs':['CNAF-DST', 'CNAF_M-DST', 'CNAF_MC_M-DST', 'CNAF_MC-DST', 'CNAF-FAILOVER']},
                                 'LHCb_USER' : {'year': '2011', 'DiracSEs':['CNAF-USER']},
                                }

    self.siteConfig[ site ] = {'originFileName': "LHCb.tar.bz2",
                               'originURL': 'http://lemon.cr.cnaf.infn.it/VO/',
                               'pathInsideTar': "LHCb/",
                               'targetPath': InputFilesLocation + 'downloadedFiles/' + site + '/',
                               'pathToInputFiles': InputFilesLocation + 'goodFormat/' + site + '/',
                               'StorageName' : site,
                               'DiracName': 'LCG.CNAF.it',
                                'FileNameType': 'LFN' 
                               }
    res = gConfig.getOption( '/Resources/StorageElements/%s/AccessProtocol.1/Path' % ( 'CNAF-RAW' ) )
    if not res[ 'OK' ]:
      gLogger.error( 'could not get configuration for SE CERN-RAW' )
    self.siteConfig[ site ][ 'SAPath'] = res['Value']
    gLogger.info( "Configuration for site: %s : %s " % ( site, self.siteConfig[ site ] ) )
    #......................................
    site = 'GRIDKA'
    self.spaceTokens[ site ] = { 'LHCb-Tape' : {'year' : '2011', 'DiracSEs':['GRIDKA-RAW', 'GRIDKA-RDST', 'GRIDKA-ARCHIVE']},
                                 'LHCb-Disk' : {'year': '2011', 'DiracSEs':['GRIDKA-DST', 'GRIDKA_M-DST', 'GRIDKA_MC_M-DST', 'GRIDKA_MC-DST', 'GRIDKA-FAILOVER']},
                                 'LHCb_USER' : {'year': '2011', 'DiracSEs':['GRIDKA-USER']},
                                 'LHCb_RAW' : {'year': '2010', 'DiracSEs':['GRIDKA-RAW']},
                                 'LHCb_RDST' : {'year': '2010', 'DiracSEs':['GRIDKA-RDST']},
                                 'LHCb_M-DST': {'year': '2010', 'DiracSEs':['GRIDKA_M-DST']},
                                 'LHCb_DST'  : {'year': '2010', 'DiracSEs':['GRIDKA-DST']},
                                 'LHCb_MC_M-DST': {'year': '2010', 'DiracSEs':['GRIDKA_MC_M-DST']},
                                 'LHCb_MC_DST'  : {'year': '2010', 'DiracSEs': ['GRIDKA_MC-DST']},
                                 'LHCb_FAILOVER' : {'year': '2010', 'DiracSEs' : ['GRIDKA-FAILOVER']}
                                 }

    self.siteConfig[ site ] = { 'originFileName': "LHCb.tar.bz2",
                                'originURL': "http://lhcb-kit.gridka.de:60080/lhcb", # without final slash
                                'pathInsideTar': "LHCb/",
                                'targetPath': InputFilesLocation + 'downloadedFiles/GRIDKA/',
                                'pathToInputFiles': InputFilesLocation + 'goodFormat/GRIDKA/',
                                'StorageName': 'GRIDKA' ,
                                'DiracName': 'LCG.GRIDKA.de',
                                'FileNameType': 'PFN' 
                               }
    res = getSEsForSite( site )
    if not res[ 'OK' ]:
      gLogger.error( 'could not get SEs for site %s ' % site )
      return S_ERROR()
    SEs = res['Value']
    gLogger.info( "SEs: %s" % SEs )
    res = gConfig.getOption( '/Resources/StorageElements/%s/AccessProtocol.1/Path' % ( 'GRIDKA-RAW' ) )
    if not res[ 'OK' ]:
      gLogger.error( 'could not get configuration for SE GRIDKA-RAW' )
    self.siteConfig[ site ][ 'SEs'] = SEs
    self.siteConfig[ site ][ 'SAPath'] = res['Value']
    gLogger.info( "Configuration for site: %s : %s " % ( site, self.siteConfig[ site ] ) )
    #......................................
    site = 'PIC'
    self.spaceTokens[ site ] = { 'LHCb-Tape' : {'year' : '2011', 'DiracSEs':['PIC-RAW', 'PIC-RDST', 'PIC-ARCHIVE']},
                                 'LHCb-Disk' : {'year': '2011', 'DiracSEs':['PIC-DST', 'PIC_M-DST', 'PIC_MC_M-DST', 'PIC_MC-DST', 'PIC-FAILOVER']},
                                 'LHCb_USER' : {'year': '2011', 'DiracSEs':['PIC-USER']},
                                 'LHCb_RAW' : {'year': '2010', 'DiracSEs':['PIC-RAW']},
                                 'LHCb_RDST' : {'year': '2010', 'DiracSEs':['PIC-RDST']},
                                 'LHCb_M-DST': {'year': '2010', 'DiracSEs':['PIC_M-DST']},
                                 'LHCb_DST'  : {'year': '2010', 'DiracSEs':['PIC-DST']},
                                 'LHCb_MC_M-DST': {'year': '2010', 'DiracSEs':['PIC_MC_M-DST']},
                                 'LHCb_MC_DST'  : {'year': '2010', 'DiracSEs': ['PIC_MC-DST']},
                                 'LHCb_FAILOVER' : {'year': '2010', 'DiracSEs' : ['PIC-FAILOVER']}
                                 }

    self.siteConfig[ site ] = { 'originFileName': "LHCb.tar.bz2",
                                'originURL': "http://lhcweb.pic.es", # without final slash
                                'pathInsideTar': "LHCb/",
                                'targetPath': InputFilesLocation + 'downloadedFiles/PIC/',
                                'pathToInputFiles': InputFilesLocation + 'goodFormat/PIC/',
                                'StorageName': 'PIC' ,
                                'DiracName': 'LCG.PIC.es',
                                'FileNameType': 'PFN' 
                               }
    res = getSEsForSite( site )
    if not res[ 'OK' ]:
      gLogger.error( 'could not get SEs for site %s ' % site )
      return S_ERROR()
    SEs = res['Value']
    gLogger.info( "SEs: %s" % SEs )
    res = gConfig.getOption( '/Resources/StorageElements/%s/AccessProtocol.1/Path' % ( 'PIC-RAW' ) )
    if not res[ 'OK' ]:
      gLogger.error( 'could not get configuration for SE PIC-RAW' )
    self.siteConfig[ site ][ 'SEs'] = SEs
    self.siteConfig[ site ][ 'SAPath'] = res['Value']
    gLogger.info( "Configuration for site: %s : %s " % ( site, self.siteConfig[ site ] ) )
    #......................................
    site = 'IN2P3'
    self.spaceTokens[ site ] = { 'LHCb-Tape' : {'year' : '2011', 'DiracSEs':['IN2P3-RAW', 'IN2P3-RDST', 'IN2P3-ARCHIVE']},
                                 'LHCb-Disk' : {'year': '2011', 'DiracSEs':['IN2P3-DST', 'IN2P3_M-DST', 'IN2P3_MC_M-DST', 'IN2P3_MC-DST', 'IN2P3-FAILOVER']},
                                 'LHCb_USER' : {'year': '2011', 'DiracSEs':['IN2P3-USER']},
                                 'LHCb_RAW' : {'year': '2010', 'DiracSEs':['IN2P3-RAW']},
                                 'LHCb_RDST' : {'year': '2010', 'DiracSEs':['IN2P3-RDST']},
                                 'LHCb_M-DST': {'year': '2010', 'DiracSEs':['IN2P3_M-DST']},
                                 'LHCb_DST'  : {'year': '2010', 'DiracSEs':['IN2P3-DST']},
                                 'LHCb_MC_M-DST': {'year': '2010', 'DiracSEs':['IN2P3_MC_M-DST']},
                                 'LHCb_MC_DST'  : {'year': '2010', 'DiracSEs': ['IN2P3_MC-DST']},
                                 'LHCb_FAILOVER' : {'year': '2010', 'DiracSEs' : ['IN2P3-FAILOVER']}
                                 }

    self.siteConfig[ site ] = { 'originFileName': "LHCb.tar.bz2",
                                'pathInsideTar': "tmp/filelisting_697_LHCb/",
                                'originURL': " https://cctools2.in2p3.fr/stockage/dcache", # without final slash
                                'targetPath': InputFilesLocation + 'downloadedFiles/IN2P3/',
                                'pathToInputFiles': InputFilesLocation + 'goodFormat/IN2P3/',
                                'StorageName': 'IN2P3' ,
                                'DiracName': 'LCG.IN2P3.fr',
                                'FileNameType': 'PFN' 
                               }
    res = getSEsForSite( site )
    if not res[ 'OK' ]:
      gLogger.error( 'could not get SEs for site %s ' % site )
      return S_ERROR()
    SEs = res['Value']
    gLogger.info( "SEs: %s" % SEs )
    res = gConfig.getOption( '/Resources/StorageElements/%s/AccessProtocol.1/Path' % ( 'IN2P3-RAW' ) )
    if not res[ 'OK' ]:
      gLogger.error( 'could not get configuration for SE IN2P3-RAW' )
    self.siteConfig[ site ][ 'SEs'] = SEs
    self.siteConfig[ site ][ 'SAPath'] = res['Value']
    gLogger.info( "Configuration for site: %s : %s " % ( site, self.siteConfig[ site ] ) )


    #......................................
    for site in self.siteConfig.keys():
      for st in self.spaceTokens[ site ].keys():
        if st in spaceTokToIgnore:
          self.spaceTokens[ site ][ st ]['Check'] = False
        else:
          self.spaceTokens[ site ][ st ]['Check'] = True
    #......................................

    return S_OK()

  def execute( self ):
    """ Loops on the input files to read the content of Storage Elements, process them, and store the result into the DB.
    It reads directory by directory (every row of the input file being a directory).
    If the directory exists in the StorageUsage su_Directory table, and if a replica also exists for the given SE in the su_SEUsage table, then the directory and
    its usage are stored in the replica table (the se_Usage table) together with the insertion time, otherwise it is added to the problematic data table (problematicDirs) """
    gLogger.info( "SEUsageAgent: start the execute method\n" )
    gLogger.info( "Sites active for checks: %s " % self.activeSites )

    specialReplicas = ['archive', 'freezer', 'failover']
    siteList = self.siteConfig.keys()
    siteList.sort()
    # Read the input files:
    for site in siteList:
      if self.siteConfig[ site ][ 'DiracName' ] not in self.activeSites:
        gLogger.info( "Skip site %s as it is not included in the configuration" % site )
        continue
      gLogger.info( "Reading input files for site: %s" % site )
      if site == 'CERN':
        res = self.readInputFileCERN( site )
      else:
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
          specialRep = ''
          for sr in specialReplicas:
            prefix = '/lhcb/' + sr
            if prefix in dirPath:
              dirPath = StorageDirPath.split( prefix )[1] # strip the initial prefix, to get the LFN as registered in the LFC
              specialRep = sr
              gLogger.info( "prefix: %s \n StorageDirPath: %s\n dirPath: %s" % ( prefix, StorageDirPath, dirPath ) )
          oneDirDict[ dirPath ] = { 'SpaceToken': spaceToken, 'Files': files, 'Size': size , 'Updated': 'na', 'Site': site, 'SpecialReplica': specialRep }
          # the format of the entry to be processed must be a dictionary with LFN path as key
          # use this format for consistency with already existing methods of StorageUsageDB which take in input a dictionary like this
          gLogger.info( "SEUsageAgent: processing directory: %s" % ( oneDirDict ) )
          # initialize the isRegistered flag. Change it according to the checks SE vs LFC
          # possible values of isRegistered flag are:
          # NotRegisteredInFC: data not registered in FC
          # RegisteredInFC: data correctly registered in FC
          # MissingDataFromSE: the directory exists in the LFC for that SE, but there is less data on the SE than what reported in the FC
          isRegistered = False
          LFCFiles = -1
          LFCSize = -1
          gLogger.info( "SEUsageAgent: check if dirName exists in su_Directory: %s" % dirPath )
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
              gLogger.debug( "SEUsageAgent: Path: %s su_Directory.DID %d" % ( dir, value[dir] ) )
            gLogger.debug( "SEUsageAgent: check if this particular replica is registered in the LFC." )
            res = self.__storageUsage.getAllReplicasInFC( dirPath )
            if not res['OK']:
              gLogger.info( "SEUsageAgent: ERROR failed to get replicas for %s directory " % dirPath )
              continue
            elif not res['Value']:
              gLogger.info( "SEUsageAgent: NO replica found for %s on any SE! Anomalous case: the LFN is registered in the FC but with NO replica! For the time being, insert it into problematicDirs table " % dir )
              # we should decide what to do in this case. This might happen, but it is a problem at FC level... TBD!
              isRegistered = 'NotRegisteredInFC'
            else: # got some replicas! let's see if there is one for this SE
              associatedDiracSEs = self.spaceTokens[ site ][spaceToken]['DiracSEs']
              gLogger.info( "SpaceToken: %s list of its DiracSEs: %s" % ( spaceToken, associatedDiracSEs ) )
              LFCFiles = 0
              LFCSize = 0
              for lfn in res['Value'].keys():
                matchedSE = ''
                for se in res['Value'][lfn].keys():
                  gLogger.info( "SpaceToken: %s -- se: %s" % ( spaceToken, se ) )
                  if se in associatedDiracSEs:
                    if oneDirDict[ dirPath ][ 'SpecialReplica' ]:# consider only the LFC replicas of the corresponding Dirac SE
                      gLogger.info( "SpecialReplica: %s" % oneDirDict[ dirPath ][ 'SpecialReplica' ] )
                      SESuffix = oneDirDict[ dirPath ][ 'SpecialReplica' ].upper()
                      if SESuffix not in se:
                        gLogger.info( "Se does not contain the suffix: %s. Skip it" % SESuffix )
                        continue
                    LFCFiles += int( res['Value'][lfn][ se ]['Files'] )
                    LFCSize += int( res['Value'][lfn][ se ]['Size'] )
                    gLogger.info( "==> the replica is registered in the FC with DiracSE= %s" % se )
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
                gLogger.info( "SEUsageAgent: entry %s correctly removed from se_Usage table" % oneDirDict )
              else:
                gLogger.info( "SEUsageAgent: entry %s was NOT in se_Usage table" % oneDirDict )
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
            gLogger.info( "SEUsageAgent: replica %s is registered! remove from problematicDirs (if necessary) and  publish to se_Usage table" % dirPath )
            res = self.__storageUsage.removeDirFromProblematicDirs( oneDirDict )
            if not res[ 'OK' ]:
              gLogger.error( "SEUsageAgent: ERROR failed to remove from problematicDirs: %s" % oneDirDict )
              continue
            else:
              removedDirs = res[ 'Value' ]
              if removedDirs:
                gLogger.info( "SEUsageAgent: entry %s correctly removed from problematicDirs" % oneDirDict )
              else:
                gLogger.info( "SEUsageAgent: entry %s was NOT in problematicDirs" % oneDirDict )
              res = self.__storageUsage.publishToSEReplicas( oneDirDict )
              if not res['OK']:
                gLogger.info( "SEUsageAgent: failed to publish %s entry to se_Usage table" % oneDirDict )
              else:
                gLogger.info( "SEUsageAgent: entry %s correctly published to se_Usage" % oneDirDict )
          else:
            gLogger.error( "Unknown value of isRegistered flag: %s " % isRegistered )

        gLogger.info( "Finished loop on file: %s " % fileP3 )

      gLogger.info( "Finished loop for site: %s " % site )
      # query problematicDirs table to get a summary of directories with the flag: NotRegisteredInFC
      #res = __storageUsage.getProblematicDirsSummary( site )
      # query problematicDirs table to get a summary of directories with the flag: MissingDataFromSE

    return S_OK()


  def readInputFile( self, site ):
    """ Download, read and parse input files with SEs content.
        Write down the results to  ASCII files.
        There are 3 phases in the manipulation of input files:
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
    StorageName = self.siteConfig[ site ][ 'StorageName' ]
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
      gLogger.info( "Found these files: %s" % previousFiles )
    except:
      gLogger.info( "no leftover to remove. Proceed downloading input files..." )
    if previousFiles:
      gLogger.info( "delete these files: %s " % previousFiles )
      for file in previousFiles:
        fullPath = targetPathForDownload + file
        gLogger.info( "removing %s" % fullPath )
        os.remove( fullPath )

    # Download input data made available by the sites. Reuse the code of dirac-install: urlretrieveTimeout, downloadAndExtractTarball
    res = self.downloadAndExtractTarball( originFileName, originURL, targetPath )
    if not res:
       return S_ERROR( "ERROR: Could not download input files" )

    defaultDate = 'na'
    defaultSize = 0

    if targetPath[-1] != "/":
      targetPath = "%s/" % targetPath

    InputFilesListP1 = os.listdir( targetPathForDownload )
    gLogger.info( "List of raw input files %s " % InputFilesListP1 )

    # delete all leftovers of previous runs from the pathToInputFiles
    try:
      previousParsedFiles = os.listdir( pathToInputFiles )
      gLogger.info( "Found these files: %s" % previousParsedFiles )
    except:
      gLogger.info( "no leftover to remove. Proceed to parse the input files..." )
    if previousParsedFiles:
      gLogger.info( "delete these files: %s " % previousParsedFiles )
      for file in previousParsedFiles:
        fullPath = pathToInputFiles + file
        gLogger.info( "removing %s" % fullPath )
        os.remove( fullPath )

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
      #if not self.spaceTokens[ site ][ st ][ 'Check']:
      #  gLogger.info( "Skip this space token: %s" % st )
      #  continue
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
      #/pnfs/grid.sara.nl/data/lhcb/data/2010/CHARMCONTROL.DST/00009283/0000/00009283_00000384_1.charmcontrol.dst          |   831797381 | 1296022620555
      # manipulate the input file to create a directory summary file (one row per directory)
      # hack necessary to deal with SARA's storage dumps:
      if 'files-outside-space-tokens.txt' in inputFileP1 or 'LHCbdefaultToken' in inputFileP1:
        gLogger.info( "For the time being , skip this file: %s " % inputFileP1)
        continue
      splitFile = inputFileP1.split( '.' )
      st = splitFile[ 0 ]
      completeSTId = st
      if len( splitFile ) > 3:
        # file with format provided by SARA e.g. LHCb_RAW.31455.INACTIVE.txt
        completeSTId = splitFile[0] + '.' + splitFile[1] + '.' + splitFile[2]
      fullPathFileP1 = targetPathForDownload + inputFileP1
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
          filePath = splitLine[0].rstrip()
          if 'dirac_directory' in filePath:
            dirac_dir_lines += 1
            continue
          size = splitLine[1].lstrip()
          totalSize += int( size )
          totalFiles += 1
          updated = splitLine[2].lstrip()
          newLine = filePath + ' ' + size + ' ' + updated
          if newLine[-1] != "\n":
            newLine = "%s\n" % newLine
          fP2.write( "%s" % newLine )
          processedLines += 1
        except:
          gLogger.error( "Error in input line format! Line is: %s" % line ) # the last line of these files is empty, so it will give this exception
          continue
      fP2.flush()
      gLogger.info( "%s - %s Total size: %d , total files: %d : publish to STSummary" % (site, completeSTId, totalSize, totalFiles))
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
      fileP2 = pathToInputFiles + file
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
        providedFilePath = splitLine[ 0 ]
        filePath = ''
        if self.siteConfig[ site ]['FileNameType'] == 'LFN':
          filePath = providedFilePath
        elif self.siteConfig[ site ]['FileNameType'] == 'PFN':
          res = self.getLFNPath( site, providedFilePath )
          if not res[ 'OK' ]:
            gLogger.error( "ERROR getLFNPath returned: %s " % res )
            continue
          filePath = res[ 'Value' ]
        #gLogger.debug("filePath: %s" %filePath)
        if not filePath:
          gLogger.info( "SEUsageAgent: it was not possible to get the LFN for PFN=%s, skip this line" % PFNfilePath )
          continue
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

    return 0


  def readInputFileCERN( self, site ):
    """ Download, read and parse input files with SEs content.
        Write down the results to  ASCII files.
        There are 3 phases in the manipulation of input files:
        phase 1- it is directly the format of the CASTOR dumps:
        /castor/cern.ch/grid/lhcb/data/2010/SDST/00008360/0004/00008360_00049557_1.sdst (618294889) size 950302398 status STAGED available YES requester lhcbprod,z5 (7947,1470)  datastaged 4 Aug 2011 22:22.13 lastaccess 5 Aug 2011 01:37.40 fileclass lhcb
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
    StorageName = self.siteConfig[ site ][ 'StorageName' ]
    if pathToInputFiles[-1] != "/":
      pathToInputFiles = "%s/" % pathToInputFiles
    gLogger.info( "Reading input files for site %s " % site )
    gLogger.info( "originFileName: %s , originURL: %s ,targetPath: %s , pathToInputFiles: %s " % ( originFileName, originURL, targetPath, pathToInputFiles ) )

    if targetPath[-1] != "/":
      targetPath = "%s/" % targetPath
    #targetPathForDownload = targetPath + 'LHCb/' # this is necessary for SARA, add it in the config dictionary
    targetPathForDownload = targetPath
    gLogger.info( "Target path to download input files: %s" % targetPathForDownload )
    # delete all existing files in the target directory if necessary:
    try:
      previousFiles = os.listdir( targetPathForDownload )
      gLogger.info( "Found these files: %s" % previousFiles )
    except:
      gLogger.info( "no leftover to remove. Proceed downloading input files..." )
    if previousFiles:
      gLogger.info( "delete these files: %s " % previousFiles )
      for file in previousFiles:
        fullPath = targetPathForDownload + file
        gLogger.info( "removing %s" % fullPath )
        os.remove( fullPath )

    # Download input data made available by the sites. Reuse the code of dirac-install: urlretrieveTimeout, downloadAndExtractTarball
    #res = self.downloadAndExtractTarball( originFileName, originURL, targetPath )
    res = self.downloadFiles( originFileName, originURL, targetPath )
    if not res:
       return S_ERROR( "ERROR: Could not download input files" )

    defaultDate = 'na'
    defaultSize = 0

    if targetPath[-1] != "/":
      targetPath = "%s/" % targetPath

    InputFilesListP1 = os.listdir( targetPathForDownload )
    gLogger.info( "List of raw input files %s " % InputFilesListP1 )

    # delete all leftovers of previous runs from the pathToInputFiles
    try:
      previousParsedFiles = os.listdir( pathToInputFiles )
      gLogger.info( "Found these files: %s" % previousParsedFiles )
    except:
      gLogger.info( "no leftover to remove. Proceed to parse the input files..." )
    if previousParsedFiles:
      gLogger.info( "delete these files: %s " % previousParsedFiles )
      for file in previousParsedFiles:
        fullPath = pathToInputFiles + file
        gLogger.info( "removing %s" % fullPath )
        os.remove( fullPath )

    # if necessary, merge the files in order to have one file per space token: LHCb-Tape, LHCb-Disk, LHCb_USER
    # this is necessary in the transition phase while there are still some data on the old space tokens of 2010
    gLogger.info( "Merge the input files to have one file per space token" )
    # Loop on N files relative to the site (one file for each space token)
    # previously open files for writing
    # every input file corresponds to one space token
    # whereas for the output, there is one file per NEW space token, so a merging is done

    #diskST = ['LHCb-Disk', 'LHCb_M-DST', 'LHCb_DST', 'LHCb_MC_M-DST', 'LHCb_MC_DST', 'LHCb_FAILOVER']
    #tapeST = ['LHCb-Tape', 'LHCb_RAW', 'LHCb_RDST']
    outputFileMerged = {}
    for st in self.spaceTokens[ site ].keys():
      if not self.spaceTokens[ site ][ st ][ 'Check']:
        gLogger.info( "Skip this space token: %s" % st )
        continue
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

    gLogger.info( "Parsed output files : " )
    for st in outputFileMerged.keys():
      gLogger.info( "space token: %s -> \n  %s\n %s  " % ( st, outputFileMerged[ st ]['MergedFileName'], outputFileMerged[ st ]['DirSummaryFileName'] ) )

    for inputFileP1 in InputFilesListP1:
      # the expected input file line is:
      #/castor/cern.ch/grid/lhcb/data/2011/RAW/FULL/LHCb/COLLISION11/97784/097784_0000000138.raw (865766672) size 3145781992 status STAGED available YES requester lhcbprod,z5 (7947,1470)  datastaged 2 Aug 2011 10:45.11 lastaccess 2 Aug 2011 10:45.11 fileclass lhcb_rawpfn | size | date
      # manipulate the input file to create a directory summary file (one row per directory)
      # retrieve the space token from the file name:
      splitFile = inputFileP1.split( '.' )
      serviceClass = splitFile[ 1 ]
      st = ''
      for spaceToken in self.spaceTokens[ site ].keys():
        if serviceClass == self.spaceTokens[ site ][spaceToken]['ServiceClass']:
          st = spaceToken
          break
      if not st:
        gLogger.error( "not possible to get space token for the service class: %s " % serviceClass )

      if not self.spaceTokens[ site ][ st ][ 'Check']:
        gLogger.info( "Skip this space token: %s" % st )
        continue
      gLogger.info( "+++++ processing input file for space token: %s " % st )
      fP2 = outputFileMerged[ st ]['pointerToMergedFile' ]
      fileP2 = outputFileMerged[ st ]['MergedFileName']
      fullPathFileP1 = targetPath + inputFileP1
      #fullPathFileP1 = targetPath + 'LHCb/' + inputFileP1
      gLogger.info( "Reading from file %s\n and writing to: %s" % ( fullPathFileP1, fileP2 ) )
      totalLines = 0 # counts all lines in input
      processedLines = 0 # counts all processed lines
      dirac_dir_lines = 0
      for line in open( fullPathFileP1, "r" ).readlines():
        totalLines += 1
        try:
          splitLine = line.split()
          filePath = splitLine[0].rstrip()
          if 'dirac_directory' in filePath:
            dirac_dir_lines += 1
            continue
          size = splitLine[3].lstrip()
          updated = 'na'
          newLine = filePath + ' ' + size + ' ' + updated
          fP2.write( "%s\n" % newLine )
          processedLines += 1
        except:
          gLogger.error( "Error in input line format! Line is: %s" % line ) # the last line of these files is empty, so it will give this exception
          continue
      fP2.flush()
      gLogger.info( "Total lines: %d , correctly processed: %d, dirac_directory found %d " % ( totalLines, processedLines, dirac_dir_lines ) )
    # close output files:
    for st in outputFileMerged.keys():
      p2 = outputFileMerged[ st ]['pointerToMergedFile' ]
      p2.close()

    gLogger.info("--------------------- Produce directory summaries files:")
    mergedFilesList = os.listdir( pathToInputFiles )
    for file in mergedFilesList:
      if 'Merged' not in file:
        continue
      fileP2 = pathToInputFiles + file
      gLogger.info( "Reading from Merged file fileP2 %s " % fileP2 )
      for spaceTok in self.spaceTokens[ site ].keys():
        if spaceTok in fileP2:
          st = spaceTok
          break

      gLogger.info( "Space token: %s" % st )
      totalLines = 0 # counts all lines in input
      processedLines = 0 # counts all processed lines
      self.dirDict = {}
      for line in open( fileP2, "r" ).readlines():
        if not line:
          continue # skip empty lines
        totalLines += 1
        splitLine = line.split()
        PFNfilePath = splitLine[ 0 ]
        res = self.getLFNPath( site, PFNfilePath )
        if not res[ 'OK' ]:
          gLogger.error( "getLFNPath returned: %s " % res )
          gLogger.error( "ERROR: could not retrieve LFN for PFN %s" % PFNfilePath )
          continue
        filePath = res[ 'Value' ]
        #gLogger.debug("filePath: %s" %filePath)
        if not filePath:
          gLogger.info( "SEUsageAgent: it was not possible to get the LFN for PFN=%s, skip this line" % PFNfilePath )
          continue
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

    return 0

  def getLFNPath( self, site, PFNfilePath ):
    """ Given a PFN returns the LFN, stripping the suffix relative to the particular site.
        Important: usually the transformation is done simply removing the SApath of the site. So for ARCHIVE and FREEZER and FAILOVER data:
        the LFN will be: /lhcb/archive/<LFN> etc...
        even if LHCb register those replicas in the LFC with the LFN: <LFN>, stripping the initial '/lhcb/archive'
        this is taken into account by the main method of the agent when it queries for replicas in the LFC
          """
    outputFile = "/opt/dirac/work/DataManagement/SEUsageAgent/unresolvedPFNs.txt"
    SEPath = self.siteConfig[ site ][ 'SAPath']
    LFN = 'None'
    try:
      LFN = PFNfilePath.split( SEPath )[1]
    except:
      gLogger.error( "ERROR retrieving LFN from PFN = %s, SEPath = %s " % ( PFNfilePath, SEPath ) )
      if not os.path.exists( outputFile ):
        of = open( outputFile , "w")
      else:
        of = open( outputFile , "a")
      of.write( "%s\n" % PFNfilePath )
      of.close()
      return S_ERROR( "Could not retrieve LFN" )
    # additional check on the LFN format:
    if not LFN.startswith( '/lhcb' ):
      gLogger.info( "SEUsageAgent: ERROR! LFN should start with /lhcb: PFN=%s LFN=%s. Skip this file." % ( PFNfilePath, LFN ) )
      if not os.listdir( outputFile ):
        of = open( outputFile , "w")
      else:
        of = open( outputFile , "a")
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



#  def downloadAndExtractTarball( pkgVer, targetPath, subDir = False, checkHash = True ):
  def downloadAndExtractTarball( self, originFileName, originURL, targetPath ):
    """ Borrowed from dirac-install ( slightly modified to fit in this agent).
    It download a tar archive and extract the content, using the method urlretrieveTimeout """

    tarName = "%s" % ( originFileName )
    # destination file:
    tarPath = os.path.join( targetPath, tarName )
    try:
      if not self.urlretrieveTimeout( "%s/%s" % ( originURL, tarName ), tarPath, 300 ):
        gLogger.error( "Cannot download %s" % tarName )
        return False
    except Exception, e:
      gLogger.error( "Cannot download %s: %s" % ( tarName, str( e ) ) )
      return False

    #Extract
    cwd = os.getcwd()
    os.chdir( targetPath )
    tf = tarfile.open( tarPath, "r" )
    for member in tf.getmembers():
      tf.extract( member )
    os.chdir( cwd )
    #Delete tar
    os.unlink( tarPath )
    return True

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





