"""   The BK Input Data Agent queries the bookkeeping for specified job input data and adds the
      file GUID to the job optimizer parameters.

"""

__RCSID__ = "$Id$"

from DIRAC.WorkloadManagementSystem.Agent.OptimizerModule  import OptimizerModule
from DIRAC.Core.DISET.RPCClient                            import RPCClient
from DIRAC                                                 import S_OK, S_ERROR

import time

class BKInputDataAgent( OptimizerModule ):

  def __init__( self, agentName, loadName, baseAgentName=False, properties={} ):
    """ c'tor
    """
    OptimizerModule.__init__( self, agentName, loadName, baseAgentName, properties )

    self.dataAgentName = self.am_getOption( 'InputDataAgent', 'InputData' )
    self.bkClient = None

  #############################################################################
  def initializeOptimizer( self ):
    """Initialize specific parameters for BKInputDataAgent.
    """
    self.dataAgentName = self.am_getOption( 'InputDataAgent', 'InputData' )

    #Define the shifter proxy needed
    # This sets the Default Proxy
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'ProductionManager' )
    self.bkClient = RPCClient( 'Bookkeeping/BookkeepingManager' )

    return S_OK()

  #############################################################################
  def checkJob( self, job, classAdJob ):
    """This method controls the checking of the job.
    """

    result = self.jobDB.getInputData( job )
    if not result['OK']:
      self.log.warn( 'Failed to get input data from JobdB for %s' % ( job ) )
      self.log.warn( result['Message'] )
      return result
    if not result['Value']:
      self.log.verbose( 'Job %s has no input data requirement' % ( job ) )
      return self.setNextOptimizer( job )

    self.log.verbose( 'Job %s has an input data requirement and will be processed' % ( job ) )
    inputData = result['Value']
    result = self.__determineInputDataIntegrity( job, inputData )
    if not result['OK']:
      self.log.warn( result['Message'] )
      return result
    return self.setNextOptimizer( job )

  #############################################################################
  def __determineInputDataIntegrity( self, job, inputData ):
    """This method checks the mutual consistency of the file catalog and bookkeeping information.
    """
    lfns = [fname.replace( 'LFN:', '' ) for fname in inputData]

    # Remove user generated files as they will not have BK entries
    self.log.info( "Obtained %s input files for job" % len( lfns ) )
    productionFiles = []
    for lfn in lfns:
      if not lfn.startswith( '/lhcb/user' ):
        productionFiles.append( lfn )
    if not productionFiles:
      self.log.info( "No production files to be checked" )
      return S_OK()
    self.log.info( "Checking the consistency of %s production files" % len( productionFiles ) )

    # Obtain the metadata stored in the BK
    start = time.time()
    res = self.bkClient.getFileMetadata( productionFiles )
    timing = time.time() - start
    self.log.info( 'BK Lookup Time: %.2f seconds ' % ( timing ) )
    if not res['OK']:
      self.log.warn( res['Message'] )
      return res

    # Fail the job if any of the files are not in the BK
    bkFileMetadata = res['Value']['Successful']
    badLFNs = []
    #bkGuidDict = {}
    try:
      for lfn in productionFiles:
        if lfn not in bkFileMetadata:
          badLFNs.append( 'BK:%s Problem: %s' % ( lfn, 'File does not exist in the BK' ) )
      if badLFNs:
        self.log.info( 'Found %s problematic LFN(s) for job %s' % ( len( badLFNs ), job ) )
        param = '\n'.join( badLFNs )
        self.log.info( param )
        result = self.setJobParam( job, self.am_getModuleParam( 'optimizerName' ), param )
        if not result['OK']:
          self.log.warn( result['Message'] )
        return S_ERROR( 'BK Input Data Not Available' )

      # Get the LFC metadata from the InputData optimizer
      res = self.getOptimizerJobInfo( job, self.dataAgentName )
      if not res['OK']:
        self.log.warn( res['Message'] )
        return S_ERROR( 'Failed To Get LFC Metadata' )
      lfcMetadataResult = res['Value']
      if not lfcMetadataResult['Value']:
        errStr = 'LFC Metadata Not Available'
        self.log.warn( errStr )
        return S_ERROR( errStr )
      lfcMetadata = res['Value']

      # Verify the consistency of the LFC and BK metadata
      badFileCount = 0
      for lfn, lfcMeta in lfcMetadata['Value']['Value']['Successful'].items():
        bkMeta = bkFileMetadata[lfn]
        badFile = False
        lfcGUID = lfcMeta.get( 'GUID', '' )
        if lfcGUID and lfcGUID.upper() != bkMeta['GUID'].upper():
          badLFNs.append( 'BK:%s Problem: %s' % ( lfn, 'LFC-BK GUID Mismatch' ) )
          badFile = True
        if int( lfcMeta.get( 'Size', 0 ) ) != int( bkMeta.get( 'FileSize', 0 ) ):
          badLFNs.append( 'BK:%s Problem: %s' % ( lfn, 'LFC-BK File Size Mismatch' ) )
          badFile = True

        # Prepare changes from "CheckSumType" to "ChecksumType"
        # and LFC ('AD') to DFC ('Adler32')
        checksumType = lfcMeta.get( 'CheckSumType', '' )
        if not checksumType:
          lfcMeta.get( 'ChecksumType', '' )
        if ( ( checksumType == 'AD' ) or ( checksumType == 'Adler32' ) ) and bkMeta['ADLER32']:
          if lfcMeta['CheckSumValue'].upper() != bkMeta['ADLER32'].upper():
            badLFNs.append( 'BK:%s Problem: %s' % ( lfn, 'LFC-BK Checksum Mismatch' ) )
            badFile = True
        if badFile:
          badFileCount += 1
      if badLFNs:
        return S_ERROR( 'BK-LFC Integrity Check Failed' )
    except:
      pass
    finally:

      # Failed the job if there are any inconsistencies
      if badLFNs:
        self.log.info( 'Found %s problematic LFN(s) for job %s' % ( badFileCount, job ) )
        param = '\n'.join( badLFNs )
        self.log.info( param )
        result = self.setJobParam( job, self.am_getModuleParam( 'optimizerName' ), param )
        if not result['OK']:
          self.log.warn( result['Message'] )

    return S_OK()
