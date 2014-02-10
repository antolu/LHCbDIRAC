"""Utilities for LHCbDIRAC/DataManagement/Agent/MergingForDQAgent. """


import glob, os, re, subprocess, time

import DIRAC
from DIRAC                                                      import gLogger, S_OK
from DIRAC.Core.Utilities.File                                  import getSize, getMD5ForFiles
from DIRAC.Core.Utilities.List                                  import sortList

from DIRAC.DataManagementSystem.Client.FailoverTransfer         import FailoverTransfer
from DIRAC.DataManagementSystem.Client.DataManager              import DataManager
from DIRAC.Resources.Catalog.FileCatalog                        import FileCatalog
from DIRAC.Resources.Utilities                                  import Utils
from DIRAC.Resources.Storage.StorageElement                     import StorageElement
from DIRAC.RequestManagementSystem.Client.Request               import Request

from LHCbDIRAC.Core.Utilities.File                              import makeGuid
from LHCbDIRAC.Core.Utilities.XMLTreeParser                     import addChildNode
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient       import BookkeepingClient
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

# Libraries needed for XML report
from xml.dom.minidom import Document, DocumentType

__RCSID__ = "$Id: $"

def getRuns( bkDict, bkClient ):
  '''
  GetRuns retrieve the list of files that correspond to the query bkDict. After that they are grouped per run.
  The output is a dictionary such as for example:

  results_ord = {'BRUNELHIST':{1234:
                   {'LFNs':['/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017703_1_Hist.root',
                            '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017894_1_Hist.root',
                            '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017749_1_Hist.root',
                            '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017734_1_Hist.root',
                            '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017810_1_Hist.root',
                            '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017838_1_Hist.root'
                           ]
                   }
                  1234:
                   {'LFNs':['/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017703_1_Hist.root',
                            '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017894_1_Hist.root',
                            '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017749_1_Hist.root',
                            '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017734_1_Hist.root',
                            '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017810_1_Hist.root',
                            '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017838_1_Hist.root'
                           ]
                   }
                  }
               'DAVINCIHIST':{
                              similar structure to BRUNELHIST
                             }
               }
  '''

  results_ord = {}

  results = bkClient.getVisibleFilesWithMetadata( bkDict )
  gLogger.debug( "Called bkClient method getVisibleFilesWithMetadata" )
  gLogger.debug( "with bk query %s" % str( bkDict ) )

  if not results[ 'OK' ]:
    gLogger.error( "Failed to retrieve data for the given query. Result is %s" % str( results ) )
    return results_ord

  for lfn in  results[ 'Value' ][ 'LFNs' ]:
    runNr = results[ 'Value' ][ 'LFNs' ][ lfn ][ 'Runnumber' ]

    if not results_ord.has_key( runNr ):
      results_ord[ runNr ] = { 'LFNs' : [] }
    results_ord[ runNr ][ 'LFNs' ].append( lfn )

  return results_ord

def getProductionId( run, procPass, eventTypeId , bkClient ):
  '''
  get the production ID for the run
  '''
  bkDict = {'Runnumber' : run,
            'ProcPass'  : procPass}
  res = bkClient.getProductionsFromView( bkDict )
  if not res['OK']:
    outMess = 'Cannot get the production id for run: %s and Processing Pass: %s' % ( run, procPass )
    gLogger.error( outMess )
    gLogger.error( res['Message'] )
    return res

  prodId = 0
  if not len( res['Value'] ):
    outMess = 'Empty production id list for run: %s Processing Pass: %s' % ( run, procPass )
    gLogger.error( outMess )
    gLogger.error( res )
    res['OK'] = False
    return res

  allProdList = res['Value']
  for prodList in allProdList:
    thisProdId = int( prodList[0] )
    res = bkClient.getProductionInformations( thisProdId )
    if not res['OK']:
      outMess = 'Cannot get the information for production %s' % ( thisProdId )
      gLogger.error( outMess )
      gLogger.error( res['Message'] )
      return res
    if int( res['Value']['Production informations'][0][2] ) == int( eventTypeId ):
      if thisProdId > prodId:
        prodId = thisProdId

  if prodId == 0:
    return prodId

  retVal = str( prodId )
  length = len( retVal )

  for _i in range( 8 - length ):
    retVal = '0' + retVal

  res = {}
  res['OK'] = True
  res['prodId'] = retVal
  return res

def mergeRun( bkDict, res_0, res_1, run, bkClient, transClient, homeDir, prodId , addFlag,
              specialMode, mergeStep1Command, mergeStep2Command, mergeStep3Command, brunelCount,
              daVinciCount, threshold, logFile, logFileName, environment ):

  '''

   mergeRun: Merge the BRUNELHIST and DAVINCIHIST of a particular run in three steps.

   1 - Group BRUNELHISTs and DAVINCIHISTs in groups of 50;
   2 - Merge the output of step 1;
   3 - Merge the output of step 2.
  '''

  results = {}
  results[ 'Merged' ] = False

  dm = DataManager()

  procPass = bkDict[ 'ProcessingPass' ]
  dqFlag = bkDict[ 'DataQualityFlag' ]
#  dtd         = bkDict[ 'DataTakingConditions' ]
#  eventTypeId = bkDict[ 'EventTypeId' ]
  eventType = bkDict[ 'EventTypeDescription' ]
  runData = {}
  brunelList = res_0[ run ][ 'LFNs' ]
  davinciList = res_1[ run ][ 'LFNs' ]

  runData[ 'BRUNELHIST' ] = brunelList
  runData[ 'DAVINCIHIST' ] = davinciList

  for histType in [ 'BRUNELHIST', 'DAVINCIHIST' ]:
    if len( runData[ histType ] ):
      _msg = "There are %d %s %s files in stream %s, processing pass %s for run %s."
      _msg = _msg % ( len( runData[histType] ), dqFlag, histType, eventType, procPass, str( run ) )
      gLogger.info( _msg )
    else:
      _msg = "There are no %s %s files in stream %s, processing pass %s for run %s."
      _msg = _msg % ( dqFlag, histType, eventType, procPass, str( run ) )
      gLogger.info( _msg )
      results[ 'OK' ] = False
      return results

  # Make sure the same number of histogram files is available
  # for DaVinci and Brunel
  countBrunel = len( runData[ 'BRUNELHIST' ] )
  countDaVinci = len( runData[ 'DAVINCIHIST' ] )

  if not countBrunel == countDaVinci:
    _msg = "Run %s in pass %s has different number of Brunel and DaVinci hist: %d vs. %d."
    gLogger.info( _msg % ( str( run ), procPass, countBrunel, countDaVinci ) )
    results[ 'OK' ] = False
    return results
  if countBrunel == 0:
    results[ 'OK' ] = False
    return results

  targetFile = _getTargetFile( run, prodId , homeDir , addFlag )

  # Redundancy check of existence
  retVal = os.path.exists( targetFile )

  if retVal:
    results[ 'OK' ] = False
    results[ 'Message' ] = '%s already merged' % targetFile
    return results

  # Check if enough files have been reconstructed
  retVal, res = _verifyReconstructionStatus( run, runData, bkDict, eventType ,
                                            bkClient , transClient, specialMode , threshold )
  if not res[ 'OK' ]:
    results[ 'OK' ] = False
    return results

  gLogger.info( "Now processing run %s in pass %s." % ( str( run ), bkDict[ 'ProcessingPass' ] ) )

  brunelLocal = []
  davinciLocal = []

  res = {}
  gLogger.info( '===>Retrieving Brunel histograms locally' )
  for lfn in brunelList:
    res[ 'OK' ] = False
    splittedLFN = lfn.split( '/' )
    localfile = homeDir + splittedLFN[ len( splittedLFN ) - 1 ]
    if os.path.exists( localfile ):
      gLogger.info( "Found file %s in local temp folder" % localfile )
      brunelLocal.append( localfile )
    else:
      retry = 1
      while retry < 6  and ( ( not res[ 'OK' ] ) or ( not res[ 'Value' ][ 'Successful' ] ) ):
        gLogger.info( "Trying to download %s time" % retry )
        res = dm.getFile( lfn, homeDir )
        retry = retry + 1
      if res[ 'OK' ] and res[ 'Value' ][ 'Successful' ].has_key( lfn ):
        brunelLocal.append( res[ 'Value' ][ 'Successful' ][ lfn ] )
      else:
        gLogger.error( "Cannot retrieve %s" % lfn )
        return results

  gLogger.info( '===>Retrieving DaVinci histograms locally' )
  for lfn in davinciList:
    res[ 'OK' ] = False
    splittedLFN = lfn.split( '/' )
    localfile = homeDir + splittedLFN[ len( splittedLFN ) - 1 ]
    if os.path.exists( localfile ):
      gLogger.info( "Found file %s in local temp folder" % localfile )
      brunelLocal.append( localfile )
    else:
      retry = 1
      while retry < 6  and ( ( not res[ 'OK' ] ) or ( not res[ 'Value' ][ 'Successful' ] ) ):
        gLogger.info( "Trying to download %s time" % retry )
        res = dm.getFile( lfn, homeDir )
        retry = retry + 1
      if res[ 'OK' ] and res[ 'Value' ][ 'Successful' ].has_key( lfn ):
        davinciLocal.append( res[ 'Value' ][ 'Successful' ][ lfn ] )
      else:
        gLogger.error( "Cannot retrieve %s" % lfn )
        return results


  # Real Merging part
  res = _merge( targetFile, run, brunelLocal , davinciLocal ,
               mergeStep1Command, mergeStep2Command, mergeStep3Command,
               homeDir, brunelCount , daVinciCount ,
               logFile , logFileName , environment )

  if res[ 'OK' ]:
    results[ 'Merged' ] = True
    results[ 'OutputFile' ] = targetFile
    return results

def _getTargetFile( run , prodId, homeDir , addFlag ):
  """
  getTargetFile:

    Define the full path of the final histogram file.
  """

  if addFlag == 'True':
    targetFile = '%sBrunelDaVinci_run_%s_%s.root' % ( homeDir , prodId , run )
  else:
    targetFile = '%sBrunelDaVinci_run_%s.root' % ( homeDir , run )
  return targetFile

def _fromBKK( bkDict, run, bkClient ):
  """ WARNING: lacking precision in case this wasn't reconstructed completely (can't do anything about it!)
  """

  rawBkDict = {}
  rawBkDict[ 'EventType' ] = bkDict[ 'EventTypeId' ]
  rawBkDict[ 'ConfigName' ] = 'LHCb'
  rawBkDict[ 'ConfigVersion' ] = bkDict[ 'ConfigVersion' ]
  rawBkDict[ 'StartRun' ] = run
  rawBkDict[ 'EndRun' ] = run
  rawBkDict[ 'FileType' ] = 'RAW'
  rawBkDict[ 'ProcessingPass' ] = 'Real Data'
  rawBkDict[ 'ReplicaFlag' ] = 'All'

  gLogger.info( "=====VerifyReconstructionStatus=====" )
  gLogger.info( str( rawBkDict ) )
  gLogger.info( "====================================" )

  reconstructedRAWFiles = bkClient.getFiles( rawBkDict )
  if not reconstructedRAWFiles['OK']:
    gLogger.error( "Cannot get RAW files for run %s" % str( run ) )
    gLogger.error( reconstructedRAWFiles[ 'Message' ] )
    return False

  reconstructedRAWFiles = reconstructedRAWFiles['Value']

  return reconstructedRAWFiles

def _fromTransformationDB( bkDict, run, bkClient, transClient = None ):
  """ Try to get from the TransformationDB
  """

  if transClient is None:
    tc = TransformationClient()
  else:
    tc = transClient

  transfForRun = tc.getTransformationRuns( {'RunNumber':run} )
  if not transfForRun['OK'] or not transfForRun['Records']:
    gLogger.warn( "Cannot get transformation that are processing run %s" % str( run ) )
    return False, 0

  prodIDs = [transf[0] for transf in transfForRun['Records']]
  transfIDs = []
  for prodID in prodIDs:
    res = bkClient.getProductionProcessingPass( prodID )
    if not res['OK']:
      continue
    else:
      if res['Value'] == bkDict['ProcessingPass']:
        transfIDs.append( prodID )

  if len( transfIDs ) != 1:
    gLogger.error( "More than one Transformation found" )
    return False, 0
  else:
    transfID = transfIDs[0]

  res = tc.getTransformationFiles( {'TransformationID':transfID,
                                    'Status': ['Assigned', 'Unused', 'Processed'],
                                    'RunNumber':run} )

  if res['OK']:
    return [raw['LFN'] for raw in res['Value']], transfID
  else:
    return False, 0

def _verifyReconstructionStatus( run, runData, bkDict, eventType, bkClient, transClient, specialMode , threshold ):
  """
  VerifyReconstructionStatus:

    Check that enough RAW data have been reconstructed and one and only one output
  is generated for each one of them.
  """

  retVal = {}
  retVal[ 'OK' ] = False
  retVal[ 'BRUNELHIST' ] = []
  retVal[ 'DAVINCIHIST' ] = []
  res = {}
  res['OK'] = False

  fraction = 1.0
  reconstructedRAWFiles, transfID = _fromTransformationDB( bkDict, run, bkClient, transClient )
  if not reconstructedRAWFiles:
    # if not present, try to re-calculate the fraction
    if transfID:
      reconstructedRAWFiles = _fromBKK( bkDict, run, bkClient )
      res = transClient.getTransformationParameters( transfID, ['FractionToProcess'] )
      if not res['OK']:
        gLogger.error( 'Problem getting from Transformation Parameters: %s' % res['Message'] )
      else:
        fraction = float( res['Value'] )
    else:
      reconstructedRAWFiles = _fromBKK( bkDict, run, bkClient )

  if not reconstructedRAWFiles:
    return ( retVal, res )

  countRAW = int( len( reconstructedRAWFiles ) * fraction )
  countBrunel = len( runData[ 'BRUNELHIST' ] )

  counter = 0
  descendants = bkClient.getFileDescendants( reconstructedRAWFiles, 2 )
  gLogger.info( "=== Performing check for multiple run in RAW ancestors ===" )
  descendants_cleaned = {}
  for raw in reconstructedRAWFiles:
    if not raw in descendants[ 'Value' ][ 'Successful' ].keys():
      continue
    descendants_cleaned[raw] = []
    for brunel_lfn in runData[ 'BRUNELHIST' ]:
      if brunel_lfn in descendants[ 'Value' ][ 'Successful' ][ raw ]:
        descendants_cleaned[raw].append( brunel_lfn )
        # Check for file mde from multiple run RAWs. Only for Brunel histograms.
        ancestors = bkClient.getFileAncestors( brunel_lfn, 2 )
        res = _checkMultiple( ancestors, brunel_lfn )
        if not res['OK']:
          return ( retVal, res )
    for davinci_lfn in runData[ 'DAVINCIHIST' ]:
      if davinci_lfn in descendants[ 'Value' ][ 'Successful' ][ raw ]:
        descendants_cleaned[raw].append( davinci_lfn )
    if len( descendants_cleaned[raw] ) == 2:
      counter = counter + 1

  # Make sure enough files have been reconstructed in the run.

  alt_counting = False
  # Special counting tempted.
  gLogger.info( "countBrunel %s countRAW %s" % ( countBrunel, countRAW ) )
  if not countBrunel == countRAW:
    if specialMode == 'True':
      _msg = "Run %s in pass %s accepted by special mode selection."
      gLogger.info( _msg % ( str( run ), 'Real Data' ) )
    else:
      #
      # New 95% or hist = RAW - 1 selection
      #
      if not eventType == "Full stream":
        gLogger.info( "Run %s in pass %s is not completed." % ( run, bkDict[ 'ProcessingPass' ] ) )
        metaDataDict = {}
        metaDataDict['DQFlag'] = 'P'
        metaDataDict['ProcessingPass'] = bkDict[ 'ProcessingPass' ]
        metaDataDict['Info'] = 'Not Completed'
        transClient.addRunsMetadata( run, metaDataDict )
        res[ 'OK' ] = False
        return ( retVal, res )

      #
      # New 95% or hist = RAW - 1 selection
      #
      if ( counter < threshold * countRAW ) and ( counter < ( countRAW - 1 ) ):
        gLogger.info( "Run %s in pass %s is not completed. Number of RAW = %d, number of hists = %d. \
        Trying to count the number of histogram from the RAW descendants." \
        % ( run, bkDict['ProcessingPass'], int( countRAW ), int( countBrunel ) ) )

        gLogger.info( "Found %s BRUNELHIST descendants." % counter )
        gLogger.info( "Found %s DAVINCIHIST descendants." % counter )

        if ( counter >= threshold * countRAW ) or ( counter == ( countRAW - 1 ) ):
          alt_counting = True
        else:
          _msg = "Run %s in pass %s is not completed. Number of RAW = %d, Number of HISTS = %d"
          gLogger.info( _msg % ( run, bkDict[ 'ProcessingPass' ], int( countRAW ) , int( counter ) ) )
          metaDataDict = {}
          metaDataDict['DQFlag'] = 'P'
          metaDataDict['ProcessingPass'] = bkDict[ 'ProcessingPass' ]
          metaDataDict['Info'] = 'Not Completed'
          transClient.addRunsMetadata( run, metaDataDict )
          res[ 'OK' ] = False
          return ( retVal , res )

      if not alt_counting:
        _msg = "Run %s in pass %s accepted by -1 or %s selection: Number of RAW = %d Number of HISTS = %d"
        gLogger.info( _msg % ( run, bkDict[ 'ProcessingPass' ], threshold, countRAW , countBrunel ) )
      else:
        _msg = "Run %s in pass %s accepted by directly counting of RAW descendants: \
        Number of RAW = %d Number of HISTS = %d"
        gLogger.info( _msg % ( run, bkDict[ 'ProcessingPass' ], countRAW , countBrunel ) )

  #
  # Make sure the RAW have one and one only BRUNELHIST and
  # DAVINCIHIST descendant
  #
  missing = { 'BRUNELHIST'  : [],
              'DAVINCIHIST' : []}
  res = {}
  res[ 'BRUNELHIST' ] = []
  res[ 'DAVINCIHIST' ] = []

  for raw in sortList( reconstructedRAWFiles ):
    res = _descendantIsDownloaded( raw, runData , bkClient )
    if not res[ 'OK' ]:
      _msg = "LFN %s in Run %s has too many hist descendants in processing pass %s"
      gLogger.info( _msg % ( raw, str( run ), bkDict[ 'ProcessingPass' ] ) )
      return ( retVal, res )

    if len( res[ 'BRUNELHIST' ] ) > 1:
      _msg = "LFN %s in Run %s has %s BRUNELHIST in processing pass %s"
      gLogger.info( _msg % ( raw, str( run ), len( res[ 'BRUNELHIST' ] ), bkDict[ 'ProcessingPass' ] ) )
      return ( retVal, res )
    elif len( res[ 'BRUNELHIST' ] ) == 0:
      missing[ 'BRUNELHIST' ].append( raw )
      continue

    if len( res[ 'DAVINCIHIST' ] ) > 1:
      _msg = "LFN %s in Run %s has %s DAVINCIHIST in processing pass %s"
      gLogger.info( _msg % ( raw, str( run ), len( res[ 'DAVINCIHIST' ] ), bkDict[ 'ProcessingPass' ] ) )
      return ( retVal, res )
    elif len( res[ 'DAVINCIHIST' ] ) == 0:
      missing[ 'DAVINCIHIST' ].append( raw )
      continue

    brunelHist = res[ 'BRUNELHIST' ][ 0 ]
    daVinciHist = res[ 'DAVINCIHIST' ][ 0 ]

    retVal[ 'BRUNELHIST' ].append( brunelHist )
    retVal[ 'DAVINCIHIST' ].append( daVinciHist )

  if ( len( retVal[ 'BRUNELHIST' ] ) > countBrunel ):
    _msg = "Run %s processing pass %s found %s BRUNELHIST expected %s"
    gLogger.info( _msg % ( str( run ), bkDict[ 'ProcessingPass' ], len( retVal[ 'BRUNELHIST' ] ), countBrunel ) )
    return ( retVal, res )
  else:
    if alt_counting:
      _msg = "Run %s processing pass %s found %s HIST by counting the RAW descendants."
      gLogger.info( _msg % ( str( run ), bkDict[ 'ProcessingPass' ], counter ) )
      retVal[ 'OK' ] = True
      return ( retVal, res )
  return ( retVal, res )

def _checkMultiple ( checkDict, lfn ):
  '''
  checkMultiple

  check for the presence of RAWs from multiple runs in the histograms ancestors
  '''

  for lfnFile in checkDict[ 'Value' ][ 'Successful' ][ lfn ]:
    if lfnFile[ 'FileType' ] == 'RAW':
      raw = lfnFile[ 'FileName' ]
      splittedRaw = raw.split( "/" )
      run_ref = splittedRaw[ -1 ].split( '_' )[ 0 ]
      break

  for lfnFile in checkDict[ 'Value' ][ 'Successful' ][ lfn ]:
    if lfnFile[ 'FileType' ] == 'RAW':
      raw = lfnFile[ 'FileName' ]
      splittedRaw = raw.split( "/" )
      run = splittedRaw[ -1 ].split( '_' )[ 0 ]
      if not ( run == run_ref ):
        gLogger.error( "Histograms created from MIXED RUNs!!!" )
        return S_OK()

  return S_OK()

def _descendantIsDownloaded( rawLFN, runData , bkClient ):
  """
  descendantIsDownloaded:

    Check if at least one descendant of a given event type has been downloaded.
  """

  retVal = {}
  retVal[ 'OK' ] = True
  retVal[ 'BRUNELHIST' ] = []
  retVal[ 'DAVINCIHIST' ] = []

  ( descList , res ) = _getDescendants( rawLFN , bkClient )
  if not res[ 'OK' ]:
    return res

  for descName in descList:
    if descName in runData[ 'BRUNELHIST' ]:
      retVal[ 'BRUNELHIST' ].append( descName )
    if descName in runData[ 'DAVINCIHIST' ]:
      retVal[ 'DAVINCIHIST' ].append( descName )

  if ( len( retVal[ 'BRUNELHIST' ] ) > 1 ) or ( len( retVal[ 'DAVINCIHIST' ] ) > 1 ):
    retVal[ 'OK' ] = False

  return retVal

def _getDescendants( rawLFN , bkClient ):
  """
  getDescendants: Get the list of descendants of a raw file in a given stream.
  """

  descLFN = []
  res = bkClient.getFileDescendants( rawLFN, 5 )

  if not res[ 'OK' ]:
    gLogger.error( "Unable to retrieve descendants for RAW %s" % rawLFN )
    return ( descLFN, res )

  if res[ 'Value' ].has_key( 'Successful' ):
    if res[ 'Value' ][ 'Successful' ].has_key( rawLFN ):
      for lfn in res[ 'Value' ][ 'Successful' ][ rawLFN ]:
        descLFN.append( lfn )
  return ( descLFN, res )

def _merge( targetFile, runNumber, brunelHist, daVinciHist, mergeStep1Command,
           mergeStep2Command, mergeStep3Command, homeDir,
           brunelCount, daVinciCount, logFile, logFileName, environment ):
  '''
  merge:  Merge all root files into one.
  '''

  retVal = {}
  retVal[ 'OK' ] = False
  retVal[ 'Merged' ] = False
  #
  # Get the current dir
  #
  cwd = os.getcwd()
  #
  # First step in Brunel and DaVinci merges the bulk of the files.
  # Second step merges the files previously merged/
  #
  gLogger.verbose( "===Brunel Step1===" )
  gLogger.verbose( brunelHist )
  gLogger.verbose( "==================" )

  brunelStep1 = _mergeStep1( brunelHist, 'Brunel', mergeStep1Command, runNumber , homeDir,
                            brunelCount, daVinciCount, logFile, logFileName, environment )
  _streamToLog( logFileName, gLogger, mergeStep1Command )

  if not brunelStep1[ 'OK' ]:
    _mergeCleanUp( brunelStep1[ 'step2Hist' ], "", [], "" )
    os.chdir( cwd )
    return retVal
  dim = len( brunelHist )

  gLogger.verbose( "===Brunel Step2===" )
  gLogger.verbose( brunelStep1[ 'step2Hist' ] )
  gLogger.verbose( "==================" )


  brunelStep2 = _mergeStep2( brunelStep1[ 'step2Hist' ], 'Brunel', mergeStep2Command,
                            dim , homeDir , logFile , logFileName, environment )

  _streamToLog( logFileName, gLogger, mergeStep2Command )

  if not brunelStep2[ 'OK' ]:
    _mergeCleanUp( brunelStep1[ 'step2Hist' ], "", [], "" )
    os.chdir( cwd )
    return retVal
  # input of final step
  brunelFile = brunelStep2[ 'file' ]

  gLogger.verbose( "===DaVinci Step1===" )
  gLogger.verbose( daVinciHist )
  gLogger.verbose( "===================" )


  daVinciStep1 = _mergeStep1( daVinciHist, 'DaVinci', mergeStep1Command, runNumber,
                             homeDir, brunelCount, daVinciCount, logFile, logFileName,
                             environment )

  _streamToLog( logFileName, gLogger, mergeStep1Command )

  if not daVinciStep1[ 'OK' ]:
    _mergeCleanUp( brunelStep1[ 'step2Hist' ], brunelFile, daVinciStep1[ 'step2Hist' ], "" )
    os.chdir( cwd )
    return retVal

  dim = len( daVinciHist )
  gLogger.verbose( "===DaVinci Step2===" )
  gLogger.verbose( daVinciStep1[ 'step2Hist' ] )
  gLogger.verbose( "===================" )
  daVinciStep2 = _mergeStep2( daVinciStep1[ 'step2Hist' ], 'DaVinci', mergeStep2Command,
                             dim , homeDir , logFile , logFileName, environment )

  _streamToLog( logFileName, gLogger, mergeStep2Command )

  if not daVinciStep2[ 'OK' ]:
    _mergeCleanUp( brunelStep1[ 'step2Hist' ], brunelFile, daVinciStep1[ 'step2Hist' ], "" )
    os.chdir( cwd )
    return retVal
  # input of final step
  daVinciFile = daVinciStep2[ 'file' ]

  #
  # Put Brunel and DaVinci in the same file.
  #
  merge = [ brunelFile, daVinciFile ]
  gLogger.verbose( "===Step3===" )
  gLogger.verbose( str( merge ) )
  gLogger.verbose( "===========" )

  merge.insert( 0, targetFile )
  merge.insert( 0, mergeStep3Command )
  command = " ".join( merge )
  returnCode = subprocess.call( args = command, env = environment , stdout = logFile , shell = True )
  gLogger.info( "=== Final Merging OutPut" )
  _streamToLog( logFileName, gLogger, mergeStep3Command )

  if not returnCode == 0:
    if os.path.isfile( targetFile ):
      os.remove( targetFile )
    os.chdir( cwd )
    return retVal

  #
  # Clean up the temporary files.
  #

  _mergeCleanUp( brunelStep1[ 'step2Hist' ], brunelFile, daVinciStep1[ 'step2Hist' ], daVinciFile )

  os.chdir( cwd )

  retVal[ 'OK' ] = True
  retVal[ 'Merged' ] = True
  retVal[ 'OutputFile' ] = targetFile

  return retVal

def _mergeStep1( stepHist, histType, mergeStep1Command, runNumber, homeDir ,
                brunelCount , daVinciCount , logFile , logFileName, environment ):
  """
  _mergeStep1

    Step 1 in the merging of root files.
  """

  retVal = {}
  retVal[ 'OK' ] = False
  retVal[ 'step2Hist' ] = []
  # Split the bunch of files in groups of 50
  splitBy = 50
  nStep = int( len( stepHist ) / splitBy ) + 1

  for i in range( 0, nStep ):
    first = i * splitBy
    last = ( i + 1 ) * splitBy

    mergeLFN = stepHist[ first:last ]
    if not len( mergeLFN ):
      break

    merge = []
    for lfn in mergeLFN:
      merge.append( lfn )

    if histType == 'Brunel':
      targetFile = '%s%s_%s_%s.root' % ( homeDir , histType , os.getpid() , str( brunelCount ) )
      gLogger.info( "Intermediate file for Brunel Step1 %s" % targetFile )
      brunelCount += 1
    elif histType == 'DaVinci':
      targetFile = '%s%s_%s_%s.root' % ( homeDir , histType , os.getpid() , str( daVinciCount ) )
      gLogger.info( "Intermediate file for DaVinci Step1 %s" % targetFile )
      daVinciCount += 1

    merge.insert( 0, targetFile )
    merge.insert( 0, mergeStep1Command )
    command = " ".join( merge )
    returnCode = subprocess.call( args = command, env = environment, stdout = logFile , shell = True )
    gLogger.debug( "Executing Step1 command %s" % command )
    _streamToLog( logFileName, gLogger, mergeStep1Command )

    if not returnCode == 0:
      gLogger.error( "Histograms merging failed for run %s at command: %s" % ( str( runNumber ), ' '.join[merge] ) )
      if os.path.isfile( targetFile ):
        os.remove( targetFile )
      return retVal
    retVal['step2Hist'].append( targetFile )
    _mergeCleanUp( mergeLFN, "", [], "" )

  gLogger.debug( "Step1 Merged histograms %s" % str( retVal[ 'step2Hist' ] ) )
  retVal['OK'] = True
  return retVal

def _mergeStep2( stepHist, histType, mergeStep2Command, dim, homeDir, logFile, logFileName, environment ):
  """
  MergeStep2

    Step 2 in the merging of root files.
  """

  retVal = {}
  retVal[ 'OK' ] = False
  retVal[ 'file' ] = []

  targetFile = '%s%s_%s.root' % ( homeDir, histType, os.getpid() )
  gLogger.debug( "Step2 intermediate file %s" % targetFile )
  merge = stepHist[0:len( stepHist )]
  merge.insert( 0, str( dim ) )
  merge.insert( 0, targetFile )
  merge.insert( 0, mergeStep2Command )
  command = " ".join( merge )
  returnCode = subprocess.call( args = command , env = environment , stdout = logFile , shell = True )
  gLogger.debug( "Executing Step2 command %s" % command )
  _streamToLog( logFileName, gLogger, mergeStep2Command )

  if not returnCode == 0:
    gLogger.error( "Step2 Failed!" )
    if os.path.isfile( targetFile ):
      os.remove( targetFile )
    return retVal

  retVal[ 'OK' ] = True
  retVal[ 'file' ] = targetFile
  gLogger.debug( "Step2 Merged histograms %s" % str( retVal[ 'file' ] ) )
  return retVal

def _mergeCleanUp( brunelStep2, brunelFile, daVinciStep2, daVinciFile ):
  """
  MergeCleanUp

    Remove all the temporary merge files.
  """

  gLogger.debug( "Cleaning intermediate files %s" % str( brunelStep2 ) )
  for mergeFile in brunelStep2:
    os.unlink( mergeFile )
  if len( brunelFile ):
    os.unlink( brunelFile )

  gLogger.debug( "Cleaning intermediate files %s" % str( daVinciStep2 ) )
  for mergeFile in daVinciStep2:
    os.unlink( mergeFile )
  if len( daVinciFile ):
    os.unlink( daVinciFile )
  return

def finalization( homeDir, logDir, lfns, outputFileName, logFileName, inputData, run, bkDict, rootVersion ):
  """
  Finalization Step:
   1 - Create a XML report containing all the Input Files and the output files (data and log).
   2 - Perform failoverTransfer.transferAndRegisterFile (LFC Upload and Registration)
   3 - From the Bookkeeping XML report previously created a request object is created.
   4 - If the registration in the BK is possible than the file is added to the calalog.
   5 - For the log file all the directory is Uploaded with the storageElement putDirectory.

  Output is a dictionary of dictionaries with first key the LFN of the file. The structure is:
  {'/lhcb/LHCb/Collision11/HIST/00010822/0000/Brunel_TEST_0.root':
    {'LocalPath':'',
    'FileSize':'',
    'MD5Sum':'',
    'Guid':''}
  '/lhcb/LHCb/Collision11/HIST/00010822/0000/Brunel_TEST_0.log':
    {'LocalPath':'',
    'FileSize':'',
    'MD5Sum':'',
    'Guid':''}
  }
  """

  output = {}
  output[ lfns[ 'DATA' ] ] = { 'Filename':'', 'FileSize':'', 'MD5Sum':'', 'Guid':'' }
  output[ lfns[ 'LOG' ] ] = { 'Filename':'', 'FileSize':'', 'MD5Sum':'', 'Guid':'' }

  output[ lfns[ 'DATA' ] ][ 'Filename' ] = outputFileName
  output[ lfns[ 'LOG' ] ][ 'Filename' ] = logDir + '/' + logFileName

  gLogger.info( "Creating XML report" )
  # Create the XML Bookkeeping Report
  res = _makeBookkeepingXML( output, lfns[ 'DATA' ], lfns[ 'LOG' ], inputData, run,
                            bkDict[ 'ConfigName' ], bkDict[ 'ConfigVersion' ], homeDir,
                            rootVersion, saveOnFile = True )

  logDict = { 'logdir' : logDir, 'logSE' : 'LogSE', 'logFilePath' : lfns[ 'LOGDIR' ],
             'logLFNPath' : lfns[ 'LOG' ] }

  gLogger.verbose( str( homeDir ) )
  gLogger.verbose( str( outputFileName ) )
  gLogger.verbose( str( lfns[ 'DATA' ] ) )
  gLogger.verbose( res[ 'XML' ] )
  gLogger.verbose( str( logDict ) )

  # Uploading data and logs
  results = _upLoadOutputData( homeDir, outputFileName, lfns[ 'DATA' ], res[ 'XML' ], logDict )
  results[ 'XML' ] = res[ 'XML' ]
  return results

def buildLFNs( bkDict, run , prodId , addFlag ):
  """
  Build LFN for data and logfile
  """

  lfns = {}
  gLogger.info( "Run %s has Prod ID %s" % ( run, prodId ) )
  if addFlag:
    pIDString = "_" + str( prodId )
  lfns[ 'DATA' ] = '/lhcb/LHCb/' + bkDict[ 'ConfigVersion' ] + '/HIST/' + str( run ) + '/BrunelDaVinci_' \
  + str( run ) + pIDString + '_Hist.root'
  lfns[ 'LOG' ] = '/lhcb/LHCb/' + bkDict[ 'ConfigVersion' ] + '/LOG/MERGEDDQ/' + str( run ) + \
  '/BrunelDaVinci_' + str( run ) + pIDString + '_Hist.log'
  lfns['LOGDIR'] = '/lhcb/LHCb/' + bkDict[ 'ConfigVersion' ] + '/LOG/MERGEDDQ/' + str( run )
  return lfns

def _streamToLog( stream, logger, exeName ):
  """
    Redirection of a stream to gLogger
  """

  try:
    streamFile = open( stream, "rb" )
  except IOError:
    logger.error( "Log file for %s doesn't exist" % exeName )
    return
  for line in streamFile:
    if not line:
      break
    logger.info( line )

def _generateJobNode( doc , configName , configVersion , ldate , ltime ):

  """ Node looks like
      <Job ConfigName="" ConfigVersion="" Date="" Time="">
  """
  jobAttributes = ( configName, configVersion, ldate, ltime )
  return addChildNode( doc, "Job", 1, jobAttributes )

def _generateInputFiles( jobNode , inputData ):
  """ InputData looks like this
      <InputFile Name=""/>
  """
  if inputData:
    for inputname in inputData:
      jobNode = addChildNode( jobNode, "InputFile", 0, ( inputname, ) )
  return jobNode

def _generateOutputFiles( jobNode, outputDict, outputlfn, logFilelfn, outputDataType,
                         outputDataVersion, configName, configVersion, run ):
  """
    OutputFile looks like this:

         <OutputFile Name="" TypeName="" TypeVersion="">
           <Parameter Name="" Value=""/>
           ...
           <Replica Location="" Name=""/>
           ....
         </OutputFile>

        What this exactly does, it is a mystery to me.
  """

  outputs = []
  outputs.append( ( outputlfn, outputDataType, outputDataVersion ) )
  outputs.append( ( logFilelfn, ( 'LOG' ), ( '1' ) ) )
  log = gLogger.getSubLogger( "BookkeepingReport" )
  log.info( outputs )

  for output, outputtype, outputversion in list( outputs ):
    log.info( 'Looking at output %s %s %s' % ( output, outputtype, outputversion ) )

    # Add Output to the XML file
    oFileAttributes = ( output, outputtype , outputversion )
    jobNode, oFile = addChildNode( jobNode, "OutputFile", 1, oFileAttributes )

    if not re.search( 'log', output ):
      # filesize = str( os.path.getsize( output ) )
      oFile = addChildNode( oFile, "Parameter", 0, ( "FileSize", outputDict[ output ][ 'FileSize' ] ) )
    else:
      logurl = 'http://lhcb-logs.cern.ch/storage/lhcb'
      fName = outputDict[ output ][ 'Filename' ].split( '/' )
      url = logurl + '/' + configName + '/' + configVersion + '/LOG/MERGEDDQ/' + str( run ) + '/' \
      + fName[len( fName ) - 1]
      # Log file replica information
      oFile = addChildNode( oFile, "Replica", 0, ( url, ) )
    oFile = addChildNode( oFile, "Parameter", 0, ( "MD5Sum", outputDict[ output ][ 'MD5Sum' ] ) )
    oFile = addChildNode( oFile, "Parameter", 0, ( "Guid", outputDict[ output ][ 'Guid' ] ) )
  return jobNode

def _generateTypedParams( jobNode , run , rootVersion , append_string ):
  '''
  add the needed parameters
  '''

  typedParams = []
  typedParams.append( ( "JobType", "DQHISTOMERGING" , "Info" ) )
  typedParams.append( ( "Name", str( run ) + append_string , "Info" ) )
  typedParams.append( ( "NumberOfEvents", "0" , "Info" ) )
  typedParams.append( ( "ProgramName", "ROOT" , "Info" ) )
  typedParams.append( ( "ProgramVersion", rootVersion, "Info" ) )

  for typedParam in typedParams:
    jobNode = addChildNode( jobNode, "TypedParameter", 0, typedParam )

  return jobNode

def _makeBookkeepingXML( output, outputlfn, logFilelfn, inputData, run, configName,
                        configVersion, homeDir, rootVersion, saveOnFile = True ):
  """
    Bookkeeping xml looks like this:

    <Job ConfigName="" ConfigVersion="" Date="" Time="">
      <TypedParameter Name="" Type="" Value=""/>
      ...
      <InputFile Name=""/>
        ...
      <OutputFile Name="" TypeName="" TypeVersion="">
        <Parameter Name="" Value=""/>
        ...
        <Replica Location="" Name=""/>
        ....
      </OutputFile>
    </Job>

  Output is a dictionary of dictionaries with first key the LFN of the file. The structure is:
  {'/lhcb/LHCb/Collision11/HIST/00010822/0000/Brunel_TEST_0.root':
    {'LocalPath':'',
    'FileSize':'',
    'MD5Sum':'',
    'Guid':''}
  '/lhcb/LHCb/Collision11/HIST/00010822/0000/Brunel_TEST_0.log':
    {'LocalPath':'',
    'FileSize':'',
    'MD5Sum':'',
    'Guid':''}
  }
  """

  logger = gLogger
  logger.setLevel( "info" )

  logger.info( "Making XML report " )

  res = {}
  res[ 'OK' ] = True
  res[ 'XML' ] = ''

  ldate = time.strftime( "%Y-%m-%d", time.localtime( time.time() ) )
  ltime = time.strftime( "%H:%M", time.localtime( time.time() ) )

  secs = time.localtime( time.time() ).tm_sec
  append_string = '_' + str( secs )

  for lfn in output.keys():
    if os.path.exists( output[ lfn ][ 'Filename' ] ):
      output[ lfn ][ 'FileSize' ] = str( getSize( output[ lfn ][ 'Filename' ] ) )
      output[ lfn ][ 'MD5Sum' ] = getMD5ForFiles( [ output[ lfn ][ 'Filename' ] ] )
      output[ lfn ][ 'Guid' ] = makeGuid( output[ lfn][ 'Filename' ] )[lfn]
    else:
      gLogger.error( 'File %s not found. Cannot write bookkeeping XML summary' % output[ lfn ][ 'Filename' ] )
      DIRAC.exit( 2 )

  # Generate XML document
  doc = Document()
  docType = DocumentType( "Job" )
  docType.systemId = "book.dtd"
  doc.appendChild( docType )

  # Generate JobNodegenerateJobNode( doc ,configName , configVersion , ldate , ltime):
  doc, jobNode = _generateJobNode( doc, configName, configVersion, ldate, ltime )

  jobNode = _generateTypedParams( jobNode, run, rootVersion, append_string )
  # Generate InputFiles
  jobNode = _generateInputFiles( jobNode, inputData )

  jobNode = _generateOutputFiles( jobNode, output, outputlfn, logFilelfn, 'MERGEFORDQ.ROOT',
                                 'ROOT', configName, configVersion, run )

  prettyXMLDoc = doc.toprettyxml( indent = "    ", encoding = "ISO-8859-1" )

  # horrible, necessary hack!
  prettyXMLDoc = prettyXMLDoc.replace( '\'book.dtd\'', '\"book.dtd\"' )

  if saveOnFile:

    bfilename = homeDir + 'bookkeeping_Merge_For_DQ_' + str( run ) + '.xml'
    res[ 'XML' ] = bfilename
    logger.info( "Saving XML report %s" % bfilename )
    bfile = open( bfilename, 'w' )
    print >> bfile, prettyXMLDoc
    bfile.close()
  else:
    print prettyXMLDoc

  return res

def _setBKRegistrationRequest( lfn, request, error = '' ):
  """
  Given an LFN and a request, a registration request for the Bookkeeping Catalog is sent.

  Set a BK registration request for changing the replica flag.  Uses the
  global request object.
  """
  if error:
    gLogger.error( 'BK registration for %s failed with message: "%s" setting failover request' % ( lfn, error ) )
  else:
    gLogger.info( 'Setting BK registration request for %s' % ( lfn ) )

  result = request.addSubRequest( {'Attributes':{'Operation':'registerFile',
                                                 'ExecutionOrder':2,
                                                 'Catalogue':'BookkeepingDB'}},
                                 'register' )
  if not result['OK']:
    gLogger.error( 'Could not set registerFile request:\n%s' % result )
    return result['OK']
  fileDict = {'LFN':lfn, 'Status':'Waiting'}
  index = result['Value']
  request.setSubRequestFiles( index, 'register', [fileDict] )
  return result

def _upLoadOutputData( localpath, localfilename, lfn, xMLBookkeepingReport, logDict ):
  """
  *localpath is the path where the file are in the local machine.
  *XMLBookkeepingReport is an XML created previously containing all infos about what will be uploaded.

  """
  os.chdir( localpath )

  bkClient = BookkeepingClient()

  oRequest = Request()
  oRequest.RequestName = xMLBookkeepingReport
  oRequest.SourceComponent = 'MergeForDQ'

  failoverTransfer = FailoverTransfer( oRequest )
  performBKRegistration = []
  registrationFailure = False

  # Registration in the LFC
  result = failoverTransfer.transferAndRegisterFile( fileName = localfilename,
                                                     localPath = localpath,
                                                     lfn = lfn,
                                                     destinationSEList = [ 'CERN-HIST' ],
                                                     fileMetaDict = {},
                                                     fileCatalog = 'LcgFileCatalogCombined' )
  if not result[ 'OK' ]:
    return result

  performBKRegistration.append( lfn )
  bkFileExtensions = ['bookkeeping*.xml']
  log = gLogger.getSubLogger( "UploadOutputData" )
  bkFiles = []
  for ext in bkFileExtensions:
    log.debug( 'Looking at BK record wildcard: %s' % ext )
    globList = glob.glob( ext )
  # Look for bookkeeping XML summaries
  for check in globList:
    if os.path.isfile( check ):
      log.info( 'Found locally existing BK file record: %s' % check )
      bkFiles.append( check )
  # result={}
  # return result
  bkFiles.sort()
  log.info( "bkfiles" )
  log.info( str( bkFiles ) )
  for bkFile in bkFiles:
    fopen = open( bkFile, 'r' )
    bkXML = fopen.read()
    fopen.close()
    result = bkClient.sendXMLBookkeepingReport( bkXML )
    if result['OK']:
      log.info( 'Bookkeeping report sent for %s' % bkFile )
    else:
      log.error( 'Could not send Bookkeeping XML file to server, preparing DISET request for', bkFile )
      request.setDISETRequest( result['rpcStub'], executionOrder = 0 )
  # Can now register the successfully uploaded files in the BK i.e. set the BK replica flags
  if not performBKRegistration:
    log.info( 'There are no files to perform the BK registration for, all could be saved to failover' )
    return result
  elif registrationFailure:
    _msg = 'There were catalog registration failures during the upload of'
    _msg += ' files for this job, BK registration requests are being prepared'
    log.info( _msg )
    for lfn in performBKRegistration:
      error = ''
      result = _setBKRegistrationRequest( lfn , request , error )
      log.info( str( result ) )
      if not result['OK']:
        return result

  else:
    result = FileCatalog( catalogs = ['BookkeepingDB'] ).addFile( performBKRegistration )
    log.verbose( result )
    if not result['OK']:
      log.err( 'Could Not Perform BK Registration' )
      return result
    if result['Value']['Failed']:
      for lfn, error in result['Value']['Failed'].items():
        result = _setBKRegistrationRequest( lfn, request, error )
        log.verbose( str( result ) )
        if not result['OK']:
          return result
    else:
      # if BK registration gone fine upload log file as well.
      log.verbose( "LogUpload results" )
      log.verbose( {logDict['logFilePath']:os.path.realpath( logDict['logdir'] )} )
      log.verbose( str( logDict['logSE'] ) )
      res = Utils.executeSingleFileOrDirWrapper( StorageElement( logDict['logSE'] ).putDirectory( { logDict['logFilePath'] : os.path.realpath( logDict['logdir'] ) } ) )

      log.verbose( str( res ) )

  return result

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
