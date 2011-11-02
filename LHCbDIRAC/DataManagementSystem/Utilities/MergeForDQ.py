import DIRAC
from DIRAC                                                 import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.List                             import sortList
from DIRAC.FrameworkSystem.Client.NotificationClient                    import NotificationClient
from DIRAC.Interfaces.API.Dirac import Dirac
import subprocess
from subprocess import PIPE
import os,sys
import itertools, copy
import re
'''
                                                                              
 GetRunningConditions:                                                        
                                                                              
 Find all known running conditions for the selected configurations.           
                                                                              
'''
def GetRunningConditions( bkTree , bkClient ):
  res = {}
  for cfgName in sortList( bkTree.keys() ):
    bkDict = {'ConfigName' : cfgName}
    for cfgVersion in sortList( bkTree[cfgName].keys() ):
      cfgData = bkTree[cfgName][cfgVersion]
      bkDict['ConfigVersion'] = cfgVersion
      res = bkClient.getConditions( bkDict )
      if not res['OK']:
        outMess = 'Cannot load the data taking conditions for version %s' % ( cfgVersion )
        gLogger.error( outMess )
        gLogger.error( res['Message'] )
        return ( bkTree, res )

      for recordList in res['Value']:
        if recordList['TotalRecords'] == 0:
          continue
        parNames = recordList['ParameterNames']

        descId = -1
        for thisId in range( len( parNames ) ):
          parName = parNames[thisId]
          if parName == 'Description':
            descId = thisId
            break

        if descId >= 0:
          records = recordList['Records']
          for record in records:
            desc = record[descId]
            #
            # Skip non interesting DataTakingDescriptions
            #
            if re.search( '3500', desc ):
              if re.search( 'Excl', desc ):
                continue
              elif re.search( 'VeloOpen', desc ):
                continue
            elif re.search( '1380', desc ):
              if re.search( 'Excl', desc ):
                continue
            else:
              continue

            if not cfgData.has_key( desc ):
              cfgData[desc] = {}

  return ( bkTree, res )

'''
                                                                              
 GetProcessingPasses:                                                         
                                                                              
 Find all known processing passes for the selected configurations.            
                                                                              
'''

def GetProcessingPasses( bkTree , bkClient ):
  res = {}
  for cfgName in sortList( bkTree.keys() ):
    bkDict = {'ConfigName' : cfgName}
    for cfgVersion in sortList( bkTree[cfgName].keys() ):
      cfgData = bkTree[cfgName][cfgVersion]
      bkDict['ConfigVersion'] = cfgVersion
      for dtd in cfgData.keys():
        dtdData = cfgData[dtd]
        bkDict['ConditionDescription'] = dtd
        res = bkClient.getProcessingPass( bkDict, '/Real Data' )
        if not res['OK']:
          gLogger.error( 'Cannot load the processing passes for Version %s Data taking condition' % ( cfgVersion, dtd ) )
          gLogger.error( res['Message'] )
          return ( bkTree, res )

        for recordList in res['Value']:
          if recordList['TotalRecords'] == 0:
            continue
          parNames = recordList['ParameterNames']

          found = False
          for thisId in range( len( parNames ) ):
            parName = parNames[thisId]
            if parName == 'Name':
              found = True
              break
          if found:
            for reco in recordList['Records']:
              recoName = '/Real Data/' + reco[0]
              if not dtdData.has_key( recoName ):
                dtdData[recoName] = {}

  return ( bkTree, res )


'''                                                                              
 GetRuns                                                                      
                                                                              
 Find all runs in the given configuration version.                            
                                                                              
bkDict = {'EventType': 91000000, 'ConfigVersion': 'Collision11',
'ConfigName': 'LHCb', 'DataQualityFlag': 'UNCHECKED'}
'''

def GetRuns( bkTree, bkClient , eventType, evtTypeList, dqFlag ):

  evtTypeId = int( evtTypeList[eventType] )
  dtdList = {}
  for cfgName in sortList( bkTree.keys() ):
    bkDict = {'ConfigName' : cfgName}
    for cfgVersion in sortList( bkTree[cfgName].keys() ):
        cfgData = bkTree[cfgName][cfgVersion]
        bkDict['ConfigVersion'] = cfgVersion
        for dtd in cfgData.keys():
          dtdList[dtd] = True

  bkDict = {'EventType'       : evtTypeId,
            'DataQualityFlag' : dqFlag}
  for cfgName in sortList( bkTree.keys() ):
    bkDict['ConfigName'] = cfgName
    for cfgVersion in sortList( bkTree[cfgName].keys() ):
      cfgData = bkTree[cfgName][cfgVersion]
      bkDict['ConfigVersion'] = cfgVersion
      res = bkClient.getRuns( bkDict )
      if not res['OK']:
        gLogger.error( 'Cannot load the run list for version %s' % ( cfgVersion ) )
        gLogger.error( res['Message'] )
        return res

      runList = sortList( res['Value'] )
      runList.reverse()

      gLogger.info( "There are %d runs in %s/%s" %
                    (len( runList ), cfgName, cfgVersion ) )

      for ll in runList:
        for run in ll:
## FIX THIS !!!!!!!!
##
#          if run == 102550 or run == 102762:
#            continue
#                      
          bkDict['EndRun'] = run
          bkDict['StartRun'] = run
          res = bkClient.getRunInformations( int( run ) )
          if not res['OK']:
            gLogger.error( 'Cannot load the information for run %s' % ( run ) )
            gLogger.error( res['Message'] )
            return res
          #
          # Skip the run if it is not in a data taking condition of interest.
          #
          dtd = res['Value']['DataTakingDescription']
          
          if not dtdList.has_key( dtd ):
            continue
          bkDict['ConditionDescription'] = dtd
          
          #
          # Find which processing passes are available for this run
          #
          res = bkClient.getRunProcPass( {'RunNumber' : run} )
          if not res['OK']:
            gLogger.error( 'Cannot load the processing pass for run %s' % ( run ) )
            gLogger.error( res['Message'] )
            return res
          dtdData = cfgData[dtd]
          for pair in res['Value']:
            runNumber = pair[0]
            thisPass = pair[1]
             #
#            # ANOTHER TEMPORARY PATCCH
#            #
#            if re.search('Reco11a', thisPass):
#              bkDict['DataQualityFlag'] = 'UNCHECKED'
#            else:
#              bkDict['DataQualityFlag'] = 'EXPRESS_OK'
#            #
#            # TEMPORARY PATCH !!!!
#            #

#            if int(runNumber) == 96813:
#              continue
            if int( run ) == int( runNumber ) and dtdData.has_key( thisPass ):
               #
               # Check if a flag is already available for
               # this run in this pass
               #
              
              isFlagged = IsRunFlagged( run, eventType, thisPass , bkClient )
              if isFlagged:
                continue
                            
              bkDict['ProcessingPass'] = thisPass
    return ( bkDict , res )






'''
                                                                              
 MergeRun:                                                                    
                                                                              
 Scan the whole bookkeeping tree and process unflagged runs.                  
                                                                              
'''
def MergeRun( bkDict, eventType , histTypeList , bkClient , homeDir , testDir , testMode ,
              specialMode , mergeExeDir , mergeStep1Command, mergeStep2Command, mergeStep3Command, 
              specialmode,testmode, castorHistPre, castorHistPost , workDir ,
              brunelCount , daVinciCount , logFile , logFileName ,dirac ,senderAddress , mailAddress):

  run = bkDict['StartRun']
  procPass = bkDict['ProcessingPass']
  dqFlag = bkDict['DataQualityFlag']
  dtd = bkDict['ConditionDescription']
  eventTypeId = bkDict['EventType']
  
  runData = {}
  for histType in histTypeList:
    bkDict['FileType'] = histType
    lfns, res = GetStreamHIST( bkDict , bkClient )
    if not res['OK']:
      return res
    runData[histType] = lfns
    if len( runData[histType].keys() ):
      outMess = "There are %d %s %s files in stream %s, processing pass %s for run %s." % ( 
        len( runData[histType].keys() ), dqFlag, histType, eventType, procPass, run )
      gLogger.info( outMess )
    else:
      gLogger.info( "There are no %s %s files in stream %s, processing pass %s for run %s." % ( 
        dqFlag, histType, eventType, procPass, run ) )
      res['OK'] = False
      return res
  
  #
  # Make sure the same number of histogram files is available
  # for DaVinci and Brunel
  #
  countBrunel = len( runData['BRUNELHIST'].keys() )
  countDaVinci = len( runData['DAVINCIHIST'].keys() )
  
  if not countBrunel == countDaVinci:
    gLogger.info( "Run %s in pass %s has different number of Brunel and DaVinci hist: %d vs. %d." % ( 
      run, procPass, countBrunel, countDaVinci ) )
    res['OK'] = False
    return res
  if countBrunel == 0:
    res['OK'] = False
    return res
  #
  # Check if the histograms have already been merged.
  #
  res = MakeDestination( run, bkDict , homeDir ,testDir ,testMode ,specialMode)
  if not res['OK']:
    outMess = 'Cannot create destination directory for run %s in pass %s' % ( run, procPass )
    gLogger.error( outMess )
    res['Message'] = 'Cannot create destination directory'
    return res
  destDir = res['DestDir']
  retVal, res = GetProductionId( run, procPass, eventTypeId , bkClient )
  if not res['OK']:
      return res
  if int( retVal ) == 0:
    res['OK'] = False
    return res
  targetFile = GetTargetFile( run, retVal, eventType, destDir  )
  retVal , res = TargetFileExists( targetFile, run , homeDir ,dirac)
  if retVal:
    return res
  #
  # Check if enough files have been reconstructed
  #
  retVal, res = VerifyReconstructionStatus( run, runData, bkDict, eventType ,
                                            bkClient , specialMode )
  if not res['OK']:
    return res

  gLogger.info( "Now processing run %s in pass %s." % ( 
  run, bkDict['ProcessingPass'] ) )
  
  brunelHist = retVal['BRUNELHIST']
  daVinciHist = retVal['DAVINCIHIST']

  res = Merge( targetFile, run, brunelHist, daVinciHist , mergeExeDir ,
               mergeStep1Command, mergeStep2Command, mergeStep3Command,
               specialMode,testMode, castorHistPre, castorHistPost, homeDir, workDir ,
               brunelCount , daVinciCount , logFile , logFileName , dirac)
  os.remove(logFileName)
  
  if res['Merged'] and not testMode:
    outMess = 'Run %s pass %s stream %s beam %s completed, merged and uploaded to castor.\n' %(
    run, procPass, eventType, dtd)
    outMess = outMess + 'ROOT file : %s' %(targetFile)
    notifyClient = NotificationClient()
    subject      = 'New %s merged stream ROOT file for run %s ready' %(eventType, run)
    res = notifyClient.sendMail(mailAddress, subject, outMess,
                                senderAddress, localAttempt=False)

  return res

'''
                                                                             
MakeDestination:                                                             
                                                                             
Create the path to the merged file location.                                 
                                                                              
'''
def MakeDestination( run, bkDict , homeDir , testDir ,testMode ,specialMode ):
  retVal = {}
  retVal['OK'] = False

  baseDir = homeDir
  if testMode:
    baseDir = testDir
  res = MakeDir( baseDir, bkDict['ConfigVersion'] )

  if not res['OK']:
    return retVal
  configDir = res['NewDir']

  res = MakeDir( configDir, bkDict['ConditionDescription'] )
  if not res['OK']:
    return retVal
  dtdDir = res['NewDir']

  res = MakeDir( dtdDir, run )
  if not res['OK']:
    return retVal
  runDir = res['NewDir']

  procPassDirName = re.sub( '\/', '', bkDict['ProcessingPass'] )
  procPassDirName = re.sub( 'Real Data', '', procPassDirName )
  procPassDirName = re.sub( '\+', '', procPassDirName )
  procPassDirName = re.sub( '\s* ', '_', procPassDirName )

  res = MakeDir( runDir, procPassDirName )
  if not res['OK']:
    return retVal
  passDir = res['NewDir']

  res = MakeDir( passDir, bkDict['EventType'] )
  if not res['OK']:
    return retVal
  histDir = res['NewDir']

  retVal['OK'] = True
  retVal['DestDir'] = histDir

  return retVal

'''
                                                                              
MakeDir:                                                                     
                                                                             
Create a subdirectory in a directory.                                       
                                                                              
'''
def MakeDir( baseDir, dirName ):
  strDirName = str( dirName )

  retVal = {}
  retVal['OK'] = False
  #
  # Store the original workdir.
  #
  cwd = os.getcwd()
  #
  # Cd to the directory containing the new subdirectory and make it unless
  # it exists.
  #
  os.chdir( baseDir )
  if not os.path.exists( '%s' % strDirName ):
    os.mkdir( strDirName )

  if not os.path.exists( '%s' % strDirName ):
    os.chdir( cwd )
    return retVal

  os.chdir( cwd )

  retVal['OK'] = True
  retVal['NewDir'] = '%s/%s' % ( baseDir, strDirName )

  return retVal


'''
                                                                              
 GetStreamHist:                                                               
                                                                              
 Get all unchecked histograms in a given configuration stream.                
                                                                              
'''
def GetStreamHIST( bkDict , bkClient ):
  lfns = {}
  res = bkClient.getFilesWithGivenDataSets( bkDict )
  if not res['OK']:
    gLogger.error( res['Message'] )
    return OutPut

  for lfn in res['Value']:
    lfns[lfn] = True

  return ( lfns, res )


'''
                                                                             
GetProductionId:                                                             
                                                                             
Extract the production id number from the histogram file name.               
                                                                             
'''
def GetProductionId( run, procPass, eventTypeId , bkClient ):
  bkDict = {'Runnumber' : run,
            'ProcPass'  : procPass}
  res = bkClient.getProductiosWithAGivenRunAndProcessing( bkDict )
  retVal = ''
  if not res['OK']:
    outMess = 'Cannot get the production id for run %s proc. pass. %s' % ( 
      run, procPass )
    gLogger.error( outMess )
    gLogger.error( res['Message'] )
    return ( retVal, res )

  prodId = 0
  retVal='0'
  if not len( res['Value'] ):
    outMess = 'Empty production id list for run %s proc. pass. %s' % ( 
      run, procPass )
    gLogger.error( outMess )
    gLogger.error( res )
    retVal='0'
    return ( retVal, res )

  allProdList = res['Value']
  for prodList in allProdList:
    thisProdId = int( prodList[0] )
    res = bkClient.getProductionInformations( thisProdId )
    if not res['OK']:
      outMess = 'Cannot get the information for production %s' % ( thisProdId )
      gLogger.error( outMess )
      gLogger.error( res['Message'] )
      return Output
    if int( res['Value']['Production informations'][0][2] ) == int( eventTypeId ):
      if thisProdId > prodId:
        prodId = thisProdId

  if prodId == 0:
    return ( retVal, res )

  retVal = str( prodId )
  length = len( retVal )

  for i in range( 8 - length ):
    retVal = '0' + retVal

  return ( retVal, res )

'''
                                                                            
GetTargetFile:                                                              
                                                                            
Define the full path of the final histogram file.                           
                                                                            
'''
def GetTargetFile( run, prodId, eventType, destDir ):
  targetFile = '%s/BrunelDaVinci_%s_%s_%s.root' % ( destDir, eventType,
                                                  run, prodId )
  return targetFile


'''
                                                                             
TargetFileExists:                                                            
                                                                             
Check if the histogram output file is already on disk.                       
                                                                             
'''
def TargetFileExists( targetFile, run , homeDir , dirac):
  retVal = False
  castorLFN = re.sub( homeDir, '/lhcb/dataquality', targetFile );
  res = dirac.getReplicas( castorLFN )
  if not res['OK']:
    gLogger.error( "Cannot check castor status of %s" % ( castorLFN ) )
    gLogger.error( res['Message'] )
    return ( retVal, res )

  if res['Value']['Successful'].has_key( castorLFN ):
    gLogger.info( 'Run %s is already available on castor' % ( run ) )
    retVal = True
  return ( retVal, res )

'''
                                                                             
VerifyReconstructionStatus:                                                  
                                                                             
Check enough RAW data have been reconstructed and one and only one output    
is generated for each one of them.                                           
                                                                             
'''
def VerifyReconstructionStatus( run, runData, bkDict, eventType , bkClient , specialMode ):
  retVal = {}
  retVal['OK'] = False
  retVal['BRUNELHIST'] = []
  retVal['DAVINCIHIST'] = []

  rawBkDict = {}
  rawBkDict['EventType'] = bkDict['EventType']
  rawBkDict['ConfigName'] = 'LHCb'
  rawBkDict['ConfigVersion'] = bkDict['ConfigVersion']
  rawBkDict['StartRun'] = bkDict['StartRun']
  rawBkDict['EndRun'] = bkDict['EndRun']
  rawBkDict['FileType'] = 'RAW'
  rawBkDict['ProcessingPass'] = 'Real Data'
  rawBkDict['ReplicaFlag'] = 'All'

  res = bkClient.getFilesWithGivenDataSets( rawBkDict )

  if ( not res['OK'] ) or ( not len( res['Value'] ) ):
    gLogger.error( "Cannot get RAW files for run %s" % ( run ) )
    gLogger.error( res['Message'] )
    return ( retVal, res )

  rawLFN = res['Value']
  countRAW = len( rawLFN )
  countBrunel = len( runData['BRUNELHIST'].keys() )

  #
  # Make sure enough files have been reconstructed in the run.
  #

  if not countBrunel == countRAW:
    if specialMode:
      gLogger.info( "Run %s in pass %s accepted by special mode selection." % ( 
        run, procPass ) )
    else:
      #
      # New 95% or hist = RAW - 1 selection
      #
      if not eventType == "FULL":
        gLogger.info( "Run %s in pass %s is not completed." % ( 
          run, bkDict['ProcessingPass'] ) )
        return Output

      if ( countBrunel < 0.95 * countRAW ) and ( countBrunel < ( countRAW - 1 ) ):
        gLogger.info( "Run %s in pass %s is not completed. Number of RAW = %d" % ( 
          run, bkDict['ProcessingPass'], int( countRAW ) ) )
        return Output

      gLogger.info( "Run %s in pass %s accepted by -1 or 0.95 selection: Number of BRUNEL hist = %d Number of RAW = %d" % ( 
        run, bkDict['ProcessingPass'], countBrunel, countRAW ) )

  #
  # Make sure the RAW have one and one only BRUNELHIST and
  # DAVINCIHIST descendant
  #

  missing = {'BRUNELHIST'  : [],
             'DAVINCIHIST' : []}

  for raw in sortList( rawLFN ):
    res = DescendantIsDownloaded( raw, runData , bkClient )
    if not res['OK']:
      gLogger.info( "LFN %s in Run %s has too many hist descendants in processing pass %s" % ( 
                   raw, run, bkDict['ProcessingPass'] ) )
      return ( retVal, res )

    if len( res['BRUNELHIST'] ) > 1:
      gLogger.info( "LFN %s in Run %s has %s BRUNELHIST in processing pass %s" % ( 
                   raw, run, len( res['BRUNELHIST'] ), bkDict['ProcessingPass'] ) )
      return ( retVal, res )
    elif len( res['BRUNELHIST'] ) == 0:
      missing['BRUNELHIST'].append( raw )
      continue

    if len( res['DAVINCIHIST'] ) > 1:
      gLogger.info( "LFN %s in Run %s has %s DAVINCIHIST in processing pass %s" % ( 
                   raw, run, len( res['DAVINCIHIST'] ), bkDict['ProcessingPass'] ) )
      return ( retVal, res )
    elif len( res['DAVINCIHIST'] ) == 0:
      missing['DAVINCIHIST'].append( raw )
      continue

    brunelHist = res['BRUNELHIST'][0]
    daVinciHist = res['DAVINCIHIST'][0]

    retVal['BRUNELHIST'].append( brunelHist )
    retVal['DAVINCIHIST'].append( daVinciHist )

  if not len( retVal['BRUNELHIST'] ) == countBrunel:
    gLogger.info( "Run %s processing pass %s found %s BRUNELHIST expected %s" % ( 
      run, bkDict['ProcessingPass'], len( retVal['BRUNELHIST'] ), countBrunel ) )
    return ( retVal, res )

  retVal['OK'] = True
  return ( retVal, res )

'''
                                                                            
DescendantIsDownloaded:                                                     
                                                                            
Check if at least one descendant of a given event type has been downloaded. 
                                                                             
'''
def DescendantIsDownloaded( rawLFN, runData , bkClient ):
  retVal = {}
  retVal['OK'] = True
  retVal['BRUNELHIST'] = []
  retVal['DAVINCIHIST'] = []
  descList={}
  (descList ,res )= GetDescendants( rawLFN , bkClient )
  if not res['OK']:
    return res
  

  for descName in descList:
    if runData['BRUNELHIST'].has_key( descName ):
      retVal['BRUNELHIST'].append( descName )
    if runData['DAVINCIHIST'].has_key( descName ):
      retVal['DAVINCIHIST'].append( descName )

  if ( len( retVal['BRUNELHIST'] ) > 1 ) or ( len( retVal['DAVINCIHIST'] ) > 1 ):
    retVal['OK'] = False

  return retVal


'''

GetDescendants: Get the list of descendants of a raw file in a given stream.                 
                                                                              
'''
def GetDescendants( rawLFN , bkClient ):
  descLFN = []

  res = bkClient.getAllDescendents( [rawLFN], 5, 0, True )

  if not res['OK']:
    gLogger.error( res['Message'] )
    return ( descLFN, res )

  if res['Value'].has_key( 'Successful' ):
    if res['Value']['Successful'].has_key( rawLFN ):
      for lfn in res['Value']['Successful'][rawLFN]:
        descLFN.append( lfn )
  return ( descLFN, res )

'''
                                                                             
Merge:                                                                       
                                                                             
Merge all root files into one.                                               
                                                                             
'''
def Merge( targetFile, runNumber, brunelHist, daVinciHist , mergeExeDir ,
           mergeStep1Command , mergeStep2Command, mergeStep3Command , specialMode,testMode,
           castorHistPre, castorHistPost , homeDir , workDir, brunelCount , daVinciCount , logFile ,logFileName , dirac):
  retVal = {}
  retVal['OK'] = False
  retVal['Merged'] = False

  #
  # Get the current dir
  #
  cwd = os.getcwd()

  #
  # First step in Brunel and DaVinci merges the bulk of the files.
  # Second step merges the files previously merged/
  #
  brunelStep1 = MergeStep1( brunelHist, 'Brunel', mergeStep1Command, runNumber ,
                            castorHistPre, castorHistPost  , workDir,
                            brunelCount , daVinciCount , logFile  , logFileName)
  StreamToLog(logFileName,gLogger,mergeStep1Command)

  if not brunelStep1['OK']:
    MergeCleanUp( brunelStep1['step2Hist'], "", [], "" )
    os.chdir( cwd )
    return retVal
  dim = len( brunelHist )
  
  brunelStep2 = MergeStep2( brunelStep1['step2Hist'],
                           'Brunel', mergeStep2Command, dim , workDir , logFile , logFileName )

  StreamToLog(logFileName,gLogger,mergeStep2Command)

  if not brunelStep2['OK']:
    MergeCleanUp( brunelStep1['step2Hist'], "", [], "" )
    os.chdir( cwd )
    return retVal
  brunelFile = brunelStep2['file']

  daVinciStep1 = MergeStep1( daVinciHist, 'DaVinci', mergeStep1Command, runNumber ,
                             castorHistPre, castorHistPost  , workDir,
                             brunelCount , daVinciCount , logFile , logFileName)
  
  StreamToLog(logFileName,gLogger,mergeStep1Command)
  
  if not daVinciStep1['OK']:
    MergeCleanUp( brunelStep1['step2Hist'], brunelFile,
                 daVinciStep1['step2Hist'], "" )
    os.chdir( cwd )
    return retVal

  dim = len( daVinciHist )
  daVinciStep2 = MergeStep2( daVinciStep1['step2Hist'],
                            'DaVinci', mergeStep2Command, dim , workDir , logFile , logFileName)

  StreamToLog(logFileName,gLogger,mergeStep2Command)

  if not daVinciStep2['OK']:
    MergeCleanUp( brunelStep1['step2Hist'], brunelFile,
                 daVinciStep1['step2Hist'], "" )
    os.chdir( cwd )
    return retVal
  daVinciFile = daVinciStep2['file']

  #
  # Put Brunel and DaVinci in the same file.
  #

  merge = [brunelFile, daVinciFile]
  merge.insert( 0, targetFile )
  merge.insert( 0, mergeStep3Command )
  p = subprocess.call( merge, stdout = logFile )
  gLogger.info("=== Final Merging OutPut")
  StreamToLog(logFileName,gLogger,mergeStep3Command)

  if not p == 0:
    if os.path.isfile( targetFile ):
      os.remove( targetFile )
    os.chdir( cwd )
    return retVal

  #
  # Clean up the temporary files.
  #

  MergeCleanUp( brunelStep1['step2Hist'], brunelFile,
               daVinciStep1['step2Hist'], daVinciFile )

  #
  # Upload the hist file to castor
  #

  if not testMode:
    castorLFN = re.sub( homeDir, '/lhcb/dataquality', targetFile );
    res = dirac.addFile( castorLFN, targetFile, 'CERN-HIST' )
    if not res['OK']:
      gLogger.error( 'Error uploadding %s to %s. Error is: %s' % ( 
        targetFile, castorLFN, res['Message'] ) )
      return retVal

  #
  # Cd to the original workdir.
  #
  os.chdir( cwd )

  retVal['OK'] = True
  retVal['Merged'] = True

  return retVal

'''
                                                                             
MergeStep1                                                                   
                                                                             
Step 1 in the merging of root files.                                         
                                                                             
'''
def MergeStep1( stepHist, histType, mergeStep1Command, runNumber,
                castorHistPre, castorHistPost , workDir ,
                brunelCount , daVinciCount , logFile , logFileName):
  
  retVal = {}
  retVal['OK'] = False
  retVal['step2Hist'] = []

  splitBy = 50
  nStep = int( len( stepHist ) / splitBy ) + 1
 
  count = 0
  
  for i in range( 0, nStep ):
    first = i * splitBy
    last = ( i + 1 ) * splitBy

    mergeLFN = stepHist[first:last]
    if not len( mergeLFN ):
      break

    merge = []
    for lfn in mergeLFN:
      filename = '%s%s%s' % ( castorHistPre, lfn, castorHistPost )
      merge.append( filename )

    if histType == 'Brunel':
      targetFile = '%s/%s_%s_%s.root' % ( workDir, histType,
                                        os.getpid(), str( brunelCount ) )
      brunelCount += 1
    elif histType == 'DaVinci':
      targetFile = '%s/%s_%s_%s.root' % ( workDir, histType,
                                        os.getpid(), str( daVinciCount ) )
      daVinciCount += 1

    merge.insert( 0, targetFile )
    merge.insert( 0, mergeStep1Command )
    p = subprocess.call( merge, stdout = logFile )
    StreamToLog(logFileName,gLogger,mergeStep1Command)
    
    if not p == 0:
      gLogger.error("Histograms merging failed for run %s at command: %s" %(runNumber, ' '.join[merge]))
      if os.path.isfile( targetFile ):
        os.remove( targetFile )
      return retVal
    retVal['step2Hist'].append( targetFile )

  retVal['OK'] = True
  return retVal


################################################################################
#                                                                              #
# MergeStep2                                                                   #
#                                                                              #
# Step 2 in the merging of root files.                                         #
#                                                                              #
################################################################################

def MergeStep2( stepHist, histType,mergeStep2Command ,dim,
                workDir , logFile , logFileName):
  retVal = {}
  retVal['OK'] = False
  retVal['file'] = []

  targetFile = '%s/%s_%s.root' % ( workDir, histType, os.getpid() )
  merge = stepHist[0:len(stepHist)]
  merge.insert( 0, str( dim ) )
  merge.insert( 0, targetFile )
  merge.insert( 0, mergeStep2Command )
  p = subprocess.call( merge, stdout = logFile )
  StreamToLog(logFileName,gLogger,mergeStep2Command)
  if not p == 0:
    if os.path.isfile( targetFile ):
      os.remove( targetFile )
    return retVal

  retVal['OK'] = True
  retVal['file'] = targetFile
  return retVal




'''
                                                                             
MergeCleanUp                                                                 
                                                                             
Remove all the temporary merge files.                                        
                                                                             
'''
def MergeCleanUp( brunelStep2, brunelFile, daVinciStep2, daVinciFile ):
  for mergeFile in brunelStep2:
    os.unlink( mergeFile )
  if len( brunelFile ):
    os.unlink( brunelFile )

  for mergeFile in daVinciStep2:
    os.unlink( mergeFile )
  if len( daVinciFile ):
    os.unlink( daVinciFile )
  return

'''
                                                                             
IsRunFlagged:                                                                
                                                                             
Given a run and a processing pass check if it is flagged.                    
                                                                              
'''
def IsRunFlagged( run, eventType, procPass , bkClient ):
  res = bkClient.getProcessingPassId( procPass )
  if not res['OK']:
     gLogger.error( res['Message'] )
     return res

  procPassId = res['Value']

  res = bkClient.getRunFlag( run, procPassId )
  if not res['OK']:
     return False
  dqFlag = res['Value']
  if dqFlag == 'BAD':
     return True
  elif dqFlag == 'UNCHECKED':
     return False
  elif dqFlag == 'EXPRESS_OK':
     if eventType == 'FULL':
        return False
     elif eventType == 'EXPRESS':
        return True
  elif dqFlag == 'OK':
     return True

  return False


def StreamToLog(stream,gLogger,exeName):
  try:
    file = open(stream, "rb")
  except IOError:
    gLogger.error("Log file for %s doesn't exist" % exeName)
    return
  for line in file:
    if not line:
      break
    gLogger.info(line)