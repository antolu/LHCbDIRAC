import DIRAC
from   DIRAC                                                 import S_OK, S_ERROR, gLogger
from   DIRAC.Core.Utilities.List                             import sortList
from   DIRAC.Interfaces.API.Dirac import Dirac
from   DIRAC.Core.Utilities.File  import *

#Libraries needed for the Upload
from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from DIRAC.DataManagementSystem.Client.FailoverTransfer import FailoverTransfer
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager

#System libraries
import subprocess
from subprocess import PIPE
import os,sys
import re,string,time,glob

#Libraries needed for XML report
from xml.dom.minidom                         import Document, DocumentType

'''
GetRuns retrieve the list of files that correspond to the query bkDict. After that they are grouped per run.
The output is a dictionary such as for example:

results_ord = {'BRUNELHIST':{1234:
                                  {'DataQuality' : 'OK',
                                   'LFNs'        :['/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017703_1_Hist.root', 
                                                   '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017894_1_Hist.root', 
                                                   '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017749_1_Hist.root', 
                                                   '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017734_1_Hist.root', 
                                                   '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017810_1_Hist.root', 
                                                   '/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017838_1_Hist.root'
                                                   ]
                                  }
                            1234:
                                  {'DataQuality' : 'OK',
                                   'LFNs'        :['/lhcb/LHCb/Collision11/HIST/00012362/0001/Brunel_00012362_00017703_1_Hist.root', 
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

def GetRuns(bkDict,bkClient):
    results={}
    results_ord={}
    results = bkClient.getFilesWithGivenDataSetsForUsers(bkDict)
    gLogger.debug("Called bkClient method getFilesWithGivenDataSetsForUsers")
    gLogger.debug("with bk query %s"%str(bkDict))
    if not results['OK']:
      gLogger.debug("Failed to retrieve dataset. Result is %s"%str(results))
      return results_ord 
    ID = bkClient.getProcessingPassId(bkDict[ 'ProcessingPass' ])
    for l in  results['Value']['LFNs']:
      r = results['Value']['LFNs'][l]['Runnumber']
      if not results_ord.has_key(r):
          results_ord[r]={}
          results_ord[r]['LFNs']=[]
          results_ord[r]['DataQuality']='None'
          q = bkClient.getRunFlag( r, ID['Value'] )
          if q['OK']:
              results_ord[r]['DataQuality']=q['Value']
              results_ord[r]['LFNs'].append(l)
      else:
          results_ord[r]['LFNs'].append(l)
    return results_ord



'''
                                                                              
 MergeRun: Merge the BRUNELHIST and DAVINCIHIST of a particular run in three steps.
 
 1 - Group BRUNELHISTs and DAVINCIHISTs in groups of 50;
 2 - Merge the output of step 1; 
 3 - Merge the output of step 2.
'''
def MergeRun( bkDict, res_0, res_1, run, bkClient , homeDir , testMode , specialMode , 
              mergeExeDir , mergeStep1Command, mergeStep2Command, mergeStep3Command,
              brunelCount , daVinciCount , logFile , logFileName ,environment):

  results={}
  results['Merged']=False
  
  rm = ReplicaManager()
  
  dirac = Dirac()

  procPass = bkDict['ProcessingPass']
  dqFlag = bkDict['DataQualityFlag']
  dtd = bkDict['DataTakingConditions']
  eventTypeId = bkDict['EventTypeId']
  eventType = bkDict['EventTypeDescription']
  runData = {}
  brunelList=res_0[run]['LFNs']
  davinciList=res_1[run]['LFNs']
  runData['BRUNELHIST']=brunelList
  runData['DAVINCIHIST']=davinciList
  
  for histType in ['BRUNELHIST','DAVINCIHIST']:
    if len( runData[histType]):
      outMess = "There are %d %s %s files in stream %s, processing pass %s for run %s." % (len( runData[histType]), dqFlag, histType, eventType, procPass, str(run) )
      gLogger.info( outMess )
    else:
      gLogger.info( "There are no %s %s files in stream %s, processing pass %s for run %s." % (dqFlag, histType, eventType, procPass, str(run) ) )
      results['OK'] = False
      return results
  #
  # Make sure the same number of histogram files is available
  # for DaVinci and Brunel
  #
  countBrunel = len( runData['BRUNELHIST'])
  countDaVinci = len( runData['DAVINCIHIST'])
  
  if not countBrunel == countDaVinci:
    gLogger.info( "Run %s in pass %s has different number of Brunel and DaVinci hist: %d vs. %d." % ( 
      str(run), procPass, countBrunel, countDaVinci ) )
    results['OK'] = False
    return results
  if countBrunel == 0:
    results['OK'] = False
    return results

  targetFile = GetTargetFile( run, homeDir  )
  
  #Redundancy check of existence
  retVal = os.path.exists(targetFile) 

  if retVal:
    results['OK']=False
    results['Message']='%s already merged' %targetFile
    return results
  #
  # Check if enough files have been reconstructed
  #
  retVal, res = VerifyReconstructionStatus( run, runData, bkDict, eventType ,
                                            bkClient , specialMode )
  if not res['OK']:
    results['OK']=False
    return results

  gLogger.info( "Now processing run %s in pass %s." % ( 
  str(run), bkDict['ProcessingPass'] ) )
  
  brunelLocal=[]
  davinciLocal=[]
  gLogger.info('===>Retrieving Brunel histograms locally')
  for lfn in brunelList:
    res = rm.getFile(lfn,homeDir)
    if res['OK']:
      brunelLocal.append(res['Value']['Successful'][lfn])

  gLogger.info('===>Retrieving DaVinci histograms locally')
  for lfn in davinciList:
    res = rm.getFile(lfn,homeDir)
    if res['OK']:
      davinciLocal.append(res['Value']['Successful'][lfn])

  '''
  Real Merging part 
  '''
  res = Merge( targetFile, run, brunelLocal , davinciLocal , mergeExeDir ,
               mergeStep1Command, mergeStep2Command, mergeStep3Command,
               specialMode,testMode,homeDir,brunelCount , daVinciCount , 
               logFile , logFileName , dirac , environment)
  if res['OK']:
    results['Merged']=True
    results['OutputFile']=targetFile
    return results

'''
                                                                            
GetTargetFile:                                                              
                                                                            
Define the full path of the final histogram file.                           
                                                                            
'''
def GetTargetFile( run , homeDir ):
  targetFile = '%sBrunel_DaVinci_run_%s.root' % (homeDir,run)
  return targetFile


'''
                                                                             
VerifyReconstructionStatus:                                                  
                                                                             
Check that enough RAW data have been reconstructed and one and only one output    
is generated for each one of them.                                           
                                                                             
'''
def VerifyReconstructionStatus( run, runData, bkDict, eventType , bkClient , specialMode ):

  retVal = {}
  retVal['OK'] = False
  retVal['BRUNELHIST'] = []
  retVal['DAVINCIHIST'] = []
  

  rawBkDict = {}
  rawBkDict['EventType'] = bkDict['EventTypeId']
  rawBkDict['ConfigName'] = 'LHCb'
  rawBkDict['ConfigVersion'] = bkDict['ConfigVersion']
  rawBkDict['StartRun'] = run
  rawBkDict['EndRun'] = run
  rawBkDict['FileType'] = 'RAW'
  rawBkDict['ProcessingPass'] = 'Real Data'
  rawBkDict['ReplicaFlag'] = 'All'
     
  logger = gLogger
  logger.setLevel("info")
  logger.info("=====VerifyReconstructionStatus=====")
  logger.info(str(rawBkDict))
  logger.info("====================================")

  gLogger.setLevel('debug')
  res = bkClient.getFilesWithGivenDataSets( rawBkDict )

  if ( not res['OK'] ) or ( not len( res['Value'] ) ):
    gLogger.error( "Cannot get RAW files for run %s" % str(run) )
    gLogger.error( res['Message'] )
    return ( retVal, res )

  rawLFN = res['Value']
  countRAW = len( rawLFN )
  countBrunel = len( runData['BRUNELHIST'])

  #
  # Make sure enough files have been reconstructed in the run.
  #

  if not countBrunel == countRAW:
    if specialMode:
      gLogger.info( "Run %s in pass %s accepted by special mode selection." % (str(run), rawBkDict['ProcessingPass'] ) )
    else:
      #
      # New 95% or hist = RAW - 1 selection
      #
      if not eventType == "FULL":
        gLogger.info( "Run %s in pass %s is not completed." % ( 
          run, bkDict['ProcessingPass'] ) )
        return retVal

      if ( countBrunel < 0.95 * countRAW ) and ( countBrunel < ( countRAW - 1 ) ):
        gLogger.info( "Run %s in pass %s is not completed. Number of RAW = %d" % ( 
          run, bkDict['ProcessingPass'], int( countRAW ) ) )
        return retVal

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
                   raw, str(run), bkDict['ProcessingPass'] ) )
      return ( retVal, res )

    if len( res['BRUNELHIST'] ) > 1:
      gLogger.info( "LFN %s in Run %s has %s BRUNELHIST in processing pass %s" % ( 
                   raw, str(run), len( res['BRUNELHIST'] ), bkDict['ProcessingPass'] ) )
      return ( retVal, res )
    elif len( res['BRUNELHIST'] ) == 0:
      missing['BRUNELHIST'].append( raw )
      continue

    if len( res['DAVINCIHIST'] ) > 1:
      gLogger.info( "LFN %s in Run %s has %s DAVINCIHIST in processing pass %s" % ( 
                   raw, str(run), len( res['DAVINCIHIST'] ), bkDict['ProcessingPass'] ) )
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
      str(run), bkDict['ProcessingPass'], len( retVal['BRUNELHIST'] ), countBrunel ) )
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
    if descName in runData['BRUNELHIST'] :
      retVal['BRUNELHIST'].append( descName )
    if descName in runData['DAVINCIHIST']:
      retVal['DAVINCIHIST'].append( descName )

  if ( len( retVal['BRUNELHIST'] ) > 1 ) or ( len( retVal['DAVINCIHIST'] ) > 1 ):
    retVal['OK'] = False

  return retVal


'''

GetDescendants: Get the list of descendants of a raw file in a given stream.                 
                                                                              
'''
def GetDescendants( rawLFN , bkClient ):
  descLFN = []

  res = bkClient.getDescendents( rawLFN, 5)

  if not res['OK']:
    gLogger.error("Unable to retrieve descendants for RAW %s"%rawLFN)
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
def Merge( targetFile, runNumber, brunelHist, daVinciHist , mergeExeDir ,mergeStep1Command , mergeStep2Command, 
           mergeStep3Command,specialMode,testMode, homeDir , brunelCount , daVinciCount ,logFile ,logFileName , 
           dirac , environment):
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
  gLogger.verbose("===Brunel Step1===")
  gLogger.verbose(brunelHist)
  gLogger.verbose("==================")
  
  brunelStep1 = MergeStep1( brunelHist, 'Brunel', mergeStep1Command, runNumber , homeDir,
                            brunelCount, daVinciCount, logFile, logFileName, environment)
  StreamToLog(logFileName,gLogger,mergeStep1Command)
  
  

  if not brunelStep1['OK']:
    MergeCleanUp( brunelStep1['step2Hist'], "", [], "" )
    os.chdir( cwd )
    return retVal
  dim = len( brunelHist )
  
  gLogger.verbose("===Brunel Step2===")
  gLogger.verbose(brunelStep1['step2Hist'])
  gLogger.verbose("==================")

  
  brunelStep2 = MergeStep2( brunelStep1['step2Hist'],
                           'Brunel', mergeStep2Command, dim , homeDir , logFile , logFileName,environment )

  StreamToLog(logFileName,gLogger,mergeStep2Command)

  if not brunelStep2['OK']:
    MergeCleanUp( brunelStep1['step2Hist'], "", [], "" )
    os.chdir( cwd )
    return retVal
  #input of final step
  brunelFile = brunelStep2['file']

  gLogger.verbose("===DaVinci Step1===")
  gLogger.verbose(daVinciHist)
  gLogger.verbose("===================")

  
  daVinciStep1 = MergeStep1( daVinciHist, 'DaVinci', mergeStep1Command, runNumber ,
                             homeDir,
                             brunelCount , daVinciCount , logFile , logFileName,environment)
  
  StreamToLog(logFileName,gLogger,mergeStep1Command)
  
  if not daVinciStep1['OK']:
    MergeCleanUp( brunelStep1['step2Hist'], brunelFile,
                 daVinciStep1['step2Hist'], "" )
    os.chdir( cwd )
    return retVal

  dim = len( daVinciHist )
  gLogger.verbose("===DaVinci Step2===")
  gLogger.verbose(daVinciStep1['step2Hist'])
  gLogger.verbose("===================")
  daVinciStep2 = MergeStep2( daVinciStep1['step2Hist'],
                            'DaVinci', mergeStep2Command, dim , homeDir , logFile , logFileName,environment)

  StreamToLog(logFileName,gLogger,mergeStep2Command)

  if not daVinciStep2['OK']:
    MergeCleanUp( brunelStep1['step2Hist'], brunelFile,
                 daVinciStep1['step2Hist'], "" )
    os.chdir( cwd )
    return retVal
  #input of final step
  daVinciFile = daVinciStep2['file']

  #
  # Put Brunel and DaVinci in the same file.
  #
  merge = [brunelFile, daVinciFile]
  gLogger.verbose("===Step3===")
  gLogger.verbose(str(merge))
  gLogger.verbose("===========")

  merge.insert( 0, targetFile )
  merge.insert( 0, mergeStep3Command )
  command = " ".join(merge)
  p = subprocess.call( args = command, env = environment ,stdout = logFile , shell = True)
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
  # MUST BE IMPLEMENTED
  #
  # Cd to the original workdir.
  #
  os.chdir( cwd )

  retVal['OK'] = True
  retVal['Merged'] = True
  retVal['OutputFile']=targetFile

  return retVal

'''
                                                                             
MergeStep1                                                                   
                                                                             
Step 1 in the merging of root files.                                         
                                                                             
'''
def MergeStep1( stepHist, histType, mergeStep1Command, runNumber, homeDir ,
                brunelCount , daVinciCount , logFile , logFileName, environment):
  
  retVal = {}
  retVal['OK'] = False
  retVal['step2Hist'] = []
  # Split the bunch of files in groups of 50
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
      merge.append(lfn)

    if histType == 'Brunel':
      targetFile = '%s%s_%s_%s.root' % ( homeDir , histType , os.getpid() , str( brunelCount ) )
      gLogger.info("Intermediate file for Brunel Step1 %s" %targetFile)
      brunelCount += 1
    elif histType == 'DaVinci':
      targetFile = '%s%s_%s_%s.root' % ( homeDir , histType , os.getpid() , str( daVinciCount ) )
      gLogger.info("Intermediate file for DaVinci Step1 %s" %targetFile)
      daVinciCount += 1

    merge.insert( 0, targetFile )
    merge.insert( 0, mergeStep1Command )
    command = " ".join(merge)
    p = subprocess.call( args = command, env = environment, stdout = logFile , shell=True)
    gLogger.debug("Executing Step1 command %s" %command)
    StreamToLog(logFileName,gLogger,mergeStep1Command)
    
    if not p == 0:
      gLogger.error("Histograms merging failed for run %s at command: %s" %(str(runNumber), ' '.join[merge]))
      if os.path.isfile( targetFile ):
        os.remove( targetFile )
      return retVal
    retVal['step2Hist'].append( targetFile )
    MergeCleanUp( mergeLFN, "", [], "" )

  gLogger.debug("Step1 Merged histograms %s"%str(retVal['step2Hist']))
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
                homeDir , logFile , logFileName , environment):
  retVal = {}
  retVal['OK'] = False
  retVal['file'] = []

  targetFile = '%s%s_%s.root' % ( homeDir, histType, os.getpid() )
  gLogger.debug("Step2 intermediate file %s" %targetFile)
  merge = stepHist[0:len(stepHist)]
  merge.insert( 0, str( dim ) )
  merge.insert( 0, targetFile )
  merge.insert( 0, mergeStep2Command )
  command = " ".join(merge)
  p = subprocess.call( args = command , env = environment , stdout = logFile , shell=True)
  gLogger.debug("Executing Step2 command %s" %command)
  StreamToLog(logFileName,gLogger,mergeStep2Command)
  if not p == 0:
    gLogger.error("Step2 Failed!")
    if os.path.isfile( targetFile ):
      os.remove( targetFile )
    return retVal

  retVal['OK'] = True
  retVal['file'] = targetFile
  gLogger.debug("Step2 Merged histograms %s"%str(retVal['file']))
  return retVal

'''
                                                                             
MergeCleanUp                                                                 
                                                                             
Remove all the temporary merge files.                                        
                                                                             
'''
def MergeCleanUp( brunelStep2, brunelFile, daVinciStep2, daVinciFile ):
  gLogger.debug("Cleaning intermediate files %s"%str(brunelStep2))
  for mergeFile in brunelStep2:
    os.unlink( mergeFile )
  if len( brunelFile ):
    os.unlink( brunelFile )

  gLogger.debug("Cleaning intermediate files %s"%str(daVinciStep2))
  for mergeFile in daVinciStep2:
    os.unlink( mergeFile )
  if len( daVinciFile ):
    os.unlink( daVinciFile )
  return

'''
Finalization Step:
 1 - Create a XML report containing all the Input Files and the output files (data and log).
 2 - Perform failoverTransfer.transferAndRegisterFile (LFC Upload and Registration)
 3 - From the Bookkeeping XML report previously created a request object is created.
 4 - If the registration in the BK is possible than the file is added to the calalog.
 5 - For the log file all the directory is Uploaded with the replicamanager putStorageDirectory.   
'''
def Finalization(homeDir,logDir,lfns,OutputFileName,LogFileName,inputData,run,bkDict,rootVersion):
  '''
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
  '''
  res={}
  
  Output={}
  Output[lfns['DATA']]={'Filename':'','FileSize':'','MD5Sum':'','Guid':''}
  Output[lfns['LOG']]={'Filename':'','FileSize':'','MD5Sum':'','Guid':''}

  Output[lfns['DATA']]['Filename']=OutputFileName
  Output[lfns['LOG']]['Filename']=logDir+'/'+LogFileName
  
  gLogger.info("Creating XML report")
  #Create the XML Bookkeeping Report
  res = makeBookkeepingXML( Output, lfns['DATA'], lfns['LOG'] , inputData, run , bkDict[ 'ConfigName' ] , bkDict[ 'ConfigVersion' ] , homeDir ,rootVersion, saveOnFile=True )
  
  logDict={'logdir':logDir,'logSE':'LogSE','logFilePath':lfns['LOGDIR'],'logLFNPath':lfns['LOG']}
  
  gLogger.verbose(str(homeDir))
  gLogger.verbose(str(OutputFileName))
  gLogger.verbose(str(lfns['DATA']))
  gLogger.verbose(res['XML'])
  gLogger.verbose(str(logDict))
  
  #Uploading data and logs
  results = UpLoadOutputData(homeDir,OutputFileName,lfns['DATA'],res['XML'],logDict)
  
  return res
'''
Build LFN for data and logfile 
'''
def BuildLFNs(bkDict,run):
  lfns={}
  lfns['DATA'] = '/lhcb/LHCb/' + bkDict[ 'ConfigVersion' ] + '/HIST/' + str(run) + '/Brunel_DaVinci_'+str(run)+'_Hist.root' 
  lfns['LOG'] = '/lhcb/LHCb/' + bkDict[ 'ConfigVersion' ] + '/LOG/MERGEDDQ/' + str(run) + '/Brunel_DaVinci_'+str(run)+'_Hist.log'
  lfns['LOGDIR'] = '/lhcb/LHCb/' + bkDict[ 'ConfigVersion' ] + '/LOG/MERGEDDQ/' + str(run)
  return lfns

'''
Redirection of a stream to gLogger
'''
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

    
'''
Utilies for XML Report
'''
    

def addChildNode( parentNode, tag, returnChildren, *args ):
  '''
  Params
  :parentNode:
  node where the new node is going to be appended
  :tag: 
  name if the XML element to be created
  :returnChildren:
  flag to return or not the children node, used to avoid unused variables
  :*args:
  possible attributes of the element 
'''
  ALLOWED_TAGS = [ 'Job', 'TypedParameter', 'InputFile', 'OutputFile','Parameter', 'Replica']

  def genJobDict( configName, configVersion, ldate, ltime ):
    return {
        "ConfigName"   : configName,
        "ConfigVersion": configVersion,
        "Date"         : ldate,
        "Time"         : ltime
        }
  def genTypedParameterDict( name, value , type  ):
    return {
        "Name"  : name,
        "Value" : value,
        "Type"  : type
        }
  def genInputFileDict( name ):
    return {
        "Name" : name
        }
  def genOutputFileDict( name, typeName, typeVersion ):
    return {
    "Name"        : name,
    "TypeName"    : typeName,
    "TypeVersion" : typeVersion
        }
  def genParameterDict( name, value ):
    return {
        "Name"  : name,
        "Value" : value
        }
  def genReplicaDict( name, location = "Web" ):
    return {
        "Name"     : name,
        "Location" : location
        }
  if not tag in ALLOWED_TAGS:
    # We can also return S_ERROR, but this let's the job keep running.
    dict = {}
  else:
    dict = locals()[ 'gen%sDict' % tag ]( *args )

  childNode = Document().createElement( tag )
  for k, v in dict.items():
    childNode.setAttribute( k, str( v ) )
  parentNode.appendChild( childNode )

  if returnChildren:
    return ( parentNode, childNode )
  return parentNode
  
def generateJobNode( doc ,configName , configVersion , ldate , ltime):

  ''' Node looks like
      <Job ConfigName="" ConfigVersion="" Date="" Time="">
  '''
  jobAttributes = ( configName, configVersion, ldate, ltime )
  return addChildNode( doc, "Job", 1, *jobAttributes )

def generateInputFiles( jobNode , inputData):
  ''' InputData looks like this
      <InputFile Name=""/>
  '''
  if inputData:
    for inputname in inputData:
          jobNode = addChildNode( jobNode, "InputFile", 0, inputname )
  return jobNode


def generateOutputFiles( jobNode , Output , outputlfn , logFilelfn , outputDataType , outputDataVersion, configName , configVersion ,run , homeDir):
  
  '''OutputFile looks like this:  
  
         <OutputFile Name="" TypeName="" TypeVersion="">
           <Parameter Name="" Value=""/>
           ...
           <Replica Location="" Name=""/>
           ....
         </OutputFile>

        What this exactly does, it is a mystery to me.
  '''

  outputs = []
  outputs.append((outputlfn, outputDataType, outputDataVersion))
  outputs.append((logFilelfn, ( 'LOG' ), ( '1' ) ))
  log = gLogger.getSubLogger( "BookkeepingReport" )
  log.info( outputs )
  
  for output, outputtype, outputversion in list( outputs ):
    log.info( 'Looking at output %s %s %s' % ( output, outputtype, outputversion ) )
    typeName = outputtype.upper()
    typeVersion = '1'

    # Add Output to the XML file
    oFileAttributes = ( output, outputtype , outputversion)
    jobNode , oFile = addChildNode( jobNode, "OutputFile", 1, *oFileAttributes )
    
    if not re.search('log',output):
      filesize = 0
      #filesize = str( os.path.getsize( output ) ) 
      oFile = addChildNode( oFile, "Parameter", 0, *( "FileSize", Output[output]['FileSize'] ) )  
    else:
      logurl = 'http://lhcb-logs.cern.ch/storage/lhcb'
      url = logurl + '/' + configName + '/' + configVersion + '/' + 'LOG' + '/' + Output[output]['Filename'][len(homeDir):]
      #Log file replica information
      oFile = addChildNode( oFile, "Replica", 0, url )
    oFile = addChildNode( oFile, "Parameter", 0, *( "MD5Sum", Output[output]['MD5Sum'] ) )
    oFile = addChildNode( oFile, "Parameter", 0, *( "Guid", Output[output]['Guid'] ) )
  return jobNode

def generateTypedParams( jobNode ,run , rootVersion , append_string):
  
  typedParams = []
  typedParams.append( ( "JobType", "DQHISTOMERGING" ,"Info") )
  typedParams.append( ( "Name",str(run)+append_string ,"Info") )
  typedParams.append( ( "NumberOfEvents","0" ,"Info") )
  typedParams.append( ( "ProgramName", "ROOT" ,"Info") )
  typedParams.append( ( "ProgramVersion", rootVersion,"Info" ) )
  
  for typedParam in typedParams:
    jobNode = addChildNode( jobNode, "TypedParameter", 0, *typedParam )
    
  return jobNode

def makeBookkeepingXML( Output , outputlfn , logFilelfn , inputData,  run , configName , configVersion , homeDir ,rootVersion , saveOnFile=True ):
  
  ''' Bookkeeping xml looks like this:
  
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
  '''
  '''
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
  '''
  logger = gLogger
  logger.setLevel("info")
  
  logger.info("Making XML report ")
  
  res={}
  res['OK']=True
  res['XML']=''
  
  ldate = time.strftime( "%Y-%m-%d", time.localtime( time.time() ) )
  ltime = time.strftime( "%H:%M", time.localtime( time.time() ) )
  
  l = time.localtime( time.time() ).tm_sec
  append_string='_'+str(l)
  
  for lfn in Output.keys():
    if os.path.exists( Output[lfn]['Filename'] ):
      Output[lfn]['FileSize']= str(getSize(Output[lfn]['Filename']))
      Output[lfn]['MD5Sum']= getMD5ForFiles([Output[lfn]['Filename']])
      Output[lfn]['Guid']= makeGuid(Output[lfn]['Filename'])
    else:
      gLogger.error( 'File %s not found. Cannot write bookkeeping XML summary'%Output[lfn]['Filename'])
      DIRAC.exit( 2 )
  
  # Generate XML document
  doc = Document()
  docType = DocumentType( "Job" )
  docType.systemId = "book.dtd"
  doc.appendChild( docType )

  # Generate JobNodegenerateJobNode( doc ,configName , configVersion , ldate , ltime):
  doc, jobNode =generateJobNode( doc , configName , configVersion , ldate , ltime)

  jobNode = generateTypedParams( jobNode ,run ,rootVersion , append_string)
  # Generate InputFiles
  jobNode = generateInputFiles( jobNode ,inputData)

  jobNode = generateOutputFiles( jobNode , Output , outputlfn , logFilelfn , 'MERGEFORDQ.ROOT' , 'ROOT' , configName , configVersion, run , homeDir)

  prettyXMLDoc = doc.toprettyxml( indent = "    ", encoding = "ISO-8859-1" )

  #horrible, necessary hack!
  prettyXMLDoc = prettyXMLDoc.replace( '\'book.dtd\'', '\"book.dtd\"' )

  if saveOnFile:
    
    bfilename = homeDir+'bookkeeping_Merge_For_DQ_' +str(run)+ '.xml'
    res['XML']=bfilename
    logger.info("Saving XML report %s"%bfilename)
    bfile = open( bfilename, 'w' )
    print >> bfile, prettyXMLDoc
    bfile.close()
  else:
    print prettyXMLDoc
   
  return res

'''
Given an LFN and a request, a registration request for the Bookkeeping Catalog is sent. 
'''
def setBKRegistrationRequest( lfn,request, error = '' ):
  """
  Set a BK registration request for changing the replica flag.  Uses the
  global request object.
  """
  if error:
    gLogger.error('BK registration for %s failed with message: "%s" setting failover request' % ( lfn, error )) 
  else:
    gLogger.info( 'Setting BK registration request for %s' % ( lfn ) )

  result = request.addSubRequest( {'Attributes':{'Operation':'registerFile', 'ExecutionOrder':2, 'Catalogue':'BookkeepingDB'}}, 'register' )
  if not result['OK']:
    gLogger.error('Could not set registerFile request:\n%s' % result) 
    return result['OK']
  fileDict = {'LFN':lfn, 'Status':'Waiting'}
  index = result['Value']
  request.setSubRequestFiles( index, 'register', [fileDict] )
  return result

def UpLoadOutputData(localpath,localfilename,lfn,XMLBookkeepingReport,logDict):
  '''
  *localpath is the path where the file are in the local machine.
  *XMLBookkeepingReport is an XML created previously containing all infos about what will be uploaded.
  
  '''
  os.chdir(localpath)

  bkClient = BookkeepingClient()
  request = RequestContainer()
  request.setRequestName(XMLBookkeepingReport)
  failoverTransfer = FailoverTransfer(request)
  performBKRegistration = []
  registrationFailure = False
  
  #Registration in the LFC
  result = failoverTransfer.transferAndRegisterFile( localpath,localfilename, lfn ,['CERN-HIST'],fileGUID = None,fileCatalog = 'LcgFileCatalogCombined' )
  performBKRegistration.append( lfn )
  bkFileExtensions = ['bookkeeping*.xml']
  log = gLogger.getSubLogger( "UploadOutputData" )
  bkFiles = []
  for ext in bkFileExtensions:
    log.debug( 'Looking at BK record wildcard: %s' % ext )
    globList = glob.glob( ext )
  #Look for bookkeeping XML summaries
  for check in globList:
    if os.path.isfile( check ):
      log.info( 'Found locally existing BK file record: %s' % check )
      bkFiles.append( check )
  #result={}
  #return result
  bkFiles.sort()
  log.info("bkfiles")
  log.info(str(bkFiles))
  for bkFile in bkFiles:
    fopen = open( bkFile, 'r' )
    bkXML = fopen.read()
    fopen.close()
    result = bkClient.sendBookkeeping( bkFile, bkXML )
    if result['OK']:
      log.info( 'Bookkeeping report sent for %s' % bkFile )
    else:
      log.error( 'Could not send Bookkeeping XML file to server, preparing DISET request for', bkFile )
      request.setDISETRequest( result['rpcStub'], executionOrder = 0 )
  #Can now register the successfully uploaded files in the BK i.e. set the BK replica flags
  if not performBKRegistration:
    log.info( 'There are no files to perform the BK registration for, all could be saved to failover' )
    return result
  elif registrationFailure:
    log.info( 'There were catalog registration failures during the upload of files for this job, BK registration requests are being prepared' )
    for lfn in performBKRegistration:
      error=''
      result = setBKRegistrationRequest( lfn , request ,error)
      log.info(str(result))
      if not result['OK']:
        return result
      
  else:
    from DIRAC.DataManagementSystem.Client.ReplicaManager import ReplicaManager
    rm = ReplicaManager()
    result = rm.addCatalogFile( performBKRegistration, catalogs = ['BookkeepingDB'] )
    log.verbose( result )
    if not result['OK']:
      log.err( 'Could Not Perform BK Registration' )
      return result 
    if result['Value']['Failed']:
      for lfn, error in result['Value']['Failed'].items():
        result = setBKRegistrationRequest( lfn,request,error)
        log.verbose(str(result))
        if not result['OK']:
          return result
    else:
      #if BK registration gone fine upload log file as well.
      log.verbose("LogUpload results")
      log.verbose({logDict['logFilePath']:os.path.realpath( logDict['logdir'] )})
      log.verbose(str(logDict['logSE']))
      res = rm.putStorageDirectory({logDict['logFilePath']:os.path.realpath( logDict['logdir'] )}, logDict['logSE'], singleDirectory = True )
      log.verbose(str(res))  
         
  return result



