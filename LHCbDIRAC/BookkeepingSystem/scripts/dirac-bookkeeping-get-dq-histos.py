from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from DIRAC.Core.Utilities.List import sortList
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
import os
from DIRAC import gLogger
from DIRAC.Interfaces.API.Dirac import Dirac


def getStreamRAW(ID):
  """ This performs a bookkeeping query to obtain the EXPRESS/FULL stream RAW files that are in the DataQuality status 'UNCHECKED'.
      This will get you the files for the runs that have not yet been checked.

      returns a list of RAW files.
  """
  bkDict = {'ProcessingPass':'Real Data','FileType':'RAW', 'EventType': ID, 'ConfigName':'Fest', 'ConfigVersion':'Fest','DataQualityFlag':'UNCHECKED'}
  res = bkClient.getFilesWithGivenDataSets(bkDict)
  if not res['OK']:
    gLogger.error(res['Message'])
    DIRAC.exit(2)
  lfns = res['Value']
  if not lfns:
    gLogger.info("There were no UNCHECKED '+str(ID)+' files found.")
    DIRAC.exit(0)
  return lfns

def getRunRAWFiles(runID):
  """ This queries the BKK for ALL the files (EXPRESS and FULL stream) that are assocaited with a supplied runID (which is an int).
  
      returns a list of RAW files.
  """
  res = bkClient.getRunFiles(runID)
  if not res['OK']:
    gLogger.error(res['Message'])
    DIRAC.exit(2)
  lfns = res['Value']
  if not lfns:
    gLogger.info("There were no files associated to this run.")
    DIRAC.exit(0)
  return lfns.keys()

def getRAWDescendants(rawLfns):
  """ This queries the BKK to get the files produced with the supplied files as input. The depth is set to two because two steps are needed to perform Brunel then DaVinci. 
   
      returns a dictionary of format: {'rawFile':['decendant1','decendant2']}
  """
  # Get the files with the RAW files as input
  depth = 2 # This is because RAW->RDST,BRUNELHIST->DAVINCIHIST
  res = bkClient.getDescendents(rawLfns, depth)
  if not res['OK']:
    gLogger.error(res['Message'])
    DIRAC.exit(2)
  if not res['Value']['Successful']:
    gLogger.info("No data output from supplied RAW files.")
    DIRAC.exit(0)
  return res['Value']['Successful']

def getFilesOfInterest(rawDescendants):
  """ This will take the dictionary of file decendants {'rawFile':['decendant1','decendant2']} and check the DQ Status of each of the decendants.
      It is assumed here that you are interested only in the decendants that are in UNCHECKED status.
 
      returns a dictionary containing the RAW files and their decendants {'rawFile':['decendant1','decendant2']}
  """
  raw2Reco = {}
  uncheckedRAW = []
  for lfn,produced in rawDescendants.items():
    if not produced:
      # The reconstruction job has probably not finished yet
      gLogger.info("No output data yet associated to this file: %s" % lfn)
    else:
      # Now check the DQFlag of the produced histos
      res = bkClient.getFileMetadata(produced)
      if not res['OK']:
        gLogger.error(res['Message'])
      else:
        for dec in sortList(res['Value'].keys()):
          if res['Value'][dec]['DQFlag'] != 'UNCHECKED':
            res['Value'].pop(dec)
        if res['Value']:
          raw2Reco[lfn] = res['Value'].keys()
        else:
          gLogger.info("The RAW file is UNCHECKED but the histgrams are not: %s" % lfn)
  return raw2Reco

def getInfo(lfn,info):
  """
  get info from file
  """
  l = [ lfn ]
  res = bkClient.getFilesInformations(l)
  if ( res.has_key('Value')):
    res2 = res['Value']
    if ( res2.has_key(lfn)):
      res3 = res2[lfn]
      if ( res3.has_key(info)):
        ft = res3[info]
        if (debug): print '##', info, ' ::', ft
        return ft
  return 'UNDEFINED'
  
def getCERNpfn(lfn):
  """
  Download file if it is a root file
  """
  res = dirac.getFile(lfn,False)
  return res['OK']

def makeNiceName(lfn,run):
  """
  Give the file a clearer name
  """
  et = getInfo(lfn,'EventTypeId')
  if ( et==expressID): ex = 'EX'
  else: ex = 'FULL'
  name = lfn.split('/')[-1]
  names = name.split('_')
  names.insert(1, str(run))
  names.insert(1, ex)
  nn = '_'.join(names)
  return nn
  
def download(lfn,run):
  """
  Download file if it is a root file
  """
  ft = getInfo(lfn,'FileType')
  if ( ft.find('HIST')>0 ):
    name = makeNiceName(lfn,run)
    if (not os.path.isfile(name)):
      if ( getCERNpfn(lfn) ):
        realname = lfn.split('/')[-1]
        if (os.path.isfile(realname)):
          os.rename(realname,name)
          print '## downloaded', lfn, 'as', name
        else:
          print '## downloaded somehow failed for', lfn, 'as', name
          
      else :
        print '## download failed for', lfn, run, ft
    if (debug): print 'No need to download again', name
  
############################################################################
"""
Main program : get all files
"""

gLogger.setLevel("WARNING")
expressID = 91000000
fullID = 90000000
debug = False
bkClient = BookkeepingClient()
dirac = Dirac()
if args:
  runID = int(args[0])
  rawLfns = getRunRAWFiles(runID)
else:
  rawLfns = getStreamRAW(expressID)
  rawLfns2 = getStreamRAW(fullID)
  for l in rawLfns2 : rawLfns.append(l)
  
gLogger.info("Obtained %s RAW files for consideration." %len(rawLfns))
rawDescendants = getRAWDescendants(rawLfns)
raw2Reco = getFilesOfInterest(rawDescendants)

for lfn in sortList(raw2Reco.keys()):
  run = getInfo(lfn,'RunNumber')
  if raw2Reco[lfn]:
    for dec in sortList(raw2Reco[lfn]):
      download(dec,run)

for lfn in sortList(raw2Reco.keys()):
  run = getInfo(lfn,'RunNumber')
  if raw2Reco[lfn]:
    print "\n%s produced:" % lfn
    for dec in sortList(raw2Reco[lfn]):
      print "\t%s" % dec

DIRAC.exit(0)

