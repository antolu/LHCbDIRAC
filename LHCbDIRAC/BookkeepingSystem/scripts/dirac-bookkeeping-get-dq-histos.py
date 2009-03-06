from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from DIRAC.Core.Utilities.List import sortList
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
import os
from DIRAC import gLogger


def getExpressStreamRAW():
  """ This performs a bookkeeping query to obtain the EXPRESS stream RAW files that are in the DataQuality status 'UNCHECKED'.
      This will get you the files for the runs that have not yet been checked.

      returns a list of RAW files.
  """
  bkDict = {'ProcessingPass':'Real Data','FileType':'RAW', 'EventType':91000000,'ConfigName':'Fest', 'ConfigVersion':'Fest','DataQualityFlag':'UNCHECKED'}
  res = bkClient.getFilesWithGivenDataSets(bkDict)
  if not res['OK']:
    gLogger.error(res['Message'])
    DIRAC.exit(2)
  lfns = res['Value']
  if not lfns:
    gLogger.info("There were no UNCHECKED EXPRESS stream files found.")
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

bkClient = BookkeepingClient()
if args:
  runID = int(args[0])
  rawLfns = getRunRAWFiles(runID)
else:
  rawLfns = getExpressStreamRAW()
gLogger.info("Obtained %s RAW files for consideration." %len(rawLfns))
rawDescendants = getRAWDescendants(rawLfns)
raw2Reco = getFilesOfInterest(rawDescendants)

for lfn in sortList(raw2Reco.keys()):
  if raw2Reco[lfn]:
    print "\n%s produced:" % lfn
    for dec in sortList(raw2Reco[lfn]):
      print "\t%s" % dec

DIRAC.exit(0)

