from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from DIRAC.Core.Utilities.List import sortList
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
import os
from DIRAC import gLogger


def getExpressStreamRAW():
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

def getRunFiles(runID):
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
rawLfns = getExpressStreamRAW()
#rawLfns = getRunFiles(44878)
gLogger.info("Obtained %s RAW files for consideration." %len(rawLfns))
rawDescendants = getRAWDescendants(rawLfns)
raw2Reco = getFilesOfInterest(rawDescendants)

for lfn in sortList(raw2Reco.keys()):
  if raw2Reco[lfn]:
    print "\n%s produced:" % lfn
    for dec in sortList(raw2Reco[lfn]):
      print "\t%s" % dec

DIRAC.exit(0)

