from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
from DIRAC.Core.Utilities.List import sortList
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
import os
from DIRAC import gLogger
from DIRAC.Interfaces.API.Dirac import Dirac

def getStreamHIST(configName,configVersion,ID,fileType):
  """ This performs a bookkeeping query to obtain the EXPRESS/FULL stream RAW files that are in the DataQuality status 'UNCHECKED'.
      This will get you the files for the runs that have not yet been checked.

      returns a list of RAW files.
  """
  bkDict = {'EventType': ID,
            'ConfigName':configName,
            'ConfigVersion':configVersion,
            'FileType':fileType,
            'DataQualityFlag':'UNCHECKED'}
  res = bkClient.getFilesWithGivenDataSets(bkDict)
  if not res['OK']:
    gLogger.error(res['Message'])
    DIRAC.exit(2)
  lfns = res['Value']
  if not lfns:
    gLogger.info("There were no UNCHECKED %s files from %s stream found." % (fileType,ID))
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
  return lfns.keys()

def getHistoAncestors(histograms):
  """ This queries the BKK to get the ancestors for the files supplied. The depth is set to 3 to ensure that the rDST and the RAW are obtained.
     
      returns a dictionary of format: {'rawFile':['decendant1','decendant2']}
  """
  res = bkClient.getAncestors(histograms,3)
  raw2Histos = {}
  if not res['OK']:
    gLogger.error("Failed to get ancestors for files.",res['Message'])
    DIRAC.exit(2)
  for lfn in sortList(res['Value']['Successful'].keys()):
    ancestors = res['Value']['Successful'][lfn]
    if not ancestors:
      print 'Not good:', lfn
    else:  
      rawFile = ancestors[-1]
      rDSTFile = ancestors[:-1]
      if not raw2Histos.has_key(rawFile):
        raw2Histos[rawFile] = []
      raw2Histos[rawFile].append(lfn)
      raw2Histos[rawFile] = raw2Histos[rawFile] +rDSTFile
  return raw2Histos

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
#    DIRAC.exit(0)
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
        return ft
  return 'UNDEFINED'
  
def getCERNpfn(lfn):
  """
  Download file if it is a root file
  """
  res = dirac.getFile(lfn)
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
          print '##        downloaded', lfn, 'as', name
        else:
          print '## downloaded somehow failed for', lfn, 'as', name
          
      else :
        print '## download failed for', lfn, run, ft
    else :
      print '## already downloaded ', lfn, 'as', name
  
def usage():
  print 'Usage: %s <ConfigName> <ConfigVersion>' %(Script.scriptName)
  print 'Examples:'
  print '   %s Fest Fest' %(Script.scriptName)
  print '   %s Lhcb Beam1' %(Script.scriptName)
  
############################################################################
"""
Main program : get all files
"""
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
if len(args) != 2 :
  print 'There are', len(args), 'args'
  usage()
  DIRAC.exit(-1)

configName = args[0]
configVersion = args[1]

gLogger.setLevel("WARNING")
bkClient = BookkeepingClient()
dirac = Dirac()
expressID = 91000000
fullID = 90000000

expressHistos = getStreamHIST(configName,configVersion,expressID,'BRUNELHIST')+getStreamHIST(configName,configVersion,expressID,'DAVINCIHIST')
gLogger.info("Obtained %s UNCHECKED express stream histograms." % (len(expressHistos))) 
fullHistos = getStreamHIST(configName,configVersion,fullID,'BRUNELHIST')+getStreamHIST(configName,configVersion,fullID,'DAVINCIHIST')
gLogger.info("Obtained %s UNCHECKED full stream histograms." % (len(fullHistos)))

raw2Histos = {}
for lfn in expressHistos+fullHistos:
  lfn2Histo = getHistoAncestors(lfn)
  if lfn2Histo:
    rawFile = lfn2Histo.keys()[0]
    if not raw2Histos.has_key(rawFile):
      raw2Histos[rawFile] = []
    raw2Histos[rawFile].extend(lfn2Histo[rawFile])

for lfn in sortList(raw2Histos.keys()):
  run = getInfo(lfn,'RunNumber')
  if raw2Histos[lfn]:
    for dec in sortList(raw2Histos[lfn]):
      print dec
      download(dec,run)

for lfn in sortList(raw2Histos.keys()):
  if raw2Histos[lfn]:
    print "\n%s produced:" % lfn
    for dec in sortList(raw2Histos[lfn]):
      print "\t%s" % dec

DIRAC.exit(0)
