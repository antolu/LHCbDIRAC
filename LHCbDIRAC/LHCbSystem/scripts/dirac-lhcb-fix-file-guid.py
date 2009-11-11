#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-dms-fix-file-guid
# Author : Stuart Paterson, Philippe Charpentier
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.2 $"

import os,string

import DIRAC
from DIRAC.Core.Base import Script

Script.registerSwitch( "f:", "OldLFN=", "LFN of existing file to be fixed." )
Script.registerSwitch( "n:", "NewLFN=", "Optional: specify a new LFN for the file (retaining the existing file with incorrect GUID)." )
Script.registerSwitch( "d:", "Directory=", "Optional: directory to download file (defaults to TMPDIR then PWD)." )
Script.registerSwitch( "k", "Keep", "Optional: specify this switch to retain the local copy of the downloaded file" )
Script.registerSwitch( "m", "SafeMode", "Optional: specify this switch to run the script in safe mode (will check the GUIDs only)" )
Script.parseCommandLine( ignoreErrors = True )

args = Script.getPositionalArgs()

from DIRAC.Interfaces.API.Dirac import Dirac
from LHCbDIRAC.LHCbSystem.Utilities.ClientTools  import getRootFileGUID

from DIRAC import gConfig, gLogger, S_OK, S_ERROR

oldLFN = ''
newLFN = ''
exitCode = 2
keep = False
safe = False
directory = os.getcwd()
if os.environ.has_key('TMPDIR'):
  directory = os.environ['TMPDIR']

dirac = Dirac()

def usage():
  print 'Script to fix incorrect file GUIDs.'
  print 'Usage: %s [Try -h,--help for more information]' %(Script.scriptName)
  DIRAC.exit(2)

if args or not Script.getUnprocessedSwitches():
  usage()

for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() in ('f','oldlfn'):
    oldLFN=switch[1]
  elif switch[0].lower() in ('n','newlfn'):
    newLFN=switch[1]
  elif switch[0].lower() in ('d','directory'):
    directory=switch[1]
  elif switch[0].lower() in ('k','keep'):
    keep=True
  elif switch[0].lower() in ('m','safemode'):
    safe=True

oldLFN = oldLFN.replace('LFN:','')
newLFN = newLFN.replace('LFN:','')

if not oldLFN:
  print 'ERROR: The original LFN of the file to be fixed must be specified'
  exitCode=2
  DIRAC.exit(exitCode)

if not os.path.exists(directory):
  print 'ERROR: Optional directory %s must exist' %directory
  exitCode=2
  DIRAC.exit(exitCode)

if not newLFN:
  gLogger.verbose('No new LFN specified, will replace the existing LFN %s' %(oldLFN))
  newLFN = oldLFN


gLogger.verbose('Directory for downloading file is set to %s' %directory)
start = os.getcwd()
try:
  os.chdir(directory)
except Exception,x:
  print 'ERROR: Cannot change directory to %s' %directory
  os.chdir(start)
  DIRAC.exit(exitCode)

replicaInfo = dirac.getReplicas(oldLFN)
if not replicaInfo['OK'] or replicaInfo['Value']['Failed']:
  print 'ERROR: Could not get replica information for %s' %oldLFN
  os.chdir(start)
  DIRAC.exit(exitCode)

replicas = replicaInfo['Value']['Successful'][oldLFN]
storageElements = replicas.keys()
if not storageElements:
  print 'ERROR: Could not determine SEs for replicas of %s' %oldLFN
  os.chdir(start)
  DIRAC.exit(exitCode)

gLogger.info('Existing LFN has replicas at: %s' %string.join(storageElements,', '))

oldGUID = dirac.getMetadata(oldLFN)
if not oldGUID['OK'] or oldGUID['Value']['Failed']:
  print 'ERROR: could not obtain GUID from LFC for %s' %oldLFN
  print oldGUID
  os.chdir(start)
  DIRAC.exit(exitCode)
oldGUID = oldGUID['Value']['Successful'][oldLFN]['GUID']
gLogger.verbose('Existing GUID is %s' %oldGUID)

#retrieve original file
if not os.path.exists(os.path.basename(oldLFN)):
  download = dirac.getFile(oldLFN)
  if not download['OK'] or download['Value']['Failed']:
    print 'ERROR: Could not download file with message - %s' %result['Message']
    os.chdir(start)
    DIRAC.exit(exitCode)
else:
  gLogger.verbose('Found file %s in local directory, will not redownload' %oldLFN)

newGUID = getRootFileGUID(os.path.basename(oldLFN))
if not newGUID['OK']:
  print 'ERROR: could not obtain GUID from existing file %s' %oldLFN
  os.chdir(start)
  DIRAC.exit(exitCode)

newGUID = newGUID['Value']

if newGUID==oldGUID:
  gLogger.info('Old and new GUIDs have the same value (%s), exiting without changes' %oldGUID)
  os.chdir(start)
  DIRAC.exit(0)

if safe:
  gLogger.info('Safe mode - found file GUID = %s and existing GUID = %s, exiting without changes' %(newGUID,oldGUID))
  DIRAC.exit(0)

gLogger.verbose('Will set old GUID to %s from %s' %(newGUID,oldGUID))
if newLFN==oldLFN:
  gLogger.info('Removing old LFN from storages before adding new LFN')
  result=dirac.removeFile(oldLFN)
  if not result['OK']:
    print result
    print 'ERROR: could not remove existing LFN from Grid storage'
    os.chdir(start)
    DIRAC.exit(exitCode)

gLogger.info('Uploading %s as LFN:%s with replica at %s and GUID = %s' %(os.path.basename(oldLFN),newLFN,storageElements[0],newGUID))
result = dirac.addFile(newLFN,os.path.basename(oldLFN),storageElements[0],fileGuid=newGUID,printOutput=False)
if not result['OK']:
  print result
  print 'ERROR: Failed to copy and register new LFN:%s' %newLFN
  os.chdir(start)
  DIRAC.exit(exitCode)

gLogger.info('Checking new replica information for %s' %newLFN)
replicaInfo = dirac.getReplicas(newLFN,printOutput=True)
if not replicaInfo['OK'] or replicaInfo['Value']['Failed']:
  print replicaInfo
  print 'ERROR: Could not get replica information for new LFN %s' %newLFN
  os.chdir(start)
  DIRAC.exit(exitCode)

gLogger.info('Checking new metadata information for %s' %newLFN)
replicaInfo = dirac.getMetadata(newLFN,printOutput=True)
if not replicaInfo['OK'] or replicaInfo['Value']['Failed']:
  print replicaInfo
  print 'ERROR: Could not get replica information for new LFN %s' %newLFN
  os.chdir(start)
  DIRAC.exit(exitCode)

if not keep:
  os.remove(os.path.basename(oldLFN))

os.chdir(start)
DIRAC.exit(0)


