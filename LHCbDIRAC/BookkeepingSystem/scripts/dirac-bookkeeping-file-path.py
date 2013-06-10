#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-file-path
# Author :  Zoltan Mathe
########################################################################
"""
  Return the BK path for the directories of a (list of) files
"""
__RCSID__ = "$Id: dirac-bookkeeping-file-path.py 65177 2013-04-22 15:24:07Z phicharp $"
import  DIRAC.Core.Base.Script as Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, printDMResult
import os

def __buildPath( bkDict ):
  return os.path.join( '/' + bkDict['ConfigName'], bkDict['ConfigVersion'], bkDict['ConditionDescription'],
                  bkDict['ProcessingPass'][1:], str( bkDict['EventType'] ), bkDict['FileType'] )

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'Full', '   Print out full BK dictionary (default: print out BK path)' )
  Script.registerSwitch( '', 'GroupByPath', '   Return a list of files per BK path' )
  Script.registerSwitch( '', 'GroupByProduction', '   Return a list of files per production' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... LFN|File' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name',
                                       '  File:     Name of the file with a list of LFNs' ] ) )
  Script.parseCommandLine()

  import DIRAC
  from DIRAC import S_OK
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

  full = False
  groupByPath = False
  groupByProd = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'Full':
      full = True
    elif switch[0] == 'GroupByPath':
      groupByPath = True
    elif switch[0] == 'GroupByProduction':
      groupByProd = True

  args = Script.getPositionalArgs()
  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = sorted( dmScript.getOption( 'LFNs', [] ) )

  if len( lfnList ) == 0:
    Script.showHelp()
    DIRAC.exit( 0 )

  bk = BookkeepingClient()

  directories = {}
  for lfn in lfnList:
    directories.setdefault( os.path.dirname( lfn ), [] ).append( lfn )

  res = bk.getDirectoryMetadata( sorted( directories ) )
  if not res['OK']:
    printDMResult( res )
    DIRAC.exit( 1 )

  dirList = res.get( 'Value', {} ).get( 'Successful', {} )
  paths = {'Successful':{}, 'Failed':{}}
  for dirName in dirList:
    if full:
      res['Value']['Successful'][dirName] = dirList[dirName][0]
    else:
      bkDict = dirList[dirName][0].copy()
      path = __buildPath( bkDict )
      dirList[dirName] = path
      if groupByPath:
        paths['Successful'].setdefault( path, {} ).update( dict.fromkeys( directories[dirName], '' ) )
      elif groupByProd:
        prod = 'Production ' + str( bkDict['Production'] )
        paths['Successful'].setdefault( prod, {} ).update( dict.fromkeys( directories[dirName], '' ) )

  if groupByPath or groupByProd:
    for dirName in res.get( 'Value', {} ).get( 'Failed', {} ):
      paths['Failed'].update( dict.fromkeys( directories[dirName], 'Directory not in BK' ) )
    res = S_OK( paths )

  printDMResult( res, empty = 'None', script = 'dirac-bookkeeping-file-path' )

