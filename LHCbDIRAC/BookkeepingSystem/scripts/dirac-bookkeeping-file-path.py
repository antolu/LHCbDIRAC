#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-file-path
# Author :  Zoltan Mathe
########################################################################
"""
  Return the BK path for the directories of a (list of) files
"""
__RCSID__ = "$Id$"
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script, printDMResult
import os

def __buildPath( bkDict ):
  return os.path.join( '/' + bkDict['ConfigName'], bkDict['ConfigVersion'], bkDict['ConditionDescription'],
                  bkDict['ProcessingPass'][1:].replace( 'Real Data', 'RealData' ), str( bkDict['EventType'] ), bkDict['FileType'] )

if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'Full', '   Print out full BK dictionary (default: print out BK path)' )
  Script.registerSwitch( '', 'GroupBy=', '   Return a list of files per <metadata item>' )
  Script.registerSwitch( '', 'GroupByPath', '   Return a list of files per BK path' )
  Script.registerSwitch( '', 'GroupByProduction', '   Return a list of files per production' )
  Script.registerSwitch( '', 'Summary', '   Only give the number of files in each path' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... LFN|File' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name',
                                       '  File:     Name of the file with a list of LFNs' ] ) )
  Script.parseCommandLine()

  import DIRAC
  from DIRAC import S_OK, gLogger
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

  full = False
  groupBy = False
  summary = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'Full':
      full = True
    elif switch[0] == 'GroupByPath':
      groupBy = 'Path'
    elif switch[0] == 'GroupByProduction':
      groupBy = 'Production'
    elif switch[0] == 'GroupBy':
      groupBy = switch[1]
    elif switch[0] == 'Summary':
      summary = True

  args = Script.getPositionalArgs()
  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = sorted( dmScript.getOption( 'LFNs', [] ) )

  if len( lfnList ) == 0:
    Script.showHelp()
    DIRAC.exit( 0 )

  bk = BookkeepingClient()

  dirMetadata = ( 'Production', 'ConfigName', 'ConditionDescription', 'EventType',
                 'FileType', 'ConfigVersion', 'ProcessingPass', 'Path' )
  fileMetadata = ( 'EventType', 'FileType', 'RunNumber', 'JobId', 'DQFlag', 'GotReplica' )
  if groupBy and groupBy not in dirMetadata:
    if groupBy not in fileMetadata:
      gLogger.always( 'Invalid metata item', groupBy )
      gLogger.always( 'Directory metadata:', ', '.join( dirMetadata ) )
      gLogger.always( 'File metadata:', ', '.join( fileMetadata ) )
      DIRAC.exit( 1 )
    res = bk.getFileMetadata( lfnList )
    if res['OK']:
      paths = {'Successful':{}, 'Failed':[]}
      for lfn, metadata in res['Value']['Successful'].items():
        group = metadata.get( groupBy )
        paths['Successful'].setdefault( '%s %s' % ( groupBy, group ), set() ).add( lfn )
        lfnList.remove( lfn )
      paths['Failed'].extend( lfnList )
      res = S_OK( paths )
  else:
    directories = {}
    for lfn in lfnList:
      directories.setdefault( os.path.dirname( lfn ), [] ).append( lfn )

    res = bk.getDirectoryMetadata( sorted( directories ) )
    if not res['OK']:
      printDMResult( res )
      DIRAC.exit( 1 )

    success = res.get( 'Value', {} ).get( 'Successful', {} )
    failed = res.get( 'Value', {} ).get( 'Failed', {} )
    paths = {'Successful':{}, 'Failed':{}}
    for dirName in success:
      if full:
        success[dirName] = success[dirName][0]
      else:
        bkDict = success[dirName][0].copy()
        bkDict['Path'] = __buildPath( bkDict )
        if groupBy in bkDict:
          if groupBy != 'Path':
            prStr = '%s %s' % ( groupBy, bkDict[groupBy] )
          else:
            prStr = bkDict[groupBy]
          paths['Successful'].setdefault( prStr, set() ).update( directories[dirName] )
        elif groupBy:
          gLogger.always( 'Invalid metadata item: %s' % groupBy )
          gLogger.always( 'Available are: %s' % str( bkDict.keys() ) )
          DIRAC.exit( 1 )
        else:
          success[dirName] = bkDict['Path']

    if groupBy:
      if summary:
        pathSummary = {'Successful': {}, 'Failed' : {}}
        for path in paths['Successful']:
          pathSummary['Successful'][path] = '%d files' % len( paths['Successful'][path] )
        if failed:
          pathSummary['Failed'] = dict( ( path, 'Directory not in BK (%d files)' % len( directories[path] ) ) for path in failed )
        else:
          pathSummary.pop( 'Failed' )
        res = S_OK( pathSummary )
      else:
        for dirName in failed:
          paths['Failed'].update( dict.fromkeys( directories[dirName], 'Directory not in BK' ) )
        res = S_OK( paths )

  printDMResult( res, empty = 'None', script = 'dirac-bookkeeping-file-path' )

