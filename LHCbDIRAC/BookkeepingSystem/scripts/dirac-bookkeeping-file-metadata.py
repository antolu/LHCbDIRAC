#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-eventtype-mgt-update
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve metadata from the Bookkeeping for the given files
"""
__RCSID__ = "$Id$"
import  DIRAC.Core.Base.Script as Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript


if __name__ == "__main__":
  dmScript = DMScript()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'Full', '   Print out all metadata' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... LFN|File' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name',
                                       '  File:     Name of the file with a list of LFNs' ] ) )
  Script.parseCommandLine()

  import DIRAC
  from DIRAC.Core.DISET.RPCClient import RPCClient

  full = False
  switches = Script.getUnprocessedSwitches()
  for switch in switches:
    if switch[0] == 'Full':
      full = True

  args = Script.getPositionalArgs()
  for lfn in args:
    dmScript.setLFNsFromFile( lfn )
  lfnList = sorted( dmScript.getOption( 'LFNs', [] ) )

  if len( lfnList ) == 0:
    Script.showHelp()
    DIRAC.exit( 0 )


  client = RPCClient( 'Bookkeeping/BookkeepingManager' )
  res = client.getFileMetadata( lfnList )
  if not res['OK']:
    print 'ERROR: Failed to get file metadata: %s' % res['Message']
    DIRAC.exit( 2 )

  exitCode = 0

  lenName = 0
  for lfn in lfnList:
    lenName = max( lenName, len( lfn ) )
  lenName += 2
  lenGUID = 38
  lenItem = 15
  sep = ''

  if not full:
    print '%s %s %s %s %s %s %s' % ( 'FileName'.ljust( lenName ),
                                     'Size'.ljust( 10 ),
                                     'GUID'.ljust( lenGUID ),
                                     'Replica'.ljust( 8 ),
                                     'DataQuality'.ljust( 12 ),
                                     'RunNumber'.ljust( 10 ),
                                     '#events'.ljust( 10 ) )
  lfnMetadata = res['Value'].get( 'Successful', res['Value'] )
  for lfn in lfnMetadata:
    lfnMetaDict = lfnMetadata[lfn]
    if full:
      print '%s%s %s' % ( sep, 'FileName'.ljust( lenItem ), lfn )
      sep = '\n'
      for item in sorted( lfnMetaDict ):
        print '%s %s' % ( item.ljust( lenItem ), lfnMetaDict[item] )
    else:
      size = lfnMetaDict['FileSize']
      guid = lfnMetaDict['GUID']
      gotReplica = lfnMetaDict['GotReplica']
      dq = lfnMetaDict.get( 'DataqualityFlag' )
      run = lfnMetaDict['RunNumber']
      evtStat = lfnMetaDict['EventStat']
      if not gotReplica:
        gotReplica = 'No'
      print  '%s %s %s %s %s %s %s' % ( lfn.ljust( lenName ),
                                        str( size ).ljust( 10 ),
                                        guid.ljust( lenGUID ),
                                        gotReplica.ljust( 8 ),
                                        dq.ljust( 12 ),
                                        str( run ).ljust( 10 ),
                                        str( evtStat ).ljust( 10 ) )
  failed = res['Value'].get( 'Failed', [] )
  if failed:
    print '\n'
    for lfn in failed:
      if lfn:
        print '%s does not exist in the Bookkeeping.' % lfn
        exitCode = 2

  DIRAC.exit( exitCode )

