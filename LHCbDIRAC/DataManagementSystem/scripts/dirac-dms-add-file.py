#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-add-file
########################################################################
"""
  Upload a file to the grid storage and register it in the File Catalog
"""
__RCSID__ = "$Id$"

def getDict( item_list ):
  """
    From the input list, populate the dictionary
  """
  lfn_dict = {}
  lfn_dict['lfn'] = item_list[0].replace( 'LFN:', '' ).replace( 'lfn:', '' )
  lfn_dict['localfile'] = item_list[1]
  lfn_dict['SE'] = item_list[2]
  guid = None
  if len( item_list ) > 3:
    guid = item_list[3]
  lfn_dict['guid'] = guid
  return lfn_dict

if __name__ == "__main__":

  from DIRAC.Core.Base import Script
  import os
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... LFN Path SE [GUID]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name',
                                       '  Path:     Local path to the file',
                                       '  SE:       DIRAC Storage Element',
                                       '  GUID:     GUID to use in the registration (optional)' ,
                                       '',
                                       ' ++ OR ++',
                                       '',
                                       'Usage:',
                                       '  %s [option|cfgfile] ... LocalFile' % Script.scriptName,
                                       'Arguments:',
                                       '  LocalFile: Path to local file containing all the above, i.e.:',
                                       '  lfn1 localfile1 SE [GUID1]',
                                       '  lfn2 localfile2 SE [GUID2]'] )
                          )

  Script.parseCommandLine( ignoreErrors = True )
  args = Script.getPositionalArgs()
  if len( args ) < 1 or len( args ) > 4:
    Script.showHelp()

  lfns = []
  if len( args ) == 1:
    inputFileName = args[0]
    if os.path.exists( inputFileName ):
      inputFile = open( inputFileName, 'r' )
      for line in inputFile:
        line = line.rstrip()
        items = line.split()
        items[0] = items[0].replace( 'LFN:', '' ).replace( 'lfn:', '' )
        lfns.append( getDict( items ) )
      inputFile.close()
  else:
    lfns.append( getDict( args ) )

  from DIRAC.DataManagementSystem.Client.DataManager import DataManager
  from DIRAC import gLogger
  import DIRAC
  exitCode = 0

  dm = DataManager()
  logLevel = gLogger.getLevel()
  for lfnDict in lfns:
    localFile = lfnDict['localfile']
    if not os.path.exists( localFile ):
      gLogger.error( "File %s doesn't exist locally" % localFile )
      exitCode = 1
      continue
    if not os.path.isfile( localFile ):
      gLogger.error( "%s is not a file" % localFile )
      exitCode = 2
      continue

    if not lfnDict['guid']:
      from LHCbDIRAC.Core.Utilities.File import makeGuid
      lfnDict['guid'] = makeGuid( localFile )[localFile]
    lfn = lfnDict['lfn']
    gLogger.notice( "\nUploading %s as %s" % ( localFile, lfn ) )
    gLogger.setLevel( 'FATAL' )
    res = dm.putAndRegister( lfn, localFile, lfnDict['SE'], lfnDict['guid'] )
    gLogger.setLevel( logLevel )
    if not res['OK']:
      exitCode = 3
      gLogger.error( 'Error: failed to upload %s to %s' % ( localFile, lfnDict['SE'] ), res['Message'] )
    else:
      if lfn in res['Value']['Successful']:
        gLogger.notice( 'Successfully uploaded %s to %s (%.1f seconds)' % ( localFile, lfnDict['SE'],
                                                                            res['Value']['Successful'][lfn]['put'] ) )
      else:
        gLogger.error( 'Error: failed to upload %s to %s' % ( lfn, lfnDict['SE'] ), res['Value']['Failed'][lfn] )

  DIRAC.exit( exitCode )
