#! /usr/bin/env python

__RCSID__ = "$Id: dirac-dms-check-directory-integrity.py 74045 2014-02-10 12:27:34Z chaen $"

if __name__ == "__main__":
  from DIRAC.Core.Base import Script

  Script.setUsageMessage( """
  Check the integrity of the state of the storages and information in the File Catalogs
  for a given directory or a collection of directories.

  Usage:
     %s  (<options>|<cfgFile>) <dir | fileContainingDirs>
  """ % Script.scriptName )

  Script.parseCommandLine()

  import DIRAC
  from DIRAC                                                          import gLogger
  from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
  from LHCbDIRAC.DataManagementSystem.Client.DataIntegrityClient      import DataIntegrityClient
  import sys

  fc = FileCatalog()
  integrity = DataIntegrityClient()
  gLogger.setLevel( 'INFO' )

  args = Script.getPositionalArgs()
  if len( args ) < 1:
    print "Please provide a directory or a file containing directories"
    Script.showHelp()
    DIRAC.exit( 0 )
  else:
    inputNames = args

  directories = []
  for inputFileName in inputNames:
    try:
      inputFile = open( inputFileName, 'r' )
      stringIn = inputFile.read()
      directories += stringIn.splitlines()
      inputFile.close()
    except:
      directories.append( inputFileName )

  ######################################################
  #
  # This check performs Catalog->BK and Catalog->SE for possible output directories
  #
  res = fc.exists( directories )
  if not res['OK']:
    gLogger.error( res['Message'] )
    DIRAC.exit( -2 )
  for directory, error in res['Value']['Failed']:
    gLogger.error( 'Failed to determine existence of directory', '%s %s' % ( directory, error ) )
  if res['Value']['Failed']:
    DIRAC.exit( -2 )
  directoryExists = res['Value']['Successful']
  for directory in sorted( directoryExists ):
    if not directoryExists[directory]:
      continue
    gLogger.info( "Checking the integrity of %s" % directory )
    iRes = integrity.catalogDirectoryToBK( directory )
    if not iRes['OK']:
      gLogger.error( 'Error getting directory content:', iRes['Message'] )
      continue
    catalogDirMetadata = iRes['Value']['CatalogMetadata']
    catalogDirReplicas = iRes['Value']['CatalogReplicas']
    catalogMetadata = {}
    catalogReplicas = {}
    for lfn in catalogDirMetadata:
      #if not lfn in bk2catalogMetadata.keys():
        catalogMetadata[lfn] = catalogDirMetadata[lfn]
        if lfn in catalogDirReplicas:
          catalogReplicas[lfn] = catalogDirReplicas[lfn]
    if not catalogMetadata:
      continue
    res = integrity.checkPhysicalFiles( catalogReplicas, catalogMetadata, [] )
    if not res['OK']:
      gLogger.error( "Error checking physical files:", res['Message'] )
      continue
