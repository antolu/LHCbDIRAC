#!/usr/bin/env python
"""
  Create a pool xml catalog slice for the specified LFNs
"""
__RCSID__ = "$Id: dirac-bookkeeping-genXMLCatalog.py 86927 2016-01-04 09:07:47Z phicharp $"

import sys, os, time

import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from DIRAC.Core.Base import Script
from DIRAC import gLogger

def __getLfnsFromFile( optFiles, gaudiVerbose ):
  import tempfile
  a, tmpFile = tempfile.mkstemp( suffix = ".py" )
  runOpts = ""
  for opt in optFiles:
    if not os.path.exists( opt ):
      gLogger.always( "File not found: ", opt )
      DIRAC.exit( 1 )
    runOpts += opt + " "

  gaudiRun = "gaudirun.py -n -o %s %s" % ( tmpFile, runOpts )
  if not gaudiVerbose:
    gaudiRun += " &>/dev/null"
  gLogger.info( "Extract list of input files from", optFiles )

  if  os.system( "which gaudirun.py &>/dev/null" ) != 0:
    gLogger.info( "SetupProject LHCb for getting environment" )
    command = ". SetupProject.sh LHCb --no-user" + ( " &>/dev/null;" if not gaudiVerbose else ' ;' ) + gaudiRun
    rc = os.system( command )
  else:
      rc = os.system( gaudiRun )
  if rc:
      gLogger.always( "Error when parsing options files", optFiles )
      DIRAC.exit( rc )

  optDict = eval( open( tmpFile, 'r' ).read() )
  os.remove( tmpFile )
  appInput = optDict.get( 'EventSelector', {} ).get( 'Input' )
  if not appInput:
      gLogger.always( "Options file do not set EventSelector().Input" )
      DIRAC.exit( 1 )

  return [inp.split()[0].split( "'" )[1].replace( 'LFN:', '' ) for inp in appInput]

def execute():

  catalog = 'pool_xml_catalog.xml'
  site = 'LCG.CERN.ch'
  depth = 1
  optFiles = []
  newOptFile = ''
  ignore = False
  verbose = False
  gaudiVerbose = False

  dmScript = DMScript()
  dmScript.registerSiteSwitches()
  dmScript.registerFileSwitches()
  Script.registerSwitch( '', 'Options=', '   List of option files to consider' )
  Script.registerSwitch( '', 'NewOptions=', '   Name of a new options file to be generated with LFNs (default: none)' )
  Script.registerSwitch( '', 'Catalog=', '   Catalog name (default: %s' % catalog )
  Script.registerSwitch( '', 'Depth=', '   Depth for ancestor consideration (default: none)' )
  Script.registerSwitch( 'v', 'Verbose', '   Verbose gLogger.always(out' )
  Script.registerSwitch( '', 'Ignore', '   Ignore missing files' )
  Script.registerSwitch( '', 'GaudiVerbose', '   Set Gaudi verbose when parsing option files' )

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... ' % Script.scriptName] ) )
  Script.parseCommandLine( ignoreErrors = True )

  t0 = time.time()
  switches = Script.getUnprocessedSwitches()
  for o, a in switches:
    if o in ( "Options" ):
      optFiles += a.split( ',' )
    elif o in ( "Catalog" ):
      catalog = a
      ext = os.path.splitext( catalog )
      if len( ext[1] ) == 0 and '/dev/' not in catalog:
          catalog += os.path.extsep + "xml"
    elif o in ( "Depth" ):
      try:
        depth = int( a )
      except:
        gLogger.fatal( "Invalid depth, must be integer" )
        DIRAC.exit( 1 )
    elif o in ( "v", 'Verbose' ):
      verbose = True
      gLogger.setLevel( 'Info' )
    elif o in ( "NewOptions" ):
      newOptFile = a
      ext = os.path.splitext( newOptFile )
      if len( ext[1] ) == 0:
          newOptFile += os.path.extsep + "py"
    elif o in ( "Ignore" ):
      ignore = True
    elif o in ( "GaudiVerbose" ):
      gaudiVerbose = True

  if depth <= 0:
    gLogger.fatal( "Invalid ancestor depth, should be >= 1" )
    Script.showHelp()
    DIRAC.exit( 1 )

  if newOptFile and len( optFiles ) != 1:
    if optFiles:
      gLogger.fatal( "Generating an options file is only valid with a _single_ file as input" )
    else:
      gLogger.fatal( "Generating an options file is only valid with an option file as input" )
    Script.showHelp()
    DIRAC.exit( 1 )
  if newOptFile:
    if depth > 1:
      gLogger.warn( "New options file required, depth ignored..." )
    if ignore:
      gLogger.warn( "New options file required, cannot ignore missing files" )
      ignore = False
    depth = 1
    if os.path.realpath( newOptFile ) == os.path.realpath( optFiles[0] ):
      gLogger.fatal( "Cannot write a new option file overwriting the original one, select a name different from", newOptFile )
      Script.showHelp()
      DIRAC.exit( 1 )

  sites = dmScript.getOption( 'Sites', [] )
  if sites:
    if len( sites ) != 1:
      gLogger.fatal( "Provide at most one site..." )
      Script.showHelp()
      DIRAC.exit( 1 )
    site = sites[0]

  if optFiles:
    lfnList = __getLfnsFromFile( optFiles, gaudiVerbose )
  else:
    lfnList = dmScript.getOption( 'LFNs', [] )
  if not lfnList:
    gLogger.fatal( "No option files and no list of files given" )
    Script.showHelp()
    DIRAC.exit( 1 )

  rc = 0
  try:
      # Verify the user has a valid proxy
    done = 1
    while done and os.system( "dirac-proxy-info --checkvalid > /dev/null" ) != 0:
      gLogger.always( "You don't have a valid proxy, we create one..." )
      done = os.system( "lhcb-proxy-init" )

    if depth > 1:
      from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
      bk = BookkeepingClient()
      result = bk.getFileAncestors( lfnList, depth )
      if not result['OK']:
        gLogger.fatal( "Error getting ancestor files..." )
        DIRAC.exit( 1 )
      lfnList = [anc['FileName'] for ancestors in result['Value']['Successful'].values() for anc in ancestors] + result['Value']['Successful'].keys()

    from DIRAC.Interfaces.API.Dirac import Dirac
    if not verbose:
      gLogger.setLevel( "Always" )
    if newOptFile:
      catalog = "/dev/null"
    gLogger.info( "List of LFNs:", lfnList )
    gLogger.info( "Site:", site )
    gLogger.info( "Catalog file:", catalog )
    result = Dirac().getInputDataCatalog( lfnList, site, catalog, ignoreMissing = ignore )
    if result["OK"]:
      result = result['Value']
      if result['Failed']:
        gLogger.always( 'Only a fraction of the input files are present and available at %s (%d missing)' % ( site, len( result['Failed'] ) ) )
      if not result['Successful']:
        gLogger.fatal( '... and none was actually available!' )
        DIRAC.exit( 1 )
      if newOptFile:
        lfnToPfn = result["Successful"]
        optLines = open( optFiles[0] ).readlines()
        fo = open( newOptFile, 'w' )
        for opt in optLines:
          for lfn in lfnList:
            if lfn in opt:
              # get the tURL for this lfn
              if lfn in lfnToPfn:
                pfn = lfnToPfn[lfn]['turl']
                opt = opt.replace( "LFN:", '' ).replace( lfn, pfn )
              else:
                gLogger.error( "No tURL found for ", lfn )
              break
          fo.write( opt )
        fo.close()
        gLogger.always( "New options file %s successfully created" % newOptFile )
      else:
        gLogger.always( "POOL XML catalog %s successfully created" % catalog )
        if os.path.exists( catalog + '.temp' ):
          os.remove( catalog + '.temp' )
        if os.path.isfile( catalog ):
          catOption, ext = os.path.splitext( catalog )
          catOption += os.path.extsep + "py"
          fo = open( catOption, 'w' )
          fo.write( "FileCatalog().Catalogs = [ 'xmlcatalog_file:" + catalog + "' ]\n" )
          fo.close()
          gLogger.always( "==> You must add %s to your list of options file" % catOption )
    else:
      gLogger.always( "Error getting the list of PFNs:", result['Message'] )
  except Exception, e:
    gLogger.exception( "Exception caught while creating catalog or option file:", '', e )
    rc = 1

  gLogger.info( "Total execution time for %d files: %5.2f seconds" % ( len( lfnList ), time.time() - t0 ) )
  DIRAC.exit( rc )

if __name__ == "__main__":

  execute()
  DIRAC.exit( 0 )


