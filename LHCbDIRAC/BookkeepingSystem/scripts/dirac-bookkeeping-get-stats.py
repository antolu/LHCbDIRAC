#! /usr/bin/env python
"""
   Get statistical information on a dataset
"""

__RCSID__ = "$Id: dirac-bookkeeping-get-stats.py 69357 2013-08-08 13:33:31Z phicharp $"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import gLogger
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

def intWithQuotes( val, quote = "'" ):
  chunks = []
  if not val:
    return 'None'
  while val:
    if val >= 1000:
      chunks.append( "%03d" % ( val % 1000 ) )
    else:
      chunks.append( "%d" % ( val % 1000 ) )
    val /= 1000
  chunks.reverse()
  return quote.join( chunks )

def scaleLumi( lumi ):
  lumiUnits = ( '/microBarn', '/nb', '/pb', '/fb', '/ab' )
  if lumi:
    for lumiUnit in lumiUnits:
      if lumi < 1000.:
        break
      lumi /= 1000.
  else:
    lumi = 0
    lumiUnit = ''
  return lumi, lumiUnit

def execute():

  triggerRate = False
  listRuns = False
  listFills = False
  for switch in Script.getUnprocessedSwitches():
    if switch[0] == 'TriggerRate':
      triggerRate = True
    elif switch[0] == 'ListRuns':
      listRuns = True
    elif switch[0] == 'ListFills':
      listFills = True

  if listRuns:
    listFills = False
  lfns = dmScript.getOption( 'LFNs' )
  bkQuery = dmScript.getBKQuery()
  if not bkQuery and not lfns:
    print "No BK query given..."
    DIRAC.exit( 1 )
  if bkQuery:
    prodList = bkQuery.getQueryDict().get( 'Production', [None] )
    if type( prodList ) != type( [] ):
      prodList = [prodList]
    bkQuery.setOption( 'ProductionID', None )
    fileType = bkQuery.getFileTypeList()
  else:
    prodList = [None]
    fileType = None
  if fileType != ['RAW'] and triggerRate:
    gLogger.notice( 'TriggerRate option ignored as not looking for RAW files' )
    triggerRate = False

  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  bk = BookkeepingClient()
  from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
  transClient = TransformationClient()

  for prod in prodList:
    if bkQuery:
      queryDict = bkQuery.getQueryDict()
      if queryDict.get( 'Visible', 'All' ).lower() in ( 'no', 'all' ):
        bkQuery.setOption( 'ReplicaFlag', 'ALL' )
    if prod:
      res = transClient.getTransformation( prod, extraParams = False )
      if not res['OK']:
        continue
      prodName = res['Value']['TransformationName']
      bkQuery.setOption( 'Production', prod )
      print "For production %d, %s (query %s)" % ( prod, prodName, bkQuery )
    elif lfns:
      print "For %d LFNs:" % len( lfns )
    else:
      print "For BK query:", bkQuery
      if bkQuery:
        queryDict = bkQuery.getQueryDict()

    # Get information from BK
    if not triggerRate and not lfns and 'ReplicaFlag' not in queryDict and 'DataQuality' not in queryDict:
      print "Getting info from filesSummary..."
      query = queryDict.copy()
      if len( query ) <= 3:
        query.update( {'1':1, '2':2, '3':3 } )
      fileTypes = query.get( 'FileType' )
      if not isinstance( fileTypes, list ):
        fileTypes = [fileTypes]
      if 'FileType' in query:
        query.pop( 'FileType' )
      records = []
      nDatasets = 1
      if isinstance( fileTypes, list ):
        nDatasets = len( fileTypes )
      eventTypes = query.get( 'EventType' )
      if isinstance( eventTypes, list ):
        nDatasets *= len( eventTypes )
      for fileType in fileTypes:
        if fileType:
          query['FileType'] = fileType
        res = bk.getFilesSummary( query )
        if not res['OK']:
          print "Error getting statistics from BK", res['Message']
          DIRAC.exit( 0 )
        paramNames = res['Value']['ParameterNames']
        record = []
        for paramValues in res['Value']['Records']:
          if not record:
            record = len( paramValues ) * [0]
          record = [( rec + val ) if val else rec for rec, val in zip( record, paramValues )]
        # print fileType, record
        for name, value in zip( paramNames, record ):
          if name == 'NumberOfEvents':
            nevts = value
          elif name == 'FileSize':
            size = value
          elif name == 'Luminosity':
            lumi = value
        record.append( nevts / float( lumi ) if lumi else 0. )
        record.append( size / float( lumi )  if lumi else 0. )
        if not records:
          records = len( record ) * [0]
        records = [rec1 + rec2 for rec1, rec2 in zip( record, records )]
        # print fileType, records, 'Total'
      paramNames += ['EvtsPerLumi', 'SizePerLumi' ]
    else:
      print "Getting info from files..."
      # To be replaced with getFilesWithMetadata when it allows invisible files
      paramNames = ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity', 'EvtsPerLumi', 'SizePerLumi']
      nbFiles = 0
      nbEvents = 0
      fileSize = 0
      lumi = 0
      datasets = set()
      runList = {}
      if not lfns:
        fileTypes = queryDict.get( 'FileType' )
        res = bk.getFilesWithMetadata( queryDict )
        if 'OK' in res.get( 'Value', {} ):
            res = res['Value']
        if 'ParameterNames' in res.get( 'Value', {} ):
          parameterNames = res['Value']['ParameterNames']
          info = res['Value']['Records']
        else:
          if 'Value' in res:
            gLogger.error( 'ParameterNames not present:', str( res['Value'].keys() ) if isinstance( res['Value'], dict ) else str( res['Value'] ) )
          info = []
          res = bk.getFiles( queryDict )
          if res['OK']:
            lfns = res['Value']
        for item in info:
          metadata = dict( zip( parameterNames, item ) )
          datasets.add( ( metadata.get( 'EventType', metadata['FileName'].split( '/' )[5] ), metadata.get( 'FileType', metadata.get( 'Name' ) ) ) )
          try:
            nbEvents += metadata['EventStat']
            fileSize += metadata['FileSize']
            lumi += metadata['Luminosity']
            run = metadata['RunNumber']
            runList.setdefault( run, [ 0., 0., 0. ] )
            runList[run][0] += metadata['Luminosity']
            runList[run][1] += metadata['EventStat']
            runList[run][2] += metadata['FileSize']
            nbFiles += 1
          except Exception as e:
            gLogger.exception( 'Exception for %s' % str( metadata.keys() ), e )
      if lfns:
        res = bk.getFileMetadata( lfns )
        if res['OK']:
          for lfn, metadata in res['Value']['Successful'].items():
            datasets.add( ( metadata['EventType'], metadata['FileType'] ) )
            try:
              nbEvents += metadata['EventStat']
              fileSize += metadata['FileSize']
              lumi += metadata['Luminosity']
              run = metadata['RunNumber']
              runList.setdefault( run, [ 0, 0, 0 ] )
              runList[run][0] += metadata['Luminosity']
              runList[run][1] += metadata['EventStat']
              runList[run][2] += metadata['FileSize']
              nbFiles += 1
            except Exception as e:
              gLogger.exception( 'Exception for %s' % lfn, str( metadata.keys() ), e )
        else:
          print "Error getting files metadata:", res['Message']
          continue
      records = [nbFiles, nbEvents, fileSize, lumi,
                 nbEvents / float( lumi ) if lumi else 0.,
                 fileSize / float( lumi ) if lumi else 0.]
      nDatasets = max( 1, len( datasets ) )

    # Now printout the results
    tab = 17
    sizeUnits = ( 'Bytes', 'kB', 'MB', 'GB', 'TB', 'PB' )
    nfiles = nevts = evtsPerLumi = lumi = 0
    for name, value in zip( paramNames, records ):
      if name == 'NbofFiles':
        nfiles = value
        print '%s: %s' % ( 'Nb of Files'.ljust( tab ), intWithQuotes( value ) )
      elif name == 'NumberOfEvents':
        nevts = value
        print '%s: %s' % ( 'Nb of Events'.ljust( tab ), intWithQuotes( value ) )
      elif name == 'FileSize':
        size = value
        sizePerEvt = '(%.1f kB per evt)' % ( size / nevts / 1000. ) if nevts and nDatasets == 1 else ''
        if size:
          for sizeUnit in sizeUnits:
            if size < 1000.:
              break
            size /= 1000.
        else:
          size = 0
          sizeUnit = ''
        print '%s: %.3f %s %s' % ( 'Total size'.ljust( tab ), size, sizeUnit, sizePerEvt )
      elif name == 'Luminosity':
        lumi = value / nDatasets
        lumi, lumiUnit = scaleLumi( lumi )
        lumiString = 'Luminosity' if nDatasets == 1 else 'Avg luminosity'
        print '%s: %.3f %s' % ( lumiString.ljust( tab ), lumi, lumiUnit )
      elif name == 'EvtsPerLumi':
        evtsPerLumi = value * nDatasets
      elif name == 'SizePerLumi':
        value *= nDatasets
        print "%s: %.1f GB" % ( ( 'Size  per %s' % '/pb' ).ljust( tab ), value * 1000000. / 1000000000. )
        # if nDatasets != 1:
        #  sizePerEvt = value / evtsPerLumi / 1000. if evtsPerLumi else 0.
        #  print '%s: %.1f kB' % ( 'Avg size per evt'.ljust( tab ), sizePerEvt )
    if lumi:
      filesPerLumi = nfiles / lumi
      print "%s: %.1f" % ( ( 'Files per %s' % lumiUnit ).ljust( tab ), filesPerLumi )
    if triggerRate:
      from datetime import timedelta
      # Get information from the runs, but first get those that are Finished
      res = bk.getRunStatus( list( runList ) )
      if not res['OK']:
        gLogger.error( 'Error getting run status', res['Message'] )
        runs = []
      else:
        success = res['Value']['Successful']
        runs = [run for run in success if success[run].get( 'Finished' ) == 'Y']
      notFinished = len( runList ) - len( runs )
      if notFinished:
        gLogger.notice( '%d runs not Finished (ignored), %s runs Finished (used for trigger rate)' % ( notFinished, str( len( runs ) if len( runs ) else 'no' ) ) )
      if runs:
        nevts = 0
        size = 0
        fullDuration = 0.
        totalLumi = 0.
        fills = {}
        fillDuration = {}
        for run in sorted( runs ):
          res = bk.getRunInformations( run )
          if not res['OK']:
            gLogger.error( 'Error from BK getting run information', res['Message'] )
          else:
            nevts += runList[run][1]
            size += runList[run][2]
            info = res['Value']
            fill = info['FillNumber']
            fills.setdefault( fill, [] ).append( str( run ) )
            runDuration = ( info['RunEnd'] - info['RunStart'] ).total_seconds() / 3600.
            fillDuration[fill] = fillDuration.setdefault( fill, 0 ) + runDuration
            fullDuration += runDuration
            lumiDict = dict( zip( info['Stream'], info['luminosity'] ) )
            statDict = dict( zip( info['Stream'], info['Number of events'] ) )
            lumi = info['TotalLuminosity']
            if abs( lumi - runList[run][0] / nDatasets ) > 1:
              print 'Run and files luminosity mismatch (ignored): run', run, 'runLumi', lumi, 'filesLumi', runList[run][0] / nDatasets
            else:
              totalLumi += lumi
        rate = ( '%.1f events/second' % ( nevts / fullDuration / 3600 ) ) if fullDuration else 'Run duration not available'
        totalLumi, lumiUnit = scaleLumi( totalLumi )
        print '%s: %.3f %s' % ( 'Total Luminosity'.ljust( tab ), totalLumi, lumiUnit )
        print '%s: %.1f hours (%d runs)' % ( 'Run duration'.ljust( tab ), fullDuration, len( runs ) )
        print '%s: %s' % ( 'Trigger rate'.ljust( tab ), rate )
        rate = ( '%.1f MB/second' % ( size / 1000000. / fullDuration / 3600. ) ) if fullDuration else 'Run duration not available'
        print '%s: %s' % ( 'Throughput'.ljust( tab ), rate )
        result = getCollidingBunches( min( fills.keys() ), max( fills.keys() ) )
        collBunches = 0.
        for fill in fillDuration:
          if fill not in result:
            print  "Error: no number of colliding bunches for fill %d" % fillDuration
          else:
            collBunches += result[fill] * fillDuration[fill]
        collBunches /= fullDuration
        print '%s: %.1f on average' % ( 'Colliding bunches'.ljust( tab ), collBunches )
        if listFills:
          print 'List of fills: ', ','.join( ["%d (%d runs, %.1f hours)" % ( fill, len( fills[fill] ), fillDuration[fill] ) for fill in sorted( fills )] )
        if listRuns:
          for fill in sorted( fills ):
            print 'Fill %d (%4d bunches, %.1f hours):' % ( fill, result[fill], fillDuration[fill] ), ','.join( fills[fill] )
    print ""

def getCollidingBunches( minFill, maxFill ):
  import pycurl
  import sys
  from urllib import urlencode
  from StringIO import StringIO
  runDbUrl = 'http://lbrundb.cern.ch/rundb/export_fills_maintable'

  data = urlencode( {"fill_id_min": minFill, "fill_id_max" : maxFill, "fill_flist" : "params.nCollidingBunches"} )
  buf = StringIO()
  c = pycurl.Curl()
  c.setopt( pycurl.URL, runDbUrl )
  c.setopt( pycurl.POST, 1 )
  c.setopt( pycurl.POSTFIELDS, data )
  c.setopt( c.WRITEFUNCTION, buf.write )
  c.perform()
  c.close()

  body = buf.getvalue()

  from HTMLParser import HTMLParser
  class tabHolder( object ):

    def __init__( self ):
      self.listOfTabs = []
      self.curTab = []

    def nextTab( self ):
      self.listOfTabs.append( self.curTab )
      self.curTab = []
      return self.curTab

    def getTab( self ):
      return self.curTab


  tb = tabHolder()

  # create a subclass and override the handler methods
  class MyHTMLParser( HTMLParser ):
      def handle_starttag( self, tag, attrs ):

          if ( tag == 'th' or 'td' ) and len( attrs ) == 1 and "numbers" in attrs[0][1]:
            self.toLog = True
          elif tag == 'form' and len( attrs ) > 1 and "frmExportFills" in attrs[0][1]:
            self.inTheForm = True

      def handle_endtag( self, tag ):
          if tag == 'tr':
            tb.nextTab()
          elif tag == 'th':
            self.toLog = False

      def handle_data( self, data ):
          if getattr( self, "toLog", None ) and getattr( self, 'inTheForm', None ):
            tb.getTab().append( data )

  # instantiate the parser and fed it some HTML
  parser = MyHTMLParser()
  parser.feed( body )
  wantedValues = []
  for i, t in enumerate( tb.listOfTabs ):
    if i == 0:
      continue
    elif i == 1:
      continue
    elif i == len( tb.listOfTabs ) - 1 :
      continue
    values = []
    for v in t:
      v = v.replace( '\\n', '' ).replace( '\\t', '' ).replace( ' ', '' )
      if not v:
        continue
      values.append( v )
    wantedValues.append( values )

  result = {}
  for line in wantedValues:
    result[int( line[0] )] = int( line[2] )
  return result

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerFileSwitches()

  Script.registerSwitch( '', 'TriggerRate', '   For RAW files, returns the trigger rate' )
  Script.registerSwitch( '', 'ListRuns', '   Give a list of runs (to be used with --Trigger)' )
  Script.registerSwitch( '', 'ListFills', '   Give a list of fills (to be used with --Trigger)' )
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  execute()

