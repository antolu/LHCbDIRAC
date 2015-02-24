#! /usr/bin/env python
"""
   Get statistical information on a dataset
"""

__RCSID__ = "$Id: dirac-bookkeeping-get-stats.py 69357 2013-08-08 13:33:31Z phicharp $"

import DIRAC
from DIRAC.Core.Base import Script
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

def execute():

  for switch in Script.getUnprocessedSwitches():
    pass

  lfns = dmScript.getOption( 'LFNs' )
  bkQuery = dmScript.getBKQuery()
  if not bkQuery and not lfns:
    print "No BK query given..."
    DIRAC.exit( 1 )
  if bkQuery:
    prodList = bkQuery.getQueryDict().get( 'Production', [None] )
    bkQuery.setOption( 'ProductionID', None )
  else:
    prodList = [None]

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
    if not lfns and 'ReplicaFlag' not in queryDict and 'DataQuality' not in queryDict:
      print "Getting info from filesSummary..."
      query = queryDict.copy()
      if len( query ) <= 3:
        query.update( {'1':1, '2':2, '3':3 } )
      fileTypes = query.get( 'FileType' )
      if type( fileTypes ) != type( [] ):
        fileTypes = [fileTypes]
      if 'FileType' in query:
        query.pop( 'FileType' )
      records = []
      if type( fileTypes ) == type( [] ):
        nDatasets = len( fileTypes )
      else:
        nDatasets = 1
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
      nDatasets = 1
      if not lfns:
        fileTypes = queryDict.get( 'FileType' )
        if type( fileTypes ) == type( [] ):
          nDatasets = len( fileTypes )
        res = bk.getFilesWithMetadata( queryDict )
        if res['OK']:
          if 'ParameterNames' in res['Value']:
            parameterNames = res['Value']['ParameterNames']
            info = res['Value']['Records']
          else:
            info = []
            res = bk.getFiles( queryDict )
            if res['OK']:
              lfns = res['Value']
        else:
          print "Error getting files for %s:" % queryDict, res['Message']
          continue
        for item in info:
          metadata = dict( zip( parameterNames, item ) )
          try:
            nbEvents += metadata['EventStat']
            fileSize += metadata['FileSize']
            lumi += metadata['Luminosity']
            nbFiles += 1
          except:
            pass
      if lfns:
        res = bk.getFileMetadata( lfns )
        if res['OK']:
          for lfn, metadata in res['Value'].items():
            try:
              nbEvents += metadata['EventStat']
              fileSize += metadata['FileSize']
              lumi += metadata['Luminosity']
              nbFiles += 1
            except:
              pass
        else:
          print "Error getting files metadata:", res['Message']
          continue
      records = [nbFiles, nbEvents, fileSize, lumi,
                 nbEvents * nDatasets / float( lumi ) if lumi else 0.,
                 fileSize * nDatasets / float( lumi ) if lumi else 0.]

    # Now printout the results
    tab = 17
    sizeUnits = ( 'Bytes', 'kB', 'MB', 'GB', 'TB', 'PB' )
    lumiUnits = ( '/microBarn', '/nb', '/pb', '/fb', '/ab' )
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
        lumi = value / float( nDatasets )
        if lumi:
          for lumiUnit in lumiUnits:
            if lumi < 1000.:
              break
            lumi /= 1000.
        else:
          lumi = 0
          lumiUnit = ''
        lumiString = 'Luminosity' if nDatasets == 1 else 'Avg luminosity'
        print '%s: %.3f %s' % ( lumiString.ljust( tab ), lumi, lumiUnit )
      elif name == 'EvtsPerLumi':
        evtsPerLumi = value
      elif name == 'SizePerLumi':
        print "%s: %.1f GB" % ( ( 'Size  per %s' % '/pb' ).ljust( tab ), value * 1000000. / 1000000000. )
        # if nDatasets != 1:
        #  sizePerEvt = value / evtsPerLumi / 1000. if evtsPerLumi else 0.
        #  print '%s: %.1f kB' % ( 'Avg size per evt'.ljust( tab ), sizePerEvt )
    if lumi:
      filesPerLumi = nfiles / lumi
      print "%s: %.1f" % ( ( 'Files per %s' % lumiUnit ).ljust( tab ), filesPerLumi )
    print ""

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()
  dmScript.registerFileSwitches()


  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  execute()

