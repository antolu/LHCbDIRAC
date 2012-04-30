#! /usr/bin/env python
"""
   Get statistical information on a dataset
"""

__RCSID__ = "$Id:  $"

import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript

def intWithQuotes( val, quote="'" ):
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

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()


  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors=False )

  for switch in Script.getUnprocessedSwitches():
    pass

  bkQuery = dmScript.getBKQuery()
  prodList = bkQuery.getQueryDict().get( 'ProductionID', [None] )
  bkQuery.setOption( 'ProductionID', None )

  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  bk = BookkeepingClient()

  for prod in prodList:
    bkQuery.setOption( 'Production', prod )
    print "For BK query:", bkQuery
    queryDict = bkQuery.getQueryDict()
    if 'Visible' in queryDict and 'StartRun' not in queryDict and 'EndRun' not in queryDict:
      res = bk.getFilesSummary( queryDict )
      if not res['OK']:
        print "Error getting statistics from BK"
        DIRAC.exit( 0 )
      nbRecords = res['Value']['TotalRecords']
      records = res['Value']['Records']
      paramNames = res['Value']['ParameterNames']
    else:
      # To be replaced with getFilesWithMetadata when it allows invisible files
      paramNames = ['NbofFiles', 'NumberOfEvents', 'FileSize', 'Luminosity']
      nbFiles = 0
      nbEvents = 0
      fileSize = 0
      lumi = 0
      res = bk.getFilesWithGivenDataSets( queryDict )
      if res['OK']:
        res = bk.getFileMetadata( res['Value'] )
        if res['OK']:
          for lfn, metadata in res['Value'].items():
            nbFiles += 1
            nbEvents += metadata['EventStat']
            fileSize += metadata['FileSize']
            lumi += metadata['Luminosity']
          records = [[nbFiles, nbEvents, fileSize, lumi]]
      if not res['OK']:
        records = []
        print "Error getting files information:", res['Message']

    tab = 15
    sizeUnits = ( 'Bytes', 'kB', 'MB', 'GB', 'TB', 'PB' )
    lumiUnits = ( '/microBarn', '/nb', '/pb', '/fb', '/ab' )
    for paramValues in records:
      for name, value in zip( paramNames, paramValues ):
        if name == 'NbofFiles':
          print '%s:%s' % ( 'Nb of Files'.ljust( tab ), intWithQuotes( value ) )
        elif name == 'NumberOfEvents':
          print '%s:%s' % ( 'Nb of Events'.ljust( tab ), intWithQuotes( value ) )
        elif name == 'FileSize':
          size = value
          if size:
            for unit in sizeUnits:
              if size < 1000.:
                break
              size /= 1000.
          else:
            size = 0
            unit = ''
          print '%s:%.3f %s' % ( 'Total size'.ljust( tab ), size, unit )
        elif name == 'Luminosity':
          lumi = value
          if lumi:
            for unit in lumiUnits:
              if lumi < 1000.:
                break
              lumi /= 1000.
          else:
            lumi = 0
            unit = ''
          print '%s:%.3f %s' % ( 'Luminosity'.ljust( tab ), lumi, unit )
