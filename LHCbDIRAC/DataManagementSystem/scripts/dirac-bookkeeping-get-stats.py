#! /usr/bin/env python
"""
   Get statistical information on a dataset
"""

__RCSID__ = "$Id:  $"

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

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()


  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  for switch in Script.getUnprocessedSwitches():
    pass

  bkQuery = dmScript.getBKQuery()
  prodList = bkQuery.getQueryDict().get('ProductionID', [None])
  bkQuery.setOption('ProductionID', None)

  from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  bk = BookkeepingClient()

  for prod in prodList:
    bkQuery.setOption('Production', prod)
    print "For BK query:", bkQuery
    res = bk.getFilesSumary( bkQuery.getQueryDict() )
    if not res['OK']:
      print "Error getting statistics from BK"
      DIRAC.exit( 0 )
    nbRecords = res['Value']['TotalRecords']
    paramNames = res['Value']['ParameterNames']
    tab = 15
    sizeUnits = ( 'Bytes', 'kB', 'MB', 'GB', 'TB', 'PB' )
    lumiUnits = ( '/microBarn', '/nb', '/pb', '/fb', '/ab' )
    for record in range( nbRecords ):
      paramValues = res['Value']['Records'][record]
      for i in range( len( paramNames ) ):
        if paramNames[i] == 'NbofFiles':
          print '%s:%s' % ( 'Nb of Files'.ljust( tab ), intWithQuotes( paramValues[i] ) )
        elif paramNames[i] == 'NumberOfEvents':
          print '%s:%s' % ( 'Nb of Events'.ljust( tab ), intWithQuotes( paramValues[i] ) )
        elif paramNames[i] == 'FileSize':
          size = paramValues[i]
          if size:
            for unit in sizeUnits:
              if size < 1000.:
                break
              size /= 1000.
          else:
            size = 0
            unit = ''
          print '%s:%.3f %s' % ( 'Total size'.ljust( tab ), size, unit )
        elif paramNames[i] == 'Luminosity':
          lumi = paramValues[i]
          if lumi:
            for unit in lumiUnits:
              if lumi < 1000.:
                break
              lumi /= 1000.
          else:
            lumi = 0
            unit = ''
          print '%s:%.3f %s' % ( 'Luminosity'.ljust( tab ), lumi, unit )
