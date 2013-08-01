#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-productions-summary
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve production summary from the Bookkeeping
"""
__RCSID__ = "$Id$"

import DIRAC
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

dmScript = DMScript()
dmScript.registerBKSwitches()
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

exitCode = 0

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

bkQuery = dmScript.getBKQuery()
dict = bkQuery.getQueryDict()
dictItems = ( 'ConfigName', 'ConfigVersion', 'Production', 'ConditionDescription', 'ProcessingPass', 'FileType', 'EventType' )
for item in dictItems:
  if item not in dict:
    dict[item] = 'ALL'
for item in dict.keys():
  if item not in dictItems:
    dict.pop( item )

print 'BKQuery:', dict
res = bk.getProductionSummary( dict )
print res

if not res["OK"]:
  print res["Message"]
else:
  value = res['Value']
  records = value['Records']
  nbRec = value['TotalRecords']
  params = value['ParameterNames']
  width = 20
  print params[0].ljust( 30 ) + str( params[1] ).ljust( 30 ) + \
  str( params[2] ).ljust( 30 ) + str( params[3] ).ljust( 30 ) + \
  str( params[4] ).ljust( 30 ) + str( params[5] ).ljust( 30 ) + \
  str( params[6] ).ljust( 20 ) + str( params[7] ).ljust( 20 ) + \
  str( params[8] ).ljust( 20 )
  for record in records:
    print str( record[0] ).ljust( 15 ) + str( record[1] ).ljust( 15 ) + \
    str( record[2] ).ljust( 20 ) + str( record[3] ).ljust( width ) + \
    str( record[4] ).ljust( width ) + str( record[5] ).ljust( width ) + \
    str( record[6] ).ljust( width ) + str( record[7] ).ljust( width ) + \
    str( record[8] ).ljust( width )


DIRAC.exit( exitCode )

