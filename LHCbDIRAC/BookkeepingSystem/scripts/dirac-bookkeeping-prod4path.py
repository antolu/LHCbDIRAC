#! /usr/bin/env python
"""
   Get production numbers given a dataset path
"""

__RCSID__ = "$Id: dirac-bookkeeping-get-stats.py 69357 2013-08-08 13:33:31Z phicharp $"

import DIRAC
from DIRAC.Core.Base import Script
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from DIRAC import gLogger

def printProds( title, prods ):
    types = list( set( prods.values() ) )
    if len( types ) == 1:
      gLogger.always( '%s (%s): %s' % ( title, types[0], ','.join( [str( prod ) for prod in sorted( prods )] ) ) )
    else:
      gLogger.always( '%s: %s' % ','.join( [' %s (%s)' % ( title, prod, prods[prod] ) for prod in sorted( prods )] ) )

def execute():
  from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient  import BookkeepingClient
  from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
  bk = BookkeepingClient()
  tr = TransformationClient()

  for switch in Script.getUnprocessedSwitches():
    pass

  bkQuery = dmScript.getBKQuery()

  prods = bkQuery.getBKProductions( visible = 'All' )

  parents = {}
  productions = {}
  for prod in prods:
    type = tr.getTransformation( prod ).get( 'Value', {} ).get( 'Type', 'Unknown' )
    productions[prod] = type
    parent = tr.getBookkeepingQuery( prod ).get( 'Value', {} ).get( 'ProductionID', '' )
    if parent:
      type = tr.getTransformation( parent ).get( 'Value', {} ).get( 'Type', 'Unknown' )
      parents[parent] = type

  gLogger.always( "For BK path %s:" % bkQuery.getPath() )
  if not prods:
    gLogger.always( 'No productions found!' )
  else:
    printProds( 'Productions found', productions )
    if parents:
      printProds( 'Parent productions', parents )

if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  execute()
