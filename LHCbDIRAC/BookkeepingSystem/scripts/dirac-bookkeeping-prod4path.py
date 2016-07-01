#! /usr/bin/env python
"""
   Get production numbers given a dataset path
"""

__RCSID__ = "$Id$"

import time
from DIRAC.Core.Base import Script
from DIRAC import gLogger, exit
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

def printProds( title, prods ):
  typeDict = {}
  for prod, prodType in prods.iteritems():
    typeDict.setdefault( prodType, [] ).append( prod )
  gLogger.notice( title )
  for prodType, prodList in typeDict.iteritems():
    gLogger.notice( '(%s): %s' % ( prodType, ','.join( [str( prod ) for prod in sorted( prodList )] ) ) )

def execute():
  tr = TransformationClient()

  for switch in Script.getUnprocessedSwitches():
    pass

  bkQuery = dmScript.getBKQuery()
  if not bkQuery:
    gLogger.notice( "No BKQuery given..." )
    exit( 1 )

  startTime = time.time()
  prods = bkQuery.getBKProductions()  # visible = 'All' )

  parents = {}
  productions = {}
  for prod in prods:
    type = tr.getTransformation( prod ).get( 'Value', {} ).get( 'Type', 'Unknown' )
    productions[prod] = type
    parent = tr.getBookkeepingQuery( prod ).get( 'Value', {} ).get( 'ProductionID', '' )
    if parent:
      type = tr.getTransformation( parent ).get( 'Value', {} ).get( 'Type', 'Unknown' )
      parents[parent] = type

  gLogger.notice( "For BK path %s:" % bkQuery.getPath() )
  if not prods:
    gLogger.notice( 'No productions found!' )
  else:
    printProds( 'Productions found', productions )
    if parents:
      printProds( 'Parent productions', parents )

  gLogger.notice( 'Completed in %.1f seconds' % ( time.time() - startTime ) )
if __name__ == "__main__":

  dmScript = DMScript()
  dmScript.registerBKSwitches()

  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile]' % Script.scriptName, ] ) )

  Script.addDefaultOptionValue( 'LogLevel', 'error' )
  Script.parseCommandLine( ignoreErrors = False )

  execute()
