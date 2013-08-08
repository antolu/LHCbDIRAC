#!/usr/bin/env python

"""
Move files that are Unused or MaxReset from a parent production to its derived production
The argument is a list of productions: comma separated list of ranges (a range has the form p1:p2)
"""

__RCSID__ = "$Id$"

import sys
from DIRAC.Core.Base import Script


Script.setUsageMessage( '\n'.join( [ __doc__,
                                    'Usage:',
                                    '  %s [option|cfgfile] [prod1[:prod2][,prod3[:prod4]]' % Script.scriptName, ] ) )
Script.registerSwitch( '', 'NoReset', "Don't reset the MaxRest files to unused (default is to reset)" )
Script.parseCommandLine( ignoreErrors=True )
import DIRAC

resetUnused = True
switches = Script.getUnprocessedSwitches()
for switch in switches:
  if switch[0] == 'NoReset':
    resetUnused = False
args = Script.getPositionalArgs()

if not len( args ):
    print "Specify transformation number..."
    DIRAC.exit( 0 )
else:
    ids = args[0].split( "," )
    idList = []
    for id in ids:
        r = id.split( ':' )
        if len( r ) > 1:
            for i in range( int( r[0] ), int( r[1] ) + 1 ):
                idList.append( i )
        else:
            idList.append( int( r[0] ) )

from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
transClient = TransformationClient()

for prod in idList:
  res = transClient.getTransformation( prod, extraParams=True )
  if not res['OK']:
    print "Error getting transformation %s" % prod, res['Message']
  else:
    res = transClient.moveFilesToDerivedTransformation( res['Value'], resetUnused )
    if not res['OK']:
      print "Error updating a derived transformation %d:" % prod, res['Message']
    else:
      parentProd, movedFiles = res['Value']
      if movedFiles:
        print "Successfully moved files from %d to %d:" % ( parentProd, prod )
        for status, val in movedFiles.items():
          print "\t%d files to status %s" % ( val, status )
