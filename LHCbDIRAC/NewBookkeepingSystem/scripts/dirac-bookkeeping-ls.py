#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-ls.py
# Author :  Zoltan Mathe
########################################################################
"""
  List bookkeeping path
"""
__RCSID__ = "$Id$"
from DIRAC.Core.Base                                      import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Path ...' % Script.scriptName,
                                     'Arguments:',
                                     '  Path:     Directory Path' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
if len( args ) == 0:
  Script.showHelp()

path = ''
if len( args ) > 0:
  path = args[0]
if len( args ) > 1:
  for i in range( 1, len( args ) ):
    path += args[i]
# This concatenates without spaces the arguments !!!!!!

from LHCbDIRAC.NewBookkeepingSystem.Client.LHCB_BKKDBClient  import LHCB_BKKDBClient
cl = LHCB_BKKDBClient()
print cl.list( path )

