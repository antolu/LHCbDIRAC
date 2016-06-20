#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-get-files.py
# Author :  Zoltan Mathe
########################################################################
"""
  Retrieve files of a BK query
"""
__RCSID__ = "$Id$"
from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript, Script

dmScript = DMScript()
dmScript.registerBKSwitches()
Script.registerSwitch( '', 'File=', '   Provide a list of BK paths' )
Script.registerSwitch( '', 'Term', '   Provide the list of BK paths from terminal' )
Script.registerSwitch( '', 'Output=', '  Specify a file that will contain the list of files' )
maxFiles = 20
Script.registerSwitch( '', 'MaxFiles=', '   Print only <MaxFiles> lines on stdout (%d if output, else All)' % maxFiles )
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... ProdID Type' % Script.scriptName] ) )

Script.parseCommandLine()

from LHCbDIRAC.BookkeepingSystem.Client.ScriptExecutors import executeGetFiles
executeGetFiles( dmScript, maxFiles )

