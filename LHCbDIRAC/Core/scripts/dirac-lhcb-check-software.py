#!/usr/bin/env python
########################################################################
# File :    dirac-lhcb-check-software
# Author :  Joel Closier
########################################################################
"""
  Script to check if a package and version exist in the list of software distributed on the GRID
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities import List
from DIRAC import gConfig

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Name Version' % Script.scriptName,
                                     'Arguments:',
                                     '  Name:     Name of the LHCb software package',
                                     '  Version:  Version of the LHCb software package' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()


if len( args ) != 2:
  Script.showHelp()

package = args[0] + '.' + args[1]

softwareDistribution = gConfig.getOptionsDict( '/Operations/SoftwareDistribution' )
if not softwareDistribution['OK']:
  print 'ERROR: Could not get values for /Operations/SoftwareDistribution section with message:\n%s' % ( softwareDistribution['Message'] )
  DIRAC.exit( 2 )

software = softwareDistribution['Value']
systemConfigs = software.keys()
systemConfigs.remove( 'Active' )
systemConfigs.remove( 'Deprecated' )


active = List.fromChar( software['Active'], ',' )
active.sort()
deprecated = List.fromChar( software['Deprecated'], ',' )
deprecated.sort()

if not package in active:
  print 'This package was not distributed on the GRID'
  DIRAC.exit( 1 )
else:
  print 'This package is distributed on the GRID'
  DIRAC.exit( 0 )
