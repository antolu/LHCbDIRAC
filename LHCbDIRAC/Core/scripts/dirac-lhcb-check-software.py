#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Core/scripts/dirac-lhcb-check-software.py $
# File :    dirac-lhcb-check-software
# Author :  Joel Closier
########################################################################
__RCSID__ = "$Id: dirac-lhcb-check-software.py 18700 2009-11-30 13:48:50Z paterson $"

"""
Script to check if a package and version exist in the list of software distributed on the GRID
"""

import sys, string
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC import gConfig

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

def usage():
  print 'Usage: %s [package] [ version] [<Optional System Configuration To Query For>]' % ( Script.scriptName )
  DIRAC.exit( 2 )


if len( args ) > 3:
  usage()
else:
  package = args[0] + '.' + args[1]

softwareDistribution = gConfig.getOptionsDict( '/Operations/SoftwareDistribution' )
if not softwareDistribution['OK']:
  print 'ERROR: Could not get values for /Operations/SoftwareDistribution section with message:\n%s' % ( result['Message'] )
  DIRAC.exit( 2 )

software = softwareDistribution['Value']
systemConfigs = software.keys()
systemConfigs.remove( 'Active' )
systemConfigs.remove( 'Deprecated' )


active = software['Active'].replace( ' ', '' ).split( ',' )
active.sort()
deprecated = software['Deprecated'].replace( ' ', '' ).split( ',' )
deprecated.sort()

if not package in active:
  print 'This package was not distributed on the GRID'
  DIRAC.exit( 1 )
else:
  print 'This package is distributed on the GRID'
  DIRAC.exit( 0 )
