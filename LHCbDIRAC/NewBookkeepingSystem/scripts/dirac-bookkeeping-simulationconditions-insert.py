#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-bookkeeping-simulationconditions-insert
# Author :  Zoltan Mathe
########################################################################
"""
  Insert a new set of simulation conditions in the Bookkeeping
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )
Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

exitCode = 0

desc = raw_input( "SimDescription: " )
beamcond = raw_input( "BeamCond: " )
beamEnergy = raw_input( "BeamEnergy: " )
generator = raw_input( "Generator: " )
magneticField = raw_input( "MagneticField: " )
detectorCond = raw_input( "DetectorCond: " )
luminosity = raw_input( "Luminosity: " )
print 'Do you want to add these new simulation conditions? (yes or no)'
value = raw_input( 'Choice:' )
choice = value.lower()
if choice in ['yes', 'y']:
  res = bk.insertSimConditions( desc, beamcond, beamEnergy, generator, magneticField, detectorCond, luminosity )
  if res['OK']:
    print 'The simulation conditions added successfully!'
  else:
    print "ERROR:", res['Message']
    exitCode = 2
elif choice in ['no', 'n']:
  print 'Aborted!'
else:
  print 'Unexpected choice:', value
  exitCode = 2

DIRAC.exit( exitCode )
