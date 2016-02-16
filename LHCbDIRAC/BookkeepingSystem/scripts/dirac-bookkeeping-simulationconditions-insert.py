#!/usr/bin/env python
########################################################################
# File :    dirac-bookkeeping-simulationconditions-insert
# Author :  Zoltan Mathe
########################################################################
"""
  Insert a new set of simulation conditions in the Bookkeeping
"""
__RCSID__ = "$Id: dirac-bookkeeping-simulationconditions-insert.py 69359 2013-08-08 13:57:13Z phicharp $"

import DIRAC
from DIRAC.Core.Base import Script
Script.setUsageMessage('\n'.join([ __doc__.split('\n')[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ]))
Script.parseCommandLine(ignoreErrors=True)

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

exitCode = 0

desc = raw_input("SimDescription: ")
beamcond = raw_input("BeamCond: ")
beamEnergy = raw_input("BeamEnergy: ")
generator = raw_input("Generator: ")
magneticField = raw_input("MagneticField: ")
detectorCond = raw_input("DetectorCond: ")
luminosity = raw_input("Luminosity: ")
g4settings = raw_input("G4settings: ")
print 'Do you want to add these new simulation conditions? (yes or no)'
value = raw_input('Choice:')
choice = value.lower()
if choice in ['yes', 'y']:
  in_dict={'SimDescription':desc,'BeamCond':beamcond, 'BeamEnergy': beamEnergy, 'Generator':generator,\
           'MagneticField':magneticField,'DetectorCond':detectorCond,'Luminosity':luminosity,'G4settings':g4settings,\
           'Visible':'Y'}
  res = bk.insertSimConditions(in_dict)
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

DIRAC.exit(exitCode)

