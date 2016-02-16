#!/usr/bin/env python

__RCSID__ = "$Id: dirac-admin-grid-weather.py 60160 2012-12-19 14:53:31Z phicharp $"

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Gives an overview of the grid resources status
""" )
Script.parseCommandLine()

from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
DiracLHCb().gridWeather( printOutput = True )
