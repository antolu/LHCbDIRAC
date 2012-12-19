#!/usr/bin/env python

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage( """
Gives an overview of the grid resources status
""" )
Script.parseCommandLine()

from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
DiracLHCb().gridWeather( printOutput = True )
