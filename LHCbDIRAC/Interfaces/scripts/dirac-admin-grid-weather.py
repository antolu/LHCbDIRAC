from DIRAC.Core.Base import Script

Script.setUsageMessage("""
Gives an overview of the grid resources status

Usage:
   %s 
""" % Script.scriptName)
Script.parseCommandLine()

from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
DiracLHCb().gridWeather(printOutput=True)