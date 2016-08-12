#! /usr/bin/env python
""" Retrieves the GUID from a local file
"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from LHCbDIRAC.Core.Utilities.File import makeGuid

fileName = Script.getPositionalArgs()[0]

guids = makeGuid( fileName )
print guids[fileName]
