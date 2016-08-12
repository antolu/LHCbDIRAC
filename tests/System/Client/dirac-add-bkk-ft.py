#! /usr/bin/env python
""" Add a file type to the BKK
"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

ftype, desc, version = Script.getPositionalArgs()

res = bk.insertFileTypes(ftype.upper(), desc, version)

print res
