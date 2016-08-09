#! /usr/bin/env python
""" Retrieves the GUID from a local file
"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bkClient = BookkeepingClient()

xmlFile = Script.getPositionalArgs()[0]
with open(xmlFile, 'r') as fd:
  bkXML = fd.read()

res = bkClient.sendXMLBookkeepingReport( bkXML )
print res
