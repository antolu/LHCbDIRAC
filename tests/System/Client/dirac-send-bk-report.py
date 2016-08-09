#! /usr/bin/env python
""" Retrieves the GUID from a local file
"""

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bkClient = BookkeepingClient()

bkXML = Script.getPositionalArgs()[0]

bkClient.sendXMLBookkeepingReport( bkXML )
