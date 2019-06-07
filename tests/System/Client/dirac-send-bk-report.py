#! /usr/bin/env python
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
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
