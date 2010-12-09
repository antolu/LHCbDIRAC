#!/usr/bin/env python
########################################################################
# $HeadURL:  $
# File :   dirac-bookkeeping-filetypes-list
# Author : Zoltan Mathe
########################################################################

__RCSID__   = "$Id: $"
__VERSION__ = "$ $"

import sys,string,re
import DIRAC
from DIRAC.Core.Base                                               import Script
from DIRAC.ConfigurationSystem.Client.Config                       import gConfig
Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()
exitCode = 0



mfiletypes = []
res=bk.getAvailableFileTypes()

if res['OK']:
  dbresult = res['Value']
  print 'Filetypes:'
  for record in dbresult['Records']:
    print str(record[0]).ljust(30)+str(record[1])
    
DIRAC.exit(exitCode)