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

dataTypes = gConfig.getValue('/Operations/Bookkeeping/FileTypes',[])

mfiletypes = []
res=bk.getAvailableFileTypes()
if res['OK']:
  dbresult = res['Value']
  print 'Filetypes:'
  for record in dbresult:
    print str(record[0]).ljust(10)
    if record[0] not in dataTypes:
      mfiletypes += [record[0]]
  if len(mfiletypes) > 0:
    print 'File Types have to be add to CS:'
    for i in mfiletypes:
      print str(i).ljust(10)

DIRAC.exit(exitCode)