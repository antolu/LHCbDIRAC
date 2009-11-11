#!/usr/bin/env python
########################################################################
# $Id$
########################################################################

import DIRAC
from DIRAC.Interfaces.API.Dirac                       import Dirac
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient  import LHCB_BKKDBClient
from DIRAC.Core.Base import Script

__RCSID__ = "$Id$"

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()


cl = LHCB_BKKDBClient()
path = ''
if len(args) > 0:
  path = args[0]
if len(args) > 1:
  for i in range(1, len(args)):
    path += args[i]

print cl.list(path)

